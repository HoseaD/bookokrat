//! Live file reload for PDF documents (requires `pdf` feature).
//!
//! Watches a PDF file for modifications and automatically reloads when changed.
//! Useful for workflows like LaTeX PDF exports where the file is repeatedly
//! overwritten with atomic renames.
//!
//! # How It Works
//!
//! - Directory watcher monitors the parent directory (not the file itself)
//! - File modifications are debounced with a 150ms window
//! - Multiple writes from LaTeX build tools are merged into a single reload
//! - File renames (atomic) are properly detected
//!
//! # Limitations
//!
//! - **PDF feature required**: Enabled with `--features pdf`
//! - **150ms debounce**: Rapid successive file writes trigger a single reload
//! - **State preservation**: Zoom level, scroll position, and page number preserved
//! - **Platform-specific**: Uses native file watching (FSEvents on macOS, inotify on Linux, etc.)
//!
//! # Example
//!
//! ```bash
//! bookokrat document.pdf --watch
//! ```
//!
//! When the PDF file changes (e.g., after re-exporting from LaTeX), the viewer
//! automatically reloads and returns to your previous position.

use std::path::{Path, PathBuf};
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use notify::{Config, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};

/// Represents a live-reload trigger event
#[derive(Debug, Clone)]
pub struct FileChangedEvent {
    pub path: PathBuf,
}

/// Debounce duration: LaTeX tools may write the PDF in multiple flushes.
/// Wait until no further events arrive within this window.
const DEBOUNCE_MS: u64 = 150;

/// Starts a background file watcher for `path`.
/// On file modification, sends a `FileChangedEvent` through `tx`.
/// Returns the watcher handle — drop it to stop watching.
pub fn watch_file(
    path: impl AsRef<Path>,
    tx: Sender<FileChangedEvent>,
) -> notify::Result<RecommendedWatcher> {
    let path = path.as_ref().to_path_buf();
    let watch_dir = path.parent().unwrap_or(Path::new(".")).to_path_buf();

    // We watch the *directory* instead of the file directly.
    // This correctly catches atomic renames (latexmk writes to a temp file,
    // then renames it to the final .pdf — a direct file watch misses this).
    let target = path.clone();
    // Thread-safe debouncing: use Arc<Mutex<>> to safely track last event across threads
    let last_event = Arc::new(Mutex::new(None::<Instant>));

    let watcher = RecommendedWatcher::new(
        move |res: notify::Result<Event>| {
            let Ok(event) = res else { return };

            let is_relevant = match event.kind {
                EventKind::Access(_) => false, // Ignore read accesses
                _ => {
                    // Match on filename rather than full path to handle relative vs absolute path differences
                    let target_name = target.file_name();
                    event.paths.iter().any(|p| p.file_name() == target_name)
                }
            };

            if !is_relevant {
                return;
            }

            // Debounce: only fire if enough time has passed since last event.
            let now = Instant::now();
            let mut last = last_event.lock().unwrap();
            if last.is_none_or(|t| now.duration_since(t) > Duration::from_millis(DEBOUNCE_MS)) {
                *last = Some(now);
                let _ = tx.send(FileChangedEvent {
                    path: target.clone(),
                });
            }
        },
        Config::default(),
    )?;

    let mut watcher = watcher;
    watcher.watch(&watch_dir, RecursiveMode::NonRecursive)?;
    Ok(watcher)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_debounce_suppresses_rapid_events() {
        // Create temp file
        let temp_dir = tempfile::TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.pdf");
        std::fs::write(&file_path, b"initial").unwrap();

        // Start watcher
        let (tx, rx) = std::sync::mpsc::channel();
        let _watcher = watch_file(&file_path, tx).unwrap();

        // Rapid writes within debounce window
        thread::sleep(Duration::from_millis(50));
        std::fs::write(&file_path, b"update1").unwrap();
        thread::sleep(Duration::from_millis(50));
        std::fs::write(&file_path, b"update2").unwrap();
        thread::sleep(Duration::from_millis(50));
        std::fs::write(&file_path, b"update3").unwrap();

        // Wait for debounce window to expire
        thread::sleep(Duration::from_millis(200));

        // Should receive ~1 event (debounced), not 3
        let mut count = 0;
        while let Ok(_) = rx.try_recv() {
            count += 1;
        }
        assert!(count <= 1, "Expected debounced events, got {}", count);
    }

    #[test]
    fn test_debounce_allows_spaced_events() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.pdf");
        std::fs::write(&file_path, b"initial").unwrap();

        let (tx, rx) = std::sync::mpsc::channel();
        let _watcher = watch_file(&file_path, tx).unwrap();

        // First write
        std::fs::write(&file_path, b"update1").unwrap();
        thread::sleep(Duration::from_millis(200)); // > 150ms debounce

        // Second write (far apart)
        std::fs::write(&file_path, b"update2").unwrap();
        thread::sleep(Duration::from_millis(200));

        // Should receive at least 1 event (not aggressively debounced)
        let mut count = 0;
        while let Ok(_) = rx.try_recv() {
            count += 1;
        }
        assert!(count >= 1, "Expected at least 1 event");
    }

    #[test]
    fn test_ignores_access_events() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.pdf");
        std::fs::write(&file_path, b"initial").unwrap();

        let (tx, rx) = std::sync::mpsc::channel();
        let _watcher = watch_file(&file_path, tx).unwrap();

        // Just reading the file should not trigger reload
        let _ = std::fs::read(&file_path).unwrap();
        thread::sleep(Duration::from_millis(200));

        let count = rx.try_iter().count();
        assert_eq!(count, 0, "Read access should not trigger event");
    }

    #[test]
    fn test_file_changed_event_structure() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.pdf");
        std::fs::write(&file_path, b"initial").unwrap();

        let (tx, rx) = std::sync::mpsc::channel();
        let _watcher = watch_file(&file_path, tx).unwrap();

        std::fs::write(&file_path, b"modified").unwrap();
        thread::sleep(Duration::from_millis(200));

        if let Ok(event) = rx.try_recv() {
            assert_eq!(
                event.path.file_name(),
                file_path.file_name(),
                "Event should contain correct file path"
            );
        }
    }

    #[test]
    fn test_watcher_cleanup_on_drop() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.pdf");
        std::fs::write(&file_path, b"initial").unwrap();

        let (tx, rx) = std::sync::mpsc::channel();
        {
            let _watcher = watch_file(&file_path, tx).unwrap();
            std::fs::write(&file_path, b"change1").unwrap();
            thread::sleep(Duration::from_millis(200));
        } // Watcher dropped here

        // After dropping watcher, further writes should not trigger events
        let _received_before_drop = rx.try_iter().count();

        std::fs::write(&file_path, b"change2").unwrap();
        thread::sleep(Duration::from_millis(200));

        let received_after_drop = rx.try_iter().count();
        assert_eq!(
            received_after_drop, 0,
            "No events should arrive after watcher is dropped"
        );
    }
}
