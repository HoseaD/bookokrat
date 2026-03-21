use std::path::{Path, PathBuf};
use std::sync::mpsc::Sender;
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
    let mut last_event: Option<Instant> = None;

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
            if last_event.map_or(true, |t| {
                now.duration_since(t) > Duration::from_millis(DEBOUNCE_MS)
            }) {
                last_event = Some(now);
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
