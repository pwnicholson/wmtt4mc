# CHANGELOG

## [1.7.0] - 2026-05-05

### Added
- **Cache build progress & ETA**: Now shows real-time chunk count and estimated time remaining during cache pre-build phase, separate from rendering progress.
- **Improved stop/close reliability**: Fixed critical deadlock that prevented stopping or closing the app during cache building; workers now respond immediately to stop requests.
- **UI refinements**:
  - Cache mode help text moved to clickable `?` button with bulleted list format (cleaner UI).
  - Combobox styling made explicit: timelapse dropdowns (Dimension, Video resolution, Hill shading) now white when active; cache mode dropdown white when editable, grey when disabled.
  - Cache mode auto-disabled for non-Original video resolutions (cache building only enabled at Original scale).

### Fixed
- **Stop/Close deadlock during cache build**: Workers were suspended by psutil but never resumed before `cancel_event` was set, causing `pool.terminate()` to fail silently. Fixed by resuming workers before signaling cancellation.
- **Worker thread hang on cache build failure**: `pool.join()` in exception handlers had no timeout, allowing hung worker threads to block graceful shutdown forever. Now joins with 5-second timeout in daemon thread.
- **Cache build progress invisible**: Prebuild loop never passed `progress_cb` to `build_snapshot_cache()`. Now shows per-chunk progress, total chunks to scan, and real-time ETA.

### Changed
- Cache building now occurs in separate "Cache Creation Progress" phase with its own ETA, resetting progress bar when rendering begins.
- UI labels and status messages clarified to distinguish cache prep from rendering.

## [1.6.0] - 2026-05-04

### Added
- Pre-render cache building: When output cache mode is enabled, snapshots are cached before rendering instead of after.
- ETA enhancements for long raw renders: Live raw chunk-scan progress parsing with dynamic ETA updates.
- Bedrock world quick-scan estimator for more accurate early ETA.
- Stop/Close hard-termination infrastructure with 3-way stop prompt (partial GIF / immediate / cancel).
- Pause/resume child processes during stop/close confirmation dialogs.

### Fixed
- `RuntimeError` crash on Windows when passing sync primitives to ProcessPoolExecutor tasks.
- ETA severe underestimation early in long raw renders.
- Orphan Python worker processes after stop/close.

## [1.5.0] and earlier

See GitHub Releases page for historical versions.
