package config

import (
	"context"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/fsnotify/fsnotify"
)

// reloadDebounce collapses the burst of events an editor emits when saving
// (write, chmod, rename-into-place) into a single reload.
const reloadDebounce = 150 * time.Millisecond

// Watcher reloads a config file when it changes on disk.
//
// It watches the containing directory rather than the file itself: many
// editors and config managers save by writing a temp file and renaming it over
// the original, which detaches an inode-level watch after the first save.
type Watcher struct {
	path    string
	watcher *fsnotify.Watcher
	log     *slog.Logger
	onLoad  func(*Config)
}

// Watch starts watching path and calls onLoad with each successfully parsed
// version of the file. A file that fails to parse or validate is logged and
// ignored, leaving the previously loaded config in force — a typo in the quota
// table must not take the limiter down.
//
// The watcher stops when ctx is cancelled or Close is called.
func Watch(ctx context.Context, path string, log *slog.Logger, onLoad func(*Config)) (*Watcher, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	fsw, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	if err := fsw.Add(filepath.Dir(abs)); err != nil {
		fsw.Close()
		return nil, err
	}
	w := &Watcher{path: abs, watcher: fsw, log: log, onLoad: onLoad}
	go w.run(ctx)
	return w, nil
}

// Close stops the watcher.
func (w *Watcher) Close() error { return w.watcher.Close() }

func (w *Watcher) run(ctx context.Context) {
	defer w.watcher.Close()

	// A stopped timer with a drained channel: the select below only fires once
	// the debounce window has actually elapsed after the last event.
	timer := time.NewTimer(reloadDebounce)
	if !timer.Stop() {
		<-timer.C
	}
	pending := false

	for {
		select {
		case <-ctx.Done():
			return

		case ev, ok := <-w.watcher.Events:
			if !ok {
				return
			}
			if filepath.Clean(ev.Name) != w.path {
				continue
			}
			if ev.Op&(fsnotify.Write|fsnotify.Create|fsnotify.Rename) == 0 {
				continue
			}
			if pending && !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(reloadDebounce)
			pending = true

		case err, ok := <-w.watcher.Errors:
			if !ok {
				return
			}
			w.log.Warn("config watcher error", "error", err)

		case <-timer.C:
			pending = false
			cfg, err := Load(w.path)
			if err != nil {
				w.log.Error("config reload failed, keeping previous config",
					"path", w.path, "error", err)
				continue
			}
			w.log.Info("config reloaded", "path", w.path)
			w.onLoad(cfg)
		}
	}
}
