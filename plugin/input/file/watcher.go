package file

import (
	"os"
	"path/filepath"

	"github.com/bmatcuk/doublestar/v4"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rjeczalik/notify"
	"go.uber.org/zap"
)

type watcher struct {
	dir                       string // dir in which watch for files
	paths                     Paths
	notifyFn                  notifyFn // function to receive notifications
	watcherCh                 chan notify.EventInfo
	shouldWatchWrites         bool
	notifyChannelLengthMetric prometheus.Gauge
	logger                    *zap.SugaredLogger
}

type notifyFn func(e notify.Event, filename string, stat os.FileInfo)

// NewWatcher creates a watcher that see file creations in the path
// and if they match filePattern and dirPattern, pass them to notifyFn.
func NewWatcher(
	dir string,
	paths Paths,
	notifyFn notifyFn,
	shouldWatchWrites bool,
	notifyChannelLengthMetric prometheus.Gauge,
	logger *zap.SugaredLogger,
) *watcher {
	return &watcher{
		dir:                       dir,
		paths:                     paths,
		notifyFn:                  notifyFn,
		shouldWatchWrites:         shouldWatchWrites,
		notifyChannelLengthMetric: notifyChannelLengthMetric,
		logger:                    logger,
	}
}

func (w *watcher) start() {
	w.logger.Infof(
		"starting watcher path=%s, pattern_included=%q, pattern_excluded=%q",
		w.dir, w.paths.Include, w.paths.Exclude,
	)

	eventsCh := make(chan notify.EventInfo, 256)
	w.watcherCh = eventsCh

	events := []notify.Event{notify.Create, notify.Rename, notify.Remove}
	if w.shouldWatchWrites {
		events = append(events, notify.Write)
	}

	// watch recursively.
	err := notify.Watch(filepath.Join(w.dir, "..."), eventsCh, events...)
	if err != nil {
		w.logger.Warnf("can't create fs watcher: %s", err.Error())
		return
	}
	w.notifyChannelLengthMetric.Set(float64(len(w.watcherCh)))

	go w.watch()

	w.tryAddPath(w.dir)
}

func (w *watcher) stop() {
	w.logger.Infof("stopping watcher")

	notify.Stop(w.watcherCh)
	close(w.watcherCh)
}

func (w *watcher) tryAddPath(path string) {
	files, err := os.ReadDir(path)
	if err != nil {
		return
	}

	w.logger.Infof("starting path watch: %s ", path)

	for _, file := range files {
		if file.Name() == "" || file.Name() == "." || file.Name() == ".." {
			continue
		}

		w.notify(notify.Create, filepath.Join(path, file.Name()))
	}
}

func (w *watcher) notify(e notify.Event, path string) {
	filename := path
	if filename == "" || filename == "." || filename == ".." {
		return
	}

	filename, err := filepath.Abs(filename)
	if err != nil {
		w.logger.Fatalf("can't get abs file name: %s", err.Error())
		return
	}

	dirRel, _ := filepath.Abs(w.dir)
	rel, _ := filepath.Rel(dirRel, filename)

	w.logger.Infof("%s %s", e, path)

	for _, pattern := range w.paths.Exclude {
		match, err := doublestar.PathMatch(pattern, rel)
		if err != nil {
			w.logger.Fatalf("wrong paths exclude pattern %q: %s", pattern, err.Error())
			return
		}
		if match {
			return
		}
	}

	stat, err := os.Lstat(filename)
	if err != nil {
		return
	}

	if stat.IsDir() {
		w.tryAddPath(filename)
		return
	}

	for _, pattern := range w.paths.Include {
		match, err := doublestar.PathMatch(pattern, rel)
		if err != nil {
			w.logger.Fatalf("wrong paths include pattern %q: %s", pattern, err.Error())
			return
		}

		if match {
			w.notifyFn(e, filename, stat)
		}
	}
}

func (w *watcher) watch() {
	var prevLen int
	for {
		event, ok := <-w.watcherCh
		if !ok {
			return
		}
		newLen := len(w.watcherCh)
		if prevLen != newLen {
			prevLen = newLen
			w.notifyChannelLengthMetric.Set(float64(newLen))
		}
		w.notify(event.Event(), event.Path())
	}
}
