package file

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/bmatcuk/doublestar/v4"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rjeczalik/notify"
	"go.uber.org/zap"
)

type watcher struct {
	commonPath                string
	basePaths                 []string
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
		paths:                     paths,
		notifyFn:                  notifyFn,
		shouldWatchWrites:         shouldWatchWrites,
		notifyChannelLengthMetric: notifyChannelLengthMetric,
		logger:                    logger,
	}
}

func (w *watcher) start() {
	for _, pattern := range w.paths.Include {
		// /var/lib/docker/containers/**/*-json.log -> /var/lib/docker/containers
		basePattern, _ := doublestar.SplitPattern(pattern)
		w.basePaths = append(w.basePaths, basePattern)
	}
	w.commonPath = commonPathPrefix(w.basePaths)

	w.logger.Infof(
		"starting watcher path=%s, pattern_included=%q, pattern_excluded=%q",
		w.commonPath, w.paths.Include, w.paths.Exclude,
	)

	eventsCh := make(chan notify.EventInfo, 256)
	w.watcherCh = eventsCh

	events := []notify.Event{notify.Create, notify.Rename, notify.Remove}
	if w.shouldWatchWrites {
		events = append(events, notify.Write)
	}

	// watch recursively.
	err := notify.Watch(filepath.Join(w.commonPath, "..."), eventsCh, events...)
	if err != nil {
		w.logger.Warnf("can't create fs watcher: %s", err.Error())
		return
	}
	w.notifyChannelLengthMetric.Set(float64(len(w.watcherCh)))

	go w.watch()

	w.tryAddPath(w.commonPath)
}

func commonPathPrefix(paths []string) string {
	results := make([][]string, 0, len(paths))
	results = append(results, strings.Split(paths[0], string(os.PathSeparator)))
	longest := results[0]

	cmpWithLongest := func(a []string) {
		if len(a) < len(longest) {
			longest = longest[:len(a)]
		}
		for i := 0; i < len(longest); i++ {
			if a[i] != longest[i] {
				longest = longest[:i]
				return
			}
		}
	}

	for i := 1; i < len(paths); i++ {
		r := strings.Split(paths[i], string(os.PathSeparator))
		results = append(results, r)
		cmpWithLongest(r)
	}

	return filepath.Join(string(os.PathSeparator), filepath.Join(longest...))
}

func (w *watcher) stop() {
	w.logger.Infof("stopping watcher")

	notify.Stop(w.watcherCh)
	close(w.watcherCh)
}

func (w *watcher) tryAddPath(path string) {
	w.logger.Infof("starting path watch: %s ", path)

	err := filepath.Walk(path,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() {
				w.notify(notify.Create, path)
			}
			return nil
		},
	)
	if err != nil {
		return
	}
}

func (w *watcher) notify(e notify.Event, path string) {
	filename := path
	if filename == "" || filename == "." || filename == ".." {
		return
	}

	for _, pattern := range w.paths.Exclude {
		match, err := doublestar.PathMatch(pattern, path)
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
		dirFilename := filename
	check_dir:
		for {
			for _, path := range w.basePaths {
				if path == dirFilename {
					w.tryAddPath(filename)
					break check_dir
				}
			}
			if dirFilename == w.commonPath {
				break
			}
			dirFilename = filepath.Dir(dirFilename)
		}
		return
	}

	dirFilename := filepath.Dir(filename)
check_file:
	for {
		for _, path := range w.basePaths {
			if path == dirFilename {
				break check_file
			}
		}
		if dirFilename == w.commonPath {
			return
		}
		dirFilename = filepath.Dir(dirFilename)
	}

	w.logger.Infof("%s %s", e, path)

	for _, pattern := range w.paths.Include {
		match, err := doublestar.PathMatch(pattern, path)
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
