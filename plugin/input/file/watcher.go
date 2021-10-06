package file

import (
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/fsnotify/fsnotify"
	"github.com/ozonru/file.d/longpanic"
	"go.uber.org/zap"
)

type watcher struct {
	path            string // dir in which watch for files
	filenamePattern string // files which match this pattern will be watched
	dirPattern      string // dirs which match this pattern will be watched
	notifyFn        notify // function to receive notifications
	fsWatcher       *fsnotify.Watcher
	logger          *zap.SugaredLogger
}

type notify func(filename string, stat os.FileInfo)

func NewWatcher(path string, filenamePattern string, dirPattern string, notifyFn notify, logger *zap.SugaredLogger) *watcher {
	return &watcher{
		path:            path,
		filenamePattern: filenamePattern,
		dirPattern:      dirPattern,
		notifyFn:        notifyFn,
		logger:          logger,
	}
}

func (w *watcher) start() {
	w.logger.Infof("starting watcher path=%s, pattern=%s", w.path, w.filenamePattern)

	if _, err := filepath.Match(w.filenamePattern, "_"); err != nil {
		w.logger.Fatalf("wrong file name pattern %q: %s", w.filenamePattern, err.Error())
	}

	if _, err := filepath.Match(w.dirPattern, "_"); err != nil {
		w.logger.Fatalf("wrong dir name pattern %q: %s", w.dirPattern, err.Error())
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		w.logger.Warnf("can't create fs watcher: %s", err.Error())
		return
	}
	w.fsWatcher = watcher

	longpanic.Go(w.watch)

	w.tryAddPath(w.path)
}

func (w *watcher) stop() {
	w.logger.Infof("stopping watcher")

	err := w.fsWatcher.Close()
	if err != nil {
		w.logger.Errorf("can't close watcher: %s", err.Error())
	}
}

func (w *watcher) tryAddPath(path string) {
	err := w.fsWatcher.Add(path)
	if err != nil {
		return
	}

	files, err := ioutil.ReadDir(path)
	if err != nil {
		return
	}

	w.logger.Infof("starting path watch: %s ", path)

	for _, file := range files {
		if file.Name() == "" || file.Name() == "." || file.Name() == ".." {
			continue
		}

		filename := filepath.Join(path, file.Name())
		event := fsnotify.Event{Name: filename, Op: fsnotify.Create}
		w.notify(&event)
	}
}

func (w *watcher) notify(event *fsnotify.Event) {
	filename := event.Name
	if filename == "" || filename == "." || filename == ".." {
		return
	}

	filename, err := filepath.Abs(filename)
	if err != nil {
		w.logger.Fatalf("can't get abs file name: %s", err.Error())
		return
	}

	stat, err := os.Lstat(filename)
	if err != nil {
		return
	}

	match, _ := filepath.Match(w.filenamePattern, filepath.Base(filename))
	if match {
		w.notifyFn(filename, stat)
	}

	match, _ = filepath.Match(w.dirPattern, filepath.Base(filename))
	if stat.IsDir() && match {
		w.tryAddPath(filename)
	}
}

func (w *watcher) watch() {
	for {
		select {
		case event, ok := <-w.fsWatcher.Events:
			if !ok {
				return
			}

			w.notify(&event)
		case err, ok := <-w.fsWatcher.Errors:
			if !ok {
				return
			}
			w.logger.Infof("watching path error(have it been deleted?): %s", err.Error())
		}
	}
}
