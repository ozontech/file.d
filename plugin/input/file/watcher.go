package file

import (
	"github.com/fsnotify/fsnotify"
	"gitlab.ozon.ru/sre/filed/logger"
	"go.uber.org/atomic"
)

type watcher struct {
	jobProvider *jobProvider
	path        string
	fsWatcher   *fsnotify.Watcher

	filesCreated atomic.Int32
}

func NewWatcher(path string, jobProvider *jobProvider) *watcher {

	return &watcher{path: path, jobProvider: jobProvider}
}

func (w *watcher) start() {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		logger.Fatalf("can't watching for files: %s", err.Error())
	}
	w.fsWatcher = watcher

	go w.watchEvents()
	go w.watchErrors()

	err = w.fsWatcher.Add(w.path)
	if err != nil {
		logger.Fatalf("can't watch path: %s", err.Error())
	}
}

func (w *watcher) stop() {
	err := w.fsWatcher.Close()
	if err != nil {
		logger.Errorf("can't stop watching: %s", err.Error())
	}
}

func (w *watcher) watchEvents() {
	logger.Info("file watching started")
	for {
		event, ok := <-w.fsWatcher.Events
		if !ok {
			return
		}

		filename := event.Name

		if event.Op&fsnotify.Create == fsnotify.Create {
			logger.Debugf("create event received for %s", filename)
			w.filesCreated.Inc()

			w.jobProvider.addJob(filename, false)
		}

		if event.Op&fsnotify.Write == fsnotify.Write {
			logger.Debugf("write event received for %s", filename)
			w.jobProvider.addJob(filename, false)
		}
	}
}

func (w *watcher) watchErrors() {
	for {
		err, ok := <-w.fsWatcher.Errors
		if !ok {
			return
		}

		logger.Errorf("error while watching: %s", err.Error())
	}
}

func (w *watcher) FilesCreated() int {
	return int(w.filesCreated.Load())
}
