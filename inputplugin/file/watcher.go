package file

import (
	"github.com/fsnotify/fsnotify"
	"gitlab.ozon.ru/sre/filed/logger"
)

type watcher struct {
	jobProvider *jobProvider
	path        string
	fsWatcher   *fsnotify.Watcher

	filesCreated int
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

		//if event.Op&fsnotify.Rename == fsnotify.Rename {
		//	//logger.Infof("file renaming detected %s", filename)
		//}
		if event.Op&fsnotify.Create == fsnotify.Create {
			w.filesCreated++

			w.jobProvider.addJob(filename, false)
		}
		if event.Op&fsnotify.Write == fsnotify.Write {
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
