package http

import (
	"net/http"

	"github.com/ozontech/file.d/logger"
)

var info = []byte(`{
  "name" : "file-d",
  "cluster_name" : "file-d",
  "cluster_uuid" : "Rz-wj_pkT8a0Y1KXTLmN9g",
  "version" : {
    "number" : "6.7.1",
    "build_flavor" : "default",
    "build_type" : "deb",
    "build_hash" : "2f32220",
    "build_date" : "2019-04-02T15:59:27.961366Z",
    "build_snapshot" : false,
    "lucene_version" : "7.7.0",
    "minimum_wire_compatibility_version" : "5.6.0",
    "minimum_index_compatibility_version" : "5.0.0"
  },
  "tagline" : "You know, for file.d"
}`)

var xpack = []byte(`{
  "build": {
    "date": "2019-04-02T15:59:27.961366Z",
    "hash": "2f32220"
  },
  "features": {
    "graph": {
      "available": false,
      "description": "Graph Data Exploration for the Elastic Stack",
      "enabled": true
    },
    "ilm": {
      "available": true,
      "description": "Index lifecycle management for the Elastic Stack",
      "enabled": true
    },
    "logstash": {
      "available": false,
      "description": "Logstash management component for X-Pack",
      "enabled": true
    },
    "ml": {
      "available": false,
      "description": "Machine Learning for the Elastic Stack",
      "enabled": false,
      "native_code_info": {
        "build_hash": "N/A",
        "version": "N/A"
      }
    },
    "monitoring": {
      "available": true,
      "description": "Monitoring for the Elastic Stack",
      "enabled": true
    },
    "rollup": {
      "available": true,
      "description": "Time series pre-aggregation and rollup",
      "enabled": true
    },
    "security": {
      "available": false,
      "description": "Security for the Elastic Stack",
      "enabled": false
    },
    "sql": {
      "available": true,
      "description": "SQL access to Elasticsearch",
      "enabled": true
    },
    "watcher": {
      "available": false,
      "description": "Alerting, Notification and Automation for the Elastic Stack",
      "enabled": true
    }
  },
  "license": {
    "mode": "basic",
    "status": "active",
    "type": "basic",
    "uid": "e76d6ce9-f78c-44ff-8fd5-b5877357d649"
  },
  "tagline": "You know, for nothing"
}`)

var result = []byte(`{
   "took": 30,
   "errors": false,
   "items": []
}`)

var empty = []byte(`{}`)

func (p *Plugin) elasticsearch(mux *http.ServeMux) {
	mux.HandleFunc("/", p.serveElasticsearchInfo)
	mux.HandleFunc("/_xpack", p.serveElasticsearchXPack)
	mux.HandleFunc("/_bulk", p.serve)
	mux.HandleFunc("/_template/", p.serveElasticsearchTemplate)
}

func (p *Plugin) serveElasticsearchXPack(w http.ResponseWriter, _ *http.Request) {
	_, err := w.Write(xpack)
	if err != nil {
		logger.Errorf("can't write response: %s", err.Error())
	}
}

func (p *Plugin) serveElasticsearchTemplate(w http.ResponseWriter, _ *http.Request) {
	_, err := w.Write(empty)
	if err != nil {
		logger.Errorf("can't write response: %s", err.Error())
	}
}

func (p *Plugin) serveElasticsearchInfo(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet && r.RequestURI == "/" {

		_, err := w.Write(info)
		if err != nil {
			logger.Errorf("can't write response: %s", err.Error())
		}
		return
	}

	logger.Errorf("unknown request uri=%s, method=%s", r.RequestURI, r.Method)
}
