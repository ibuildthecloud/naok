package ui

import (
	"net/http"
	"strings"

	"github.com/gorilla/mux"
)

func New(path string, offline string) http.Handler {
	vue := NewUIHandler(&Options{
		Path: func() string {
			if path == "" {
				return defaultPath
			}
			return path
		},
		Offline: func() string {
			switch offline {
			case
				"true",
				"false",
				"dynamic":
				return offline
			}
			return "dynamic"
		},
	})

	router := mux.NewRouter()
	router.UseEncodedPath()

	router.Handle("/", http.RedirectHandler("/dashboard/", http.StatusFound))
	router.Handle("/dashboard", http.RedirectHandler("/dashboard/", http.StatusFound))
	router.Handle("/dashboard/", vue.IndexFile())
	router.Handle("/favicon.png", vue.ServeFaviconDashboard())
	router.Handle("/favicon.ico", vue.ServeFaviconDashboard())
	router.PathPrefix("/dashboard/").Handler(vue.IndexFileOnNotFound())
	router.PathPrefix("/k8s/clusters/local").HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		url := strings.TrimPrefix(req.URL.Path, "/k8s/clusters/local")
		if url == "" {
			url = "/"
		}
		http.Redirect(rw, req, url, http.StatusFound)
	})

	return router
}
