package server

import (
	"database/sql"
	"fmt"
	"net/http"
	"strconv"

	"github.com/tim-hilt/codescene/internal"
)

type Server struct {
	DB *sql.DB
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	method := r.Method
	switch {
	case path == "/analyze" && method == http.MethodGet:
		s.analyze(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) analyze(w http.ResponseWriter, r *http.Request) {
	// TODO: Remove this once, the frontend is embedded
	w.Header().Set("Access-Control-Allow-Origin", "http://localhost:3000")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	w.Header().Set("Access-Control-Allow-Credentials", "true")
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)

	repo := r.URL.Query().Get("repo")
	if repo == "" {
		w.Write([]byte("data: repository not provided\n\n"))
		w.(http.Flusher).Flush()
		return
	}

	force := false
	f := r.URL.Query().Get("force")
	if f == "true" {
		force = true
	}

	err := internal.Analyze(s.DB, repo, force, func(c, t int) {
		w.Write([]byte("id: " + strconv.Itoa(c) + "\n"))
		w.Write([]byte(fmt.Sprintf("data: {\"current\":%d,\"total\":%d}\n\n", c, t)))
		w.(http.Flusher).Flush()
	})
	if err != nil {
		w.Write([]byte("data: " + err.Error() + "\n\n"))
		w.(http.Flusher).Flush()
		return
	}
}
