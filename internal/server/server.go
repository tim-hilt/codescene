package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strconv"

	"github.com/tim-hilt/codescene/internal"
	"github.com/tim-hilt/codescene/internal/database"
)

type Server struct {
	*database.DB
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	method := r.Method
	reMetadata := regexp.MustCompile(`^/projects/(.*)/metadata$`)
	project := reMetadata.FindStringSubmatch(path)
	switch {
	case path == "/analyze" && method == http.MethodGet:
		s.analyze(w, r)
	case path == "/projects" && method == http.MethodGet:
		s.projects(w, r)
	case len(project) > 1 && method == http.MethodGet:
		s.projectMetadata(w, r, project[1])
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

func (s *Server) projects(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "http://localhost:3000")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	w.Header().Set("Access-Control-Allow-Credentials", "true")

	w.Header().Set("Content-Type", "application/json")

	projects, err := s.GetProjects()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err = json.NewEncoder(w).Encode(projects); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *Server) projectMetadata(w http.ResponseWriter, _ *http.Request, project string) {
	w.Header().Set("Access-Control-Allow-Origin", "http://localhost:3000")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	w.Header().Set("Access-Control-Allow-Credentials", "true")

	w.Header().Set("Content-Type", "application/json")

	metadata, err := s.GetProjectMetadata(project)

	if err == database.ErrProjectNotFound {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err = json.NewEncoder(w).Encode(metadata); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
