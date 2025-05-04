package main

import (
	"net/http"

	"github.com/tim-hilt/codescene/internal/database"
	"github.com/tim-hilt/codescene/internal/server"
)

func main() {
	db, err := database.Init()
	if err != nil {
		panic(err)
	}
	s := &server.Server{DB: db}

	if err := http.ListenAndServe(":8000", s); err != nil {
		panic(err)
	}
}
