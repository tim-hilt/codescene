package main

import (
	"fmt"
	"net/http"
	"os"
	"os/signal"

	"github.com/tim-hilt/codescene/internal/database"
	"github.com/tim-hilt/codescene/internal/server"
)

func main() {
	db, err := database.Init()
	if err != nil {
		panic(err)
	}

	s := &server.Server{DB: db}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		fmt.Println("Ctrl-C pressed! Exiting...")
		if err = db.Close(); err != nil {
			panic(err)
		}
		os.Exit(0)
	}()

	if err := http.ListenAndServe(":8000", s); err != nil {
		panic(err)
	}
}
