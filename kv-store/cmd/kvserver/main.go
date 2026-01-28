package main

import (
	"log"

	"github.com/isparth/Distributed-Systems/kv-store/internal/server"
)

func main() {
	if err := server.Run(); err != nil {
		log.Fatal(err)
	}
}
