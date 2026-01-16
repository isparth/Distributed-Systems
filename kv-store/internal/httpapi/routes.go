package httpapi

import (
	"github.com/go-chi/chi/v5"
	"github.com/isparth/Distributed-Systems/kv-store/internal/kv"
)

func registerRoutes(r chi.Router, kv *kv.KVStore) {
	r.Route("/kv", func(r chi.Router) {
		r.Get("/healthz", handleHealthz())
		r.Post("/stop", handleStop(kv))
		r.Post("/restart", handleRestart(kv))
		r.Get("/{key}", handleGet(kv))
		r.Put("/{key}", handlePut(kv))
		r.Delete("/{key}", handleDelete(kv))
		r.Post("/mget", handleMGet(kv))
		r.Put("/mput", handleMPut(kv))
		r.Post("/mdelete", handleMDelete(kv))

	})
}
