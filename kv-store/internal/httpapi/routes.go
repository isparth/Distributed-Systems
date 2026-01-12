package httpapi

import (
	"github.com/go-chi/chi/v5"
	"github.com/isparth/Distributed-Systems/kv-store/internal/kv"
)

func registerRoutes(r chi.Router, kv *kv.KVStore) {
	r.Route("/v1", func(r chi.Router) {
		r.Get("/healthz", handleHealthz(kv))

	})
}
