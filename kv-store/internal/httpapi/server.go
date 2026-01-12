package httpapi

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/isparth/Distributed-Systems/kv-store/internal/kv"
)

func NewRouter(kv *kv.KVStore) http.Handler {
	r := chi.NewRouter()

	// shared middleware
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Logger)

	// attach routes
	registerRoutes(r, kv)

	return r
}
