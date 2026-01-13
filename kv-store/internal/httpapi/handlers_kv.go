package httpapi

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/isparth/Distributed-Systems/kv-store/internal/httpapi/respond"
	"github.com/isparth/Distributed-Systems/kv-store/internal/kv"
)

func handleHealthz(kv *kv.KVStore) http.HandlerFunc {
	type resp struct {
		Status string `json:"status"`
		Time   string `json:"time"`
	}

	return func(w http.ResponseWriter, r *http.Request) {
		respond.JSON(w, http.StatusOK, resp{
			Status: "ok",
			Time:   time.Now().UTC().Format(time.RFC3339),
		})
	}
}

func handleGet(kv *kv.KVStore) http.HandlerFunc {
	type resp struct {
		Value string `json:"value"`
		Ok    bool   `json:"ok"`
	}

	return func(w http.ResponseWriter, r *http.Request) {
		key := chi.URLParam(r, "key")
		if key == "" {
			http.Error(w, "Key is Missing", http.StatusBadRequest)
			return
		}
		value, ok := kv.Get(key)
		respond.JSON(w, http.StatusOK, resp{
			Value: value,
			Ok:    ok,
		})
	}
}

func handlePut(kv *kv.KVStore) http.HandlerFunc {
	type resp struct {
		Ok bool `json:"ok"`
	}

	type PutRequest struct {
		Value    *string `json:"value"`
		Expected *string `json:"expected"`
	}

	return func(w http.ResponseWriter, r *http.Request) {
		key := chi.URLParam(r, "key")
		if key == "" {
			http.Error(w, "Key is Missing", http.StatusBadRequest)
			return
		}

		var req PutRequest

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}

		if req.Value == nil {
			http.Error(w, "Value is Missing", http.StatusBadRequest)
		}

		var ok bool

		if req.Expected != nil {
			ok = kv.CAS(key, *req.Value, *req.Expected)

		} else {
			ok = kv.Put(key, *req.Value)
		}

		respond.JSON(w, http.StatusOK, resp{
			Ok: ok,
		})
	}
}

func handleDelete(kv *kv.KVStore) http.HandlerFunc {
	type resp struct {
		Ok bool `json:"ok"`
	}

	return func(w http.ResponseWriter, r *http.Request) {
		key := chi.URLParam(r, "key")

		if key == "" {
			http.Error(w, "Key is Missing", http.StatusBadRequest)
			return
		}

		ok := kv.Delete(key)
		respond.JSON(w, http.StatusOK, resp{

			Ok: ok,
		})
	}
}
