package httpapi

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/isparth/Distributed-Systems/kv-store/internal/httpapi/respond"
	"github.com/isparth/Distributed-Systems/kv-store/internal/kv"
)

func handleHealthz() http.HandlerFunc {
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

func handleGet(store *kv.KVStore) http.HandlerFunc {
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
		value, ok := store.Get(key)
		respond.JSON(w, http.StatusOK, resp{
			Value: value,
			Ok:    ok,
		})
	}
}

func handlePut(store *kv.KVStore) http.HandlerFunc {
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

		decoder := json.NewDecoder(r.Body)
		if err := decoder.Decode(&req); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}

		if req.Value == nil {
			http.Error(w, "Value is Missing", http.StatusBadRequest)
			return
		}

		var ok bool

		if req.Expected != nil {
			ok = store.CAS(key, *req.Value, *req.Expected)

		} else {
			ok = store.Put(kv.Entry{Key: key, Value: *req.Value})
		}

		respond.JSON(w, http.StatusOK, resp{
			Ok: ok,
		})
	}
}

func handleDelete(store *kv.KVStore) http.HandlerFunc {
	type resp struct {
		Ok bool `json:"ok"`
	}

	return func(w http.ResponseWriter, r *http.Request) {
		key := chi.URLParam(r, "key")

		if key == "" {
			http.Error(w, "Key is Missing", http.StatusBadRequest)
			return
		}

		ok := store.Delete(key)
		respond.JSON(w, http.StatusOK, resp{

			Ok: ok,
		})
	}
}

func handleMGet(store *kv.KVStore) http.HandlerFunc {
	type reqBody struct {
		Keys []string `json:"keys"`
	}
	type resp struct {
		Values map[string]string `json:"values"`
	}

	return func(w http.ResponseWriter, r *http.Request) {
		var req reqBody
		decoder := json.NewDecoder(r.Body)
		if err := decoder.Decode(&req); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		if len(req.Keys) == 0 {
			http.Error(w, "Keys are Missing", http.StatusBadRequest)
			return
		}

		values := store.MGet(req.Keys)
		respond.JSON(w, http.StatusOK, resp{
			Values: values,
		})
	}
}

func handleMPut(store *kv.KVStore) http.HandlerFunc {
	type reqBody struct {
		Entries []kv.Entry `json:"entries"`
	}
	type resp struct {
		Ok bool `json:"ok"`
	}

	return func(w http.ResponseWriter, r *http.Request) {
		var req reqBody
		decoder := json.NewDecoder(r.Body)
		if err := decoder.Decode(&req); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		if len(req.Entries) == 0 {
			http.Error(w, "Entries are Missing", http.StatusBadRequest)
			return
		}

		ok := store.MPut(req.Entries)
		respond.JSON(w, http.StatusOK, resp{
			Ok: ok,
		})
	}
}

func handleMDelete(store *kv.KVStore) http.HandlerFunc {
	type reqBody struct {
		Keys []string `json:"keys"`
	}
	type resp struct {
		Deleted int `json:"deleted"`
	}

	return func(w http.ResponseWriter, r *http.Request) {
		var req reqBody
		decoder := json.NewDecoder(r.Body)
		if err := decoder.Decode(&req); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		if len(req.Keys) == 0 {
			http.Error(w, "Keys are Missing", http.StatusBadRequest)
			return
		}

		count := store.MDelete(req.Keys)
		respond.JSON(w, http.StatusOK, resp{
			Deleted: count,
		})
	}
}

func handleStop(store *kv.KVStore) http.HandlerFunc {
	type resp struct {
		Ok bool `json:"ok"`
	}

	return func(w http.ResponseWriter, r *http.Request) {
		if !store.Stop() {
			http.Error(w, "failed to stop store", http.StatusInternalServerError)
			return
		}

		respond.JSON(w, http.StatusOK, resp{
			Ok: true,
		})
	}
}

func handleRestart(store *kv.KVStore) http.HandlerFunc {
	type resp struct {
		Ok bool `json:"ok"`
	}

	return func(w http.ResponseWriter, r *http.Request) {
		if !store.Restart() {
			http.Error(w, "failed to restart store", http.StatusInternalServerError)
			return
		}

		respond.JSON(w, http.StatusOK, resp{
			Ok: true,
		})
	}
}
