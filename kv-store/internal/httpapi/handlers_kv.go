package httpapi

import (
	"net/http"
	"time"

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
