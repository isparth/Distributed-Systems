package main

import (
	"net/http"

	"github.com/isparth/Distributed-Systems/kv-store/internal/httpapi"
	"github.com/isparth/Distributed-Systems/kv-store/internal/kv"
)

func main() {
	kv := kv.NewKVStore(kv.NoPersistence)

	handler := httpapi.NewRouter(kv)

	_ = http.ListenAndServe(":8080", handler)

}
