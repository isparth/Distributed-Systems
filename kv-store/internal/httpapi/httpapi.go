package httpapi

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/isparth/Distributed-Systems/kv-store/internal/distributedkv"
	"github.com/isparth/Distributed-Systems/kv-store/internal/types"
)

// Server serves the HTTP API backed by a DistributedKV.
type Server struct {
	dkv *distributedkv.DistributedKV
}

// New creates a new HTTP API server.
func New(dkv *distributedkv.DistributedKV) *Server {
	return &Server{dkv: dkv}
}

// Handler returns the HTTP handler with all routes.
func (s *Server) Handler() http.Handler {
	r := chi.NewRouter()
	r.Get("/healthz", s.Healthz)
	r.Get("/status", s.Status)
	r.Get("/kv", s.ListKeys)
	r.Get("/kv/{key}", s.GetKey)
	r.Put("/kv/{key}", s.PutKey)
	r.Delete("/kv/{key}", s.DeleteKey)
	r.Post("/kv/mget", s.MGet)
	r.Post("/kv/mput", s.MPut)
	r.Post("/kv/mdelete", s.MDelete)
	return r
}

func (s *Server) Healthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) Status(w http.ResponseWriter, r *http.Request) {
	status := s.dkv.Status()
	writeJSON(w, http.StatusOK, status)
}

func (s *Server) ListKeys(w http.ResponseWriter, r *http.Request) {
	all := s.dkv.All()
	writeJSON(w, http.StatusOK, map[string]interface{}{"ok": true, "data": all})
}

func (s *Server) GetKey(w http.ResponseWriter, r *http.Request) {
	key := chi.URLParam(r, "key")
	v, ok, err := s.dkv.Get(r.Context(), key)
	if err != nil {
		// ReadIndex failed - likely not leader or timeout
		writeError(w, http.StatusServiceUnavailable, "read_index_failed", err.Error())
		return
	}
	if !ok {
		writeError(w, http.StatusNotFound, "not_found", "key not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"ok": true, "value": v})
}

func (s *Server) PutKey(w http.ResponseWriter, r *http.Request) {
	if s.redirectIfNotLeader(w) {
		return
	}
	key := chi.URLParam(r, "key")
	var body struct {
		ClientID string `json:"client_id"`
		Seq      uint64 `json:"seq"`
		Value    string `json:"value"`
	}
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", "invalid JSON")
		return
	}
	cmd := types.Command{
		ClientID: body.ClientID,
		Seq:      body.Seq,
		Key:      key,
		Value:    body.Value,
	}
	res, err := s.dkv.Put(r.Context(), cmd)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	if !res.Ok {
		writeJSON(w, http.StatusBadRequest, res)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) DeleteKey(w http.ResponseWriter, r *http.Request) {
	if s.redirectIfNotLeader(w) {
		return
	}
	key := chi.URLParam(r, "key")
	var body struct {
		ClientID string `json:"client_id"`
		Seq      uint64 `json:"seq"`
	}
	_ = decodeJSON(r, &body)
	cmd := types.Command{
		ClientID: body.ClientID,
		Seq:      body.Seq,
		Key:      key,
	}
	res, err := s.dkv.Delete(r.Context(), cmd)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) MGet(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Keys []string `json:"keys"`
	}
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", "invalid JSON")
		return
	}
	if len(body.Keys) == 0 {
		writeError(w, http.StatusBadRequest, "bad_request", "keys is required")
		return
	}
	vals, err := s.dkv.MGet(r.Context(), body.Keys)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "read_index_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"ok": true, "values": vals})
}

func (s *Server) MPut(w http.ResponseWriter, r *http.Request) {
	if s.redirectIfNotLeader(w) {
		return
	}
	var body struct {
		ClientID string        `json:"client_id"`
		Seq      uint64        `json:"seq"`
		Entries  []types.Entry `json:"entries"`
	}
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", "invalid JSON")
		return
	}
	cmd := types.Command{
		ClientID: body.ClientID,
		Seq:      body.Seq,
		Entries:  body.Entries,
	}
	res, err := s.dkv.MPut(r.Context(), cmd)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	if !res.Ok {
		writeJSON(w, http.StatusBadRequest, res)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) MDelete(w http.ResponseWriter, r *http.Request) {
	if s.redirectIfNotLeader(w) {
		return
	}
	var body struct {
		ClientID string   `json:"client_id"`
		Seq      uint64   `json:"seq"`
		Keys     []string `json:"keys"`
	}
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", "invalid JSON")
		return
	}
	cmd := types.Command{
		ClientID: body.ClientID,
		Seq:      body.Seq,
		Keys:     body.Keys,
	}
	res, err := s.dkv.MDelete(r.Context(), cmd)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	if !res.Ok {
		writeJSON(w, http.StatusBadRequest, res)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// redirectIfNotLeader returns 307 with leader hint if this node is not the leader.
func (s *Server) redirectIfNotLeader(w http.ResponseWriter) bool {
	if s.dkv.IsLeader() {
		return false
	}
	hint := s.dkv.LeaderHint()
	writeJSON(w, http.StatusTemporaryRedirect, map[string]interface{}{
		"error":       "not_leader",
		"leader_hint": hint,
	})
	return true
}

// --- JSON helpers ---

func decodeJSON(r *http.Request, dst interface{}) error {
	return json.NewDecoder(r.Body).Decode(dst)
}

func writeJSON(w http.ResponseWriter, status int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, types.ApplyResult{Ok: false, ErrCode: code, ErrMsg: msg})
}
