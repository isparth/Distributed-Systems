package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/isparth/Distributed-Systems/kv-store/internal/distributedkv"
	"github.com/isparth/Distributed-Systems/kv-store/internal/kvsm"
	"github.com/isparth/Distributed-Systems/kv-store/internal/types"
)

// mockNode implements distributedkv.RaftNodeIface for testing.
type mockNode struct {
	leader     bool
	leaderHint types.LeaderHint
	sm         *kvsm.KVStateMachine
}

func (m *mockNode) Propose(_ context.Context, cmd types.Command) (types.ApplyResult, error) {
	return m.sm.Apply(cmd), nil
}

func (m *mockNode) IsLeader() bool { return m.leader }

func (m *mockNode) LeaderHint() types.LeaderHint { return m.leaderHint }

func setupLeader() *httptest.Server {
	sm := kvsm.New()
	node := &mockNode{leader: true, sm: sm}
	dkv := distributedkv.New(node, sm, distributedkv.Config{})
	srv := New(dkv)
	return httptest.NewServer(srv.Handler())
}

func setupFollower() *httptest.Server {
	sm := kvsm.New()
	node := &mockNode{
		leader: false,
		sm:     sm,
		leaderHint: types.LeaderHint{
			LeaderID:   "leader",
			LeaderAddr: "http://leader:8080",
		},
	}
	dkv := distributedkv.New(node, sm, distributedkv.Config{})
	srv := New(dkv)
	return httptest.NewServer(srv.Handler())
}

func noRedirectClient() *http.Client {
	return &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

func TestHTTPAPI_Healthz(t *testing.T) {
	ts := setupLeader()
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var body map[string]string
	json.NewDecoder(resp.Body).Decode(&body)
	if body["status"] != "ok" {
		t.Fatalf("expected ok, got %s", body["status"])
	}
}

func TestHTTPAPI_PutGetDelete(t *testing.T) {
	ts := setupLeader()
	defer ts.Close()
	client := ts.Client()

	// PUT
	putBody, _ := json.Marshal(map[string]interface{}{
		"client_id": "c1", "seq": 1, "value": "hello",
	})
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/kv/mykey", bytes.NewReader(putBody))
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("put: expected 200, got %d", resp.StatusCode)
	}

	// GET
	resp, err = http.Get(ts.URL + "/kv/mykey")
	if err != nil {
		t.Fatal(err)
	}
	var getResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&getResp)
	resp.Body.Close()
	if getResp["value"] != "hello" {
		t.Fatalf("get: expected hello, got %v", getResp["value"])
	}

	// GET missing
	resp, err = http.Get(ts.URL + "/kv/nokey")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Fatalf("get missing: expected 404, got %d", resp.StatusCode)
	}

	// DELETE
	req, _ = http.NewRequest(http.MethodDelete, ts.URL+"/kv/mykey", nil)
	resp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var delResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&delResp)
	resp.Body.Close()
	if delResp["deleted"].(float64) != 1 {
		t.Fatalf("delete: expected deleted=1, got %v", delResp["deleted"])
	}

	// GET after delete
	resp, _ = http.Get(ts.URL + "/kv/mykey")
	resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Fatalf("get after delete: expected 404, got %d", resp.StatusCode)
	}
}

func TestHTTPAPI_BatchEndpoints(t *testing.T) {
	ts := setupLeader()
	defer ts.Close()
	client := ts.Client()

	// MPut
	mputBody, _ := json.Marshal(map[string]interface{}{
		"client_id": "c1", "seq": 1,
		"entries": []map[string]string{
			{"key": "a", "value": "1"},
			{"key": "b", "value": "2"},
		},
	})
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/kv/mput", bytes.NewReader(mputBody))
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("mput: expected 200, got %d", resp.StatusCode)
	}

	// MGet
	mgetBody, _ := json.Marshal(map[string]interface{}{
		"keys": []string{"a", "b", "missing"},
	})
	resp, err = http.Post(ts.URL+"/kv/mget", "application/json", bytes.NewReader(mgetBody))
	if err != nil {
		t.Fatal(err)
	}
	var mgetResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&mgetResp)
	resp.Body.Close()
	vals := mgetResp["values"].(map[string]interface{})
	if vals["a"] != "1" || vals["b"] != "2" {
		t.Fatalf("mget mismatch: %v", vals)
	}
	if _, exists := vals["missing"]; exists {
		t.Fatal("missing key should not be in values")
	}

	// MDelete
	mdelBody, _ := json.Marshal(map[string]interface{}{
		"client_id": "c1", "seq": 2,
		"keys": []string{"a"},
	})
	resp, err = http.Post(ts.URL+"/kv/mdelete", "application/json", bytes.NewReader(mdelBody))
	if err != nil {
		t.Fatal(err)
	}
	var mdelResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&mdelResp)
	resp.Body.Close()
	if mdelResp["deleted"].(float64) != 1 {
		t.Fatalf("mdelete: expected deleted=1, got %v", mdelResp["deleted"])
	}
}

func TestHTTPAPI_WriteToFollower_Returns307(t *testing.T) {
	ts := setupFollower()
	defer ts.Close()
	client := noRedirectClient()

	// PUT to follower should return 307
	putBody, _ := json.Marshal(map[string]interface{}{
		"client_id": "c1", "seq": 1, "value": "hello",
	})
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/kv/mykey", bytes.NewReader(putBody))
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 307 {
		t.Fatalf("put to follower: expected 307, got %d", resp.StatusCode)
	}

	var body map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&body)
	if body["error"] != "not_leader" {
		t.Fatalf("expected not_leader, got %v", body["error"])
	}
	hint := body["leader_hint"].(map[string]interface{})
	if hint["leader_id"] != "leader" {
		t.Fatalf("expected leader hint, got %v", hint)
	}
}

func TestHTTPAPI_WriteToLeader_Returns200(t *testing.T) {
	ts := setupLeader()
	defer ts.Close()

	putBody, _ := json.Marshal(map[string]interface{}{
		"client_id": "c1", "seq": 1, "value": "hello",
	})
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/kv/mykey", bytes.NewReader(putBody))
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("put to leader: expected 200, got %d", resp.StatusCode)
	}
}

func TestHTTPAPI_ReadFromFollower_Works(t *testing.T) {
	// Reads should work on followers (stale reads)
	sm := kvsm.New()
	sm.Apply(types.Command{Op: types.OpPut, Key: "k1", Value: "v1"})
	node := &mockNode{leader: false, sm: sm}
	dkv := distributedkv.New(node, sm, distributedkv.Config{})
	srv := New(dkv)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/kv/k1")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("read from follower: expected 200, got %d", resp.StatusCode)
	}
	var body map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&body)
	if body["value"] != "v1" {
		t.Fatalf("expected v1, got %v", body["value"])
	}
}
