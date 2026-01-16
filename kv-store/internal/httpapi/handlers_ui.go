package httpapi

import "net/http"

func handleUI() http.HandlerFunc {
	page := `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>KV Lab</title>
  <style>
    :root {
      --ink: #1f1b16;
      --muted: #5a5248;
      --accent: #b85d2e;
      --accent-2: #2b6f6d;
      --panel: #fff7ee;
      --panel-2: #eef6f5;
      --line: #e1d4c2;
      --shadow: 0 10px 30px rgba(45, 33, 20, 0.08);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Palatino Linotype", "Book Antiqua", Palatino, serif;
      color: var(--ink);
      background:
        radial-gradient(1200px 600px at 10% -10%, #f7e7d2 0%, transparent 60%),
        radial-gradient(900px 500px at 110% 10%, #d6eef3 0%, transparent 50%),
        linear-gradient(180deg, #f9f4ec 0%, #f1f5f7 100%);
      min-height: 100vh;
    }
    .page {
      max-width: 1080px;
      margin: 0 auto;
      padding: 28px 18px 48px;
    }
    .hero {
      display: grid;
      gap: 12px;
      padding: 20px;
      border: 1px solid var(--line);
      border-radius: 18px;
      background: #fffdfa;
      box-shadow: var(--shadow);
      animation: rise 0.6s ease both;
    }
    .hero h1 {
      margin: 0;
      font-size: 28px;
      letter-spacing: 0.4px;
    }
    .hero p {
      margin: 0;
      color: var(--muted);
    }
    .tag {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 6px 10px;
      border-radius: 999px;
      background: #1f1b16;
      color: #fff6ec;
      font-size: 12px;
      width: fit-content;
    }
    .grid {
      display: grid;
      gap: 16px;
      margin-top: 18px;
    }
    .card {
      padding: 18px;
      border-radius: 16px;
      border: 1px solid var(--line);
      background: var(--panel);
      box-shadow: var(--shadow);
      animation: rise 0.6s ease both;
    }
    .card.alt { background: var(--panel-2); }
    .card h2 {
      margin: 0 0 10px;
      font-size: 18px;
    }
    .row {
      display: grid;
      gap: 10px;
      grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
      align-items: end;
    }
    label {
      display: grid;
      gap: 6px;
      font-size: 13px;
      color: var(--muted);
    }
    input, textarea, select {
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 10px 12px;
      font-family: "Courier New", Courier, monospace;
      font-size: 13px;
      background: #fffdf9;
      color: #2b2219;
    }
    textarea { min-height: 96px; resize: vertical; }
    .actions {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin-top: 10px;
    }
    button {
      border: none;
      border-radius: 999px;
      padding: 10px 16px;
      cursor: pointer;
      font-weight: 600;
      letter-spacing: 0.2px;
      color: #fff;
      background: var(--accent);
      transition: transform 0.15s ease, filter 0.15s ease;
    }
    button.secondary { background: var(--accent-2); }
    button.ghost {
      background: transparent;
      color: var(--accent);
      border: 1px solid var(--accent);
    }
    button:hover { transform: translateY(-1px); filter: brightness(1.05); }
    .output {
      font-family: "Courier New", Courier, monospace;
      font-size: 12px;
      background: #0f1318;
      color: #e8f0f4;
      border-radius: 12px;
      padding: 14px;
      min-height: 130px;
      overflow: auto;
      border: 1px solid #1b2730;
    }
    .status {
      font-size: 12px;
      color: var(--muted);
      margin-top: 8px;
    }
    .hint {
      font-size: 12px;
      color: var(--muted);
      margin-top: 8px;
    }
    @keyframes rise {
      from { opacity: 0; transform: translateY(10px); }
      to { opacity: 1; transform: translateY(0); }
    }
    .grid > .card:nth-child(1) { animation-delay: 0.08s; }
    .grid > .card:nth-child(2) { animation-delay: 0.16s; }
    .grid > .card:nth-child(3) { animation-delay: 0.24s; }
    .grid > .card:nth-child(4) { animation-delay: 0.32s; }
    @media (min-width: 900px) {
      .grid.duo { grid-template-columns: 1.1fr 1fr; }
    }
  </style>
</head>
<body>
  <div class="page">
    <section class="hero">
      <span class="tag">Single Node KV Lab</span>
      <h1>KV Store Control Room</h1>
      <p>Quickly exercise the single-node endpoints. Use relative paths or set a base URL.</p>
    </section>

    <section class="grid">
      <div class="card">
        <h2>Connection</h2>
        <div class="row">
          <label>Base URL (optional)
            <input id="baseUrl" placeholder="http://localhost:8080">
          </label>
          <div class="actions">
            <button id="btnHealth">Health</button>
          </div>
        </div>
        <div class="hint">Leave blank to use the current host.</div>
      </div>
    </section>

    <section class="grid duo">
      <div class="card">
        <h2>Single Key</h2>
        <div class="row">
          <label>Key
            <input id="key" placeholder="alpha">
          </label>
          <label>Value
            <input id="value" placeholder="hello">
          </label>
          <label>Expected (CAS)
            <input id="expected" placeholder="optional">
          </label>
        </div>
        <div class="actions">
          <button id="btnGet">Get</button>
          <button id="btnPut">Put / CAS</button>
          <button class="ghost" id="btnDelete">Delete</button>
        </div>
        <div class="hint">PUT uses CAS when Expected is set.</div>
      </div>

      <div class="card alt">
        <h2>Batch Ops</h2>
        <div class="row">
          <label>Keys (comma or space)
            <input id="keys" placeholder="alpha, beta, gamma">
          </label>
        </div>
        <label>Entries (one per line, key=value)
          <textarea id="entries" placeholder="alpha=1&#10;beta=2"></textarea>
        </label>
        <div class="actions">
          <button id="btnMGet">MGet</button>
          <button id="btnMPut">MPut</button>
          <button class="ghost" id="btnMDelete">MDelete</button>
        </div>
      </div>
    </section>

    <section class="grid">
      <div class="card">
        <h2>Lifecycle</h2>
        <div class="actions">
          <button class="secondary" id="btnStop">Stop</button>
          <button class="secondary" id="btnRestart">Restart</button>
        </div>
        <div class="hint">Stop clears memory; restart recovers from WAL or snapshot.</div>
      </div>
      <div class="card alt">
        <h2>Response</h2>
        <pre class="output" id="output">{ }</pre>
        <div class="status" id="status">Ready.</div>
      </div>
    </section>
  </div>

  <script>
    const output = document.getElementById("output");
    const statusEl = document.getElementById("status");
    const baseInput = document.getElementById("baseUrl");
    const keyInput = document.getElementById("key");
    const valueInput = document.getElementById("value");
    const expectedInput = document.getElementById("expected");
    const keysInput = document.getElementById("keys");
    const entriesInput = document.getElementById("entries");

    function baseUrl() {
      const base = baseInput.value.trim();
      if (!base) return "";
      return base.replace(/\/+$/, "");
    }

    function endpoint(path) {
      return baseUrl() + path;
    }

    function setStatus(message) {
      statusEl.textContent = message;
    }

    function setOutput(payload) {
      output.textContent = JSON.stringify(payload, null, 2);
    }

    function parseKeys(raw) {
      return raw.split(/[\s,]+/).map(s => s.trim()).filter(Boolean);
    }

    function parseEntries(raw) {
      const lines = raw.split(/\r?\n/).map(line => line.trim()).filter(Boolean);
      const entries = [];
      for (const line of lines) {
        const idx = line.indexOf("=");
        if (idx <= 0) continue;
        const key = line.slice(0, idx).trim();
        const value = line.slice(idx + 1).trim();
        if (key) entries.push({ key, value });
      }
      return entries;
    }

    async function callApi(method, path, body) {
      const started = performance.now();
      const options = { method, headers: {} };
      if (body !== undefined) {
        options.headers["Content-Type"] = "application/json";
        options.body = JSON.stringify(body);
      }
      setStatus("Requesting " + path + "...");
      try {
        const res = await fetch(endpoint(path), options);
        const duration = Math.round(performance.now() - started);
        const text = await res.text();
        let payload;
        try {
          payload = JSON.parse(text);
        } catch {
          payload = text;
        }
        setOutput({
          status: res.status,
          ok: res.ok,
          durationMs: duration,
          body: payload
        });
        setStatus(res.ok ? "Done." : "Request failed.");
      } catch (err) {
        setOutput({ error: String(err) });
        setStatus("Network error.");
      }
    }

    document.getElementById("btnHealth").addEventListener("click", () => {
      callApi("GET", "/kv/healthz");
    });

    document.getElementById("btnGet").addEventListener("click", () => {
      const key = keyInput.value.trim();
      if (!key) {
        setStatus("Key is required.");
        return;
      }
      callApi("GET", "/kv/" + encodeURIComponent(key));
    });

    document.getElementById("btnPut").addEventListener("click", () => {
      const key = keyInput.value.trim();
      const value = valueInput.value.trim();
      const expected = expectedInput.value.trim();
      if (!key || !value) {
        setStatus("Key and value are required.");
        return;
      }
      const body = { value: value };
      if (expected) body.expected = expected;
      callApi("PUT", "/kv/" + encodeURIComponent(key), body);
    });

    document.getElementById("btnDelete").addEventListener("click", () => {
      const key = keyInput.value.trim();
      if (!key) {
        setStatus("Key is required.");
        return;
      }
      callApi("DELETE", "/kv/" + encodeURIComponent(key));
    });

    document.getElementById("btnMGet").addEventListener("click", () => {
      const keys = parseKeys(keysInput.value);
      if (keys.length === 0) {
        setStatus("Provide at least one key.");
        return;
      }
      callApi("POST", "/kv/mget", { keys });
    });

    document.getElementById("btnMPut").addEventListener("click", () => {
      const entries = parseEntries(entriesInput.value);
      if (entries.length === 0) {
        setStatus("Provide at least one entry.");
        return;
      }
      callApi("PUT", "/kv/mput", { entries });
    });

    document.getElementById("btnMDelete").addEventListener("click", () => {
      const keys = parseKeys(keysInput.value);
      if (keys.length === 0) {
        setStatus("Provide at least one key.");
        return;
      }
      callApi("POST", "/kv/mdelete", { keys });
    });

    document.getElementById("btnStop").addEventListener("click", () => {
      callApi("POST", "/kv/stop");
    });

    document.getElementById("btnRestart").addEventListener("click", () => {
      callApi("POST", "/kv/restart");
    });
  </script>
</body>
</html>`

	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(page))
	}
}
