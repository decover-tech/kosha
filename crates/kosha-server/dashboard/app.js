// Kosha read-only dashboard (#214). Vanilla JS, no build step.
//
// Every data call goes to this server's ordinary API with the key the
// operator pasted, sent as `Authorization: Bearer`. The key lives in this
// tab's sessionStorage only. All server data is rendered as text nodes,
// never as HTML.
"use strict";

(function () {
  const KEY_STORAGE = "kosha.dashboard.apiKey";
  const SVG_NS = "http://www.w3.org/2000/svg";
  const EXAMPLE_QUERY = {
    query_text: "breach",
    max_results: 10,
    highlight: { fields: { content: {}, title: {} } },
    timings: true,
  };

  const view = document.getElementById("view");

  // ── Key handling ──────────────────────────────────────────────────────
  function getKey() {
    try {
      return sessionStorage.getItem(KEY_STORAGE) || "";
    } catch (_) {
      return "";
    }
  }
  function setKey(key) {
    try {
      if (key) sessionStorage.setItem(KEY_STORAGE, key);
      else sessionStorage.removeItem(KEY_STORAGE);
    } catch (_) {
      /* storage unavailable: the key lasts until reload */
    }
    memoryKey = key;
    showKeyState();
  }
  let memoryKey = getKey();
  function currentKey() {
    return getKey() || memoryKey;
  }
  function showKeyState() {
    document.getElementById("key-state").textContent = currentKey()
      ? "key set"
      : "no key";
  }

  // ── DOM helpers ───────────────────────────────────────────────────────
  function el(tag, attrs, ...children) {
    const node = document.createElement(tag);
    for (const [k, v] of Object.entries(attrs || {})) {
      if (v === undefined || v === null || v === false) continue;
      if (k === "class") node.className = v;
      else if (k === "text") node.textContent = v;
      else if (k.startsWith("on")) node.addEventListener(k.slice(2), v);
      else node.setAttribute(k, v === true ? "" : String(v));
    }
    for (const child of children.flat()) {
      if (child === undefined || child === null || child === false) continue;
      node.append(child instanceof Node ? child : document.createTextNode(String(child)));
    }
    return node;
  }
  function svg(tag, attrs) {
    const node = document.createElementNS(SVG_NS, tag);
    for (const [k, v] of Object.entries(attrs || {})) node.setAttribute(k, String(v));
    return node;
  }
  function render(...nodes) {
    view.replaceChildren(...nodes);
  }
  function fmtNum(n) {
    return n === undefined || n === null ? "-" : Number(n).toLocaleString();
  }
  function fmtMs(n) {
    if (n === undefined || n === null) return "-";
    const v = Number(n);
    return (v < 10 ? v.toFixed(2) : v.toFixed(1)) + " ms";
  }
  function nsHref(kind, ns) {
    return "#/" + kind + "/" + encodeURIComponent(ns);
  }
  function errorBox(message) {
    return el("div", { class: "error", role: "alert", text: message });
  }
  function kv(pairs) {
    return el(
      "div",
      { class: "kv" },
      pairs.map(([k, v]) => el("div", {}, el("span", { class: "k", text: k }), el("span", { class: "v", text: v })))
    );
  }

  // ── API ───────────────────────────────────────────────────────────────
  async function api(method, path, body) {
    const key = currentKey();
    if (!key) throw new Error("Paste an API key above to load data.");
    const init = { method, headers: { Authorization: "Bearer " + key } };
    if (body !== undefined) {
      init.headers["Content-Type"] = "application/json";
      init.body = body;
    }
    const resp = await fetch(path, init);
    const text = await resp.text();
    let data = null;
    try {
      data = text ? JSON.parse(text) : null;
    } catch (_) {
      data = null;
    }
    if (!resp.ok) {
      const reason = data && data.error ? data.error : text || resp.statusText;
      throw new Error(resp.status + ": " + reason);
    }
    return data;
  }
  function nsPath(ns, action) {
    return "/v1/namespaces/" + encodeURIComponent(ns) + "/" + action;
  }

  // ── Namespace list ────────────────────────────────────────────────────
  async function showNamespaces() {
    render(el("h1", { text: "Namespaces" }), el("p", { class: "muted", text: "Loading…" }));
    let data;
    try {
      data = await api("GET", "/v1/namespaces");
    } catch (e) {
      render(el("h1", { text: "Namespaces" }), errorBox(e.message));
      return;
    }
    const rows = (data.namespaces || []).map((ns) =>
      el(
        "tr",
        {},
        el("td", {}, el("a", { href: nsHref("ns", ns.name), class: "mono", text: ns.name })),
        el("td", { class: "num", text: fmtNum(ns.documents) }),
        el("td", { class: "num", text: fmtNum(ns.segments) }),
        el("td", { class: "num", text: fmtNum(ns.manifest_version) }),
        el("td", {}, el("a", { href: nsHref("console", ns.name), text: "query" }))
      )
    );
    render(
      el("h1", { text: "Namespaces" }),
      el(
        "div",
        { class: "panel" },
        rows.length
          ? el(
              "table",
              {},
              el(
                "thead",
                {},
                el(
                  "tr",
                  {},
                  el("th", { text: "Name" }),
                  el("th", { class: "num", text: "Documents" }),
                  el("th", { class: "num", text: "Segments" }),
                  el("th", { class: "num", text: "Manifest version" }),
                  el("th", { text: "" })
                )
              ),
              el("tbody", {}, rows)
            )
          : el("p", { class: "muted", text: "This key's tenant has no namespaces yet." })
      )
    );
  }

  // ── Namespace detail ──────────────────────────────────────────────────
  function sparkline(samples) {
    const width = 600;
    const height = 80;
    const pad = 4;
    const root = svg("svg", {
      class: "spark",
      viewBox: `0 0 ${width} ${height}`,
      preserveAspectRatio: "none",
      role: "img",
      "aria-label": "Total latency of recent searches, oldest to newest",
    });
    if (samples.length < 2) return root;
    const max = Math.max(...samples.map((s) => s.total_ms), 1);
    const x = (i) => pad + (i * (width - 2 * pad)) / (samples.length - 1);
    const y = (v) => height - pad - (v / max) * (height - 2 * pad);
    const points = samples.map((s, i) => `${x(i).toFixed(1)},${y(s.total_ms).toFixed(1)}`);
    root.append(
      svg("polygon", {
        class: "area",
        points: `${x(0)},${height - pad} ${points.join(" ")} ${x(samples.length - 1)},${height - pad}`,
      }),
      svg("polyline", { class: "line", points: points.join(" ") })
    );
    samples.forEach((s, i) => {
      if (s.degraded) root.append(svg("circle", { class: "bad-dot", cx: x(i), cy: y(s.total_ms), r: 3 }));
    });
    return root;
  }

  async function showNamespace(ns) {
    const title = el("h1", {}, "Namespace ", el("span", { class: "mono", text: ns }));
    render(title, el("p", { class: "muted", text: "Loading…" }));
    let stats;
    try {
      stats = await api("GET", nsPath(ns, "stats"));
    } catch (e) {
      render(title, errorBox(e.message));
      return;
    }
    const parts = [
      title,
      el("p", {}, el("a", { href: nsHref("console", ns), text: "Open in query console →" })),
      el(
        "div",
        { class: "panel" },
        kv([
          ["Documents", fmtNum(stats.documents)],
          ["Segments", fmtNum(stats.segments)],
          ["Manifest version", fmtNum(stats.manifest_version)],
          ["Shards", stats.shards ? fmtNum(stats.shards) : "unsharded"],
        ])
      ),
    ];

    // Query pods follow the control store; ingest pods hold unindexed writes.
    if ("manifest_lag_seconds" in stats || "stored_manifest_version" in stats) {
      const lag = stats.manifest_lag_seconds;
      parts.push(
        el("h2", { text: "Manifest follow (query pod)" }),
        el(
          "div",
          { class: "panel" },
          kv([
            ["Manifest lag", lag === null || lag === undefined ? "never refreshed" : lag.toFixed(2) + " s"],
            ["Stored manifest version", fmtNum(stats.stored_manifest_version)],
            ["Last refresh", stats.last_refresh_at ? String(stats.last_refresh_at) : "-"],
          ])
        )
      );
    }
    if ("unindexed_documents" in stats) {
      parts.push(
        el("h2", { text: "Unindexed writes" }),
        el(
          "div",
          { class: "panel" },
          kv([
            ["Documents", fmtNum(stats.unindexed_documents)],
            ["Bytes", fmtNum(stats.unindexed_bytes)],
          ])
        )
      );
    }
    if (stats.branch) {
      parts.push(
        el("h2", { text: "Branch" }),
        el(
          "div",
          { class: "panel" },
          kv([
            ["Parent", stats.branch.parent],
            ["Parent version", fmtNum(stats.branch.parent_version)],
            ["Shared segments", fmtNum(stats.branch.shared_segments)],
          ])
        )
      );
    }
    if (Array.isArray(stats.shard_stats)) {
      parts.push(
        el("h2", { text: "Shards" }),
        el(
          "div",
          { class: "panel" },
          el(
            "table",
            {},
            el(
              "thead",
              {},
              el(
                "tr",
                {},
                el("th", { text: "Shard" }),
                el("th", { class: "num", text: "Documents" }),
                el("th", { class: "num", text: "Segments" }),
                el("th", { class: "num", text: "Manifest version" })
              )
            ),
            el(
              "tbody",
              {},
              stats.shard_stats.map((s) =>
                el(
                  "tr",
                  {},
                  el("td", { class: "mono", text: String(s.shard) }),
                  el("td", { class: "num", text: fmtNum(s.documents) }),
                  el("td", { class: "num", text: fmtNum(s.segments) }),
                  el("td", { class: "num", text: fmtNum(s.manifest_version) })
                )
              )
            )
          )
        )
      );
    }

    const recent = stats.recent_queries;
    parts.push(el("h2", { text: "Recent searches (this pod)" }));
    if (!recent) {
      parts.push(el("div", { class: "panel muted", text: "Not reported by this server." }));
    } else if (!recent.count) {
      parts.push(el("div", { class: "panel muted", text: "No searches recorded since this pod started." }));
    } else {
      parts.push(
        el(
          "div",
          { class: "panel" },
          kv([
            ["Searches", `${recent.count} (last ${recent.capacity})`],
            ["p50", fmtMs(recent.p50_ms)],
            ["p95", fmtMs(recent.p95_ms)],
            ["max", fmtMs(recent.max_ms)],
            ["kNN degraded", fmtNum(recent.degraded)],
          ]),
          sparkline(recent.samples || []),
          el("p", { class: "muted", text: "Total latency per search, oldest to newest. Red dots: kNN degraded." })
        )
      );
    }
    render(...parts);
  }

  // ── Query console ─────────────────────────────────────────────────────
  // Highlight fragments carry the default `<b>`/`</b>` tags around matches.
  // Split on them and build text nodes, so document text is never parsed
  // as HTML.
  function snippetNode(fragment) {
    const out = el("span", { class: "snippet" });
    const parts = String(fragment).split(/(<b>|<\/b>)/);
    let inMark = false;
    for (const part of parts) {
      if (part === "<b>") inMark = true;
      else if (part === "</b>") inMark = false;
      else if (part) out.append(inMark ? el("mark", { text: part }) : document.createTextNode(part));
    }
    return out;
  }
  function hitSnippet(hit) {
    if (hit.highlight && typeof hit.highlight === "object") {
      for (const [field, frags] of Object.entries(hit.highlight)) {
        if (Array.isArray(frags) && frags.length) {
          return el("span", {}, el("span", { class: "muted", text: field + ": " }), snippetNode(frags[0]));
        }
      }
    }
    if (Array.isArray(hit.highlights) && hit.highlights.length) return snippetNode(hit.highlights[0]);
    const field = (hit.fields || []).find((f) => typeof f.value === "string" && f.value);
    if (!field) return el("span", { class: "muted", text: "-" });
    const text = field.value.length > 200 ? field.value.slice(0, 200) + "…" : field.value;
    return el("span", {}, el("span", { class: "muted", text: field.name + ": " }), text);
  }

  function timingPanel(t) {
    const phases = [
      ["hydrate", t.hydrate_ms],
      ["queue", t.queue_ms],
      ["admit", t.admit_ms],
      ["bm25 stats", t.bm25_stats_ms],
      ["score", t.score_ms],
      ["materialize", t.materialize_ms],
    ];
    const total = Math.max(t.total_ms || 0, 0.001);
    const accounted = phases.reduce((sum, [, v]) => sum + (v || 0), 0);
    const other = Math.max(total - accounted, 0);
    const shown = phases.concat([["other", other]]);
    const bar = el("div", { class: "timing-bar", role: "img", "aria-label": "Timing breakdown" });
    shown.forEach(([name, v], i) => {
      if (!v) return;
      const seg = el("span", { class: "seg-" + i, title: `${name}: ${fmtMs(v)}` });
      seg.style.width = ((v / total) * 100).toFixed(2) + "%";
      bar.append(seg);
    });
    return el(
      "div",
      { class: "panel" },
      el("h2", { text: t.cached ? "Timing (served from the result cache)" : "Timing" }),
      bar,
      el(
        "table",
        {},
        el(
          "tbody",
          {},
          el("tr", {}, el("th", { text: "total" }), el("td", { class: "num", text: fmtMs(t.total_ms) })),
          shown.map(([name, v], i) =>
            el(
              "tr",
              {},
              el("td", {}, el("span", { class: "swatch seg-" + i }), name),
              el("td", { class: "num", text: fmtMs(v) })
            )
          )
        )
      )
    );
  }

  function resultNodes(result, elapsedMs) {
    const nodes = [];
    const degraded = result.knn_degraded_segments || 0;
    if (degraded > 0) {
      nodes.push(
        el(
          "div",
          { class: "banner bad", role: "alert" },
          `kNN degraded: ${degraded} segment(s) failed their vector search and contributed no neighbors. ` +
            "These results can be missing hits even though the request returned 200."
        )
      );
    }
    if (result.rerank_degraded) {
      nodes.push(
        el(
          "div",
          { class: "banner warn" },
          "Rerank degraded: results are in first-stage order" +
            (result.rerank_degraded_reason ? ` (${result.rerank_degraded_reason})` : "") + "."
        )
      );
    }
    if (result.consistency_fallback) {
      nodes.push(el("div", { class: "banner warn", text: "Strong consistency fell back to eventual: " + result.consistency_fallback }));
    }
    const summary = [
      ["Total hits", (result.total_hits_relation === "gte" ? "≥ " : "") + fmtNum(result.total_hits)],
      ["Returned", fmtNum((result.results || []).length)],
      ["Round trip", fmtMs(elapsedMs)],
    ];
    if (result.knn_mode) summary.push(["kNN mode", result.knn_mode]);
    if (result.knn_degraded_segments !== undefined) summary.push(["kNN degraded segments", fmtNum(degraded)]);
    nodes.push(el("div", { class: "panel" }, kv(summary)));
    if (result.timings) nodes.push(timingPanel(result.timings));
    else
      nodes.push(
        el("div", { class: "panel muted", text: 'No timing breakdown: add "timings": true to the request.' })
      );
    const hits = result.results || [];
    nodes.push(
      el(
        "div",
        { class: "panel" },
        el("h2", { text: "Hits" }),
        hits.length
          ? el(
              "table",
              {},
              el(
                "thead",
                {},
                el(
                  "tr",
                  {},
                  el("th", { class: "num", text: "#" }),
                  el("th", { text: "ID" }),
                  el("th", { class: "num", text: "Score" }),
                  el("th", { text: "Snippet" })
                )
              ),
              el(
                "tbody",
                {},
                hits.map((hit, i) =>
                  el(
                    "tr",
                    {},
                    el("td", { class: "num", text: String(i + 1) }),
                    el("td", { class: "mono", text: String(hit.doc_id) }),
                    el("td", { class: "num mono", text: Number(hit.score).toFixed(4) }),
                    el("td", {}, hitSnippet(hit))
                  )
                )
              )
            )
          : el("p", { class: "muted", text: "No hits." })
      )
    );
    return nodes;
  }

  async function showConsole(ns) {
    const nsInput = el("input", { id: "console-ns", class: "mono", value: ns || "", placeholder: "namespace", list: "ns-options" });
    const options = el("datalist", { id: "ns-options" });
    const query = el("textarea", { id: "query", spellcheck: "false" });
    query.value = JSON.stringify(EXAMPLE_QUERY, null, 2);
    const run = el("button", { type: "submit", text: "Search" });
    const reset = el("button", { type: "button", class: "secondary", text: "Reset example" });
    const out = el("div", {});
    reset.addEventListener("click", () => {
      query.value = JSON.stringify(EXAMPLE_QUERY, null, 2);
    });
    const form = el(
      "form",
      { class: "console panel" },
      el("div", { class: "console-row" }, el("label", { for: "console-ns", text: "Namespace" }), nsInput, options),
      el("label", { for: "query", class: "muted", text: "Request body for POST /v1/namespaces/{ns}/search" }),
      query,
      el("div", { class: "console-row" }, run, reset)
    );
    form.addEventListener("submit", async (event) => {
      event.preventDefault();
      const name = nsInput.value.trim();
      if (!name) {
        out.replaceChildren(errorBox("Choose a namespace."));
        return;
      }
      let body;
      try {
        body = JSON.stringify(JSON.parse(query.value));
      } catch (e) {
        out.replaceChildren(errorBox("Request body is not valid JSON: " + e.message));
        return;
      }
      if (location.hash !== nsHref("console", name)) history.replaceState(null, "", nsHref("console", name));
      run.disabled = true;
      out.replaceChildren(el("p", { class: "muted", text: "Searching…" }));
      const started = performance.now();
      try {
        const result = await api("POST", nsPath(name, "search"), body);
        out.replaceChildren(...resultNodes(result || {}, performance.now() - started));
      } catch (e) {
        out.replaceChildren(errorBox(e.message));
      } finally {
        run.disabled = false;
      }
    });
    render(el("h1", { text: "Query console" }), form, out);
    // Offer the tenant's namespaces as suggestions; failures are not fatal.
    try {
      const data = await api("GET", "/v1/namespaces");
      options.replaceChildren(...(data.namespaces || []).map((n) => el("option", { value: n.name })));
      if (!nsInput.value && data.namespaces && data.namespaces.length) nsInput.value = data.namespaces[0].name;
    } catch (_) {
      /* no key yet, or listing failed: the field stays free text */
    }
  }

  // ── Usage ─────────────────────────────────────────────────────────────
  // `GET /v1/usage` (#348): the key's tenant's metered usage per namespace.
  const USAGE_WINDOWS = [
    ["1", "Last 24 hours"],
    ["7", "Last 7 days"],
    ["30", "Last 30 days"],
  ];
  const USAGE_COLUMNS = [
    ["queries", "Queries", fmtNum],
    ["bytes_queried", "Bytes queried", fmtBytes],
    ["bytes_returned", "Bytes returned", fmtBytes],
    ["bytes_written", "Bytes written", fmtBytes],
    ["documents_written", "Documents written", fmtNum],
    ["vector_dimensions_written", "Vector dims written", fmtNum],
    ["stored_bytes_latest", "Stored bytes", fmtBytes],
    ["vector_dimensions_latest", "Stored vector dims", fmtNum],
  ];
  function fmtBytes(n) {
    if (n === undefined || n === null) return "-";
    const units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    let v = Number(n);
    let i = 0;
    while (v >= 1024 && i < units.length - 1) {
      v /= 1024;
      i += 1;
    }
    return (i === 0 ? String(v) : v.toFixed(v < 10 ? 2 : 1)) + " " + units[i];
  }

  async function showUsage(days) {
    const windowDays = USAGE_WINDOWS.some(([d]) => d === days) ? days : "30";
    const picker = el(
      "select",
      { id: "usage-window", "aria-label": "Usage window" },
      USAGE_WINDOWS.map(([d, label]) => el("option", { value: d, selected: d === windowDays, text: label }))
    );
    picker.addEventListener("change", () => {
      location.hash = "#/usage/" + picker.value;
    });
    const title = el("h1", { text: "Usage" });
    const controls = el("p", {}, picker);
    render(title, controls, el("p", { class: "muted", text: "Loading…" }));
    const to = Math.floor(Date.now() / 1000) + 1;
    const from = to - Number(windowDays) * 86400;
    let data;
    try {
      data = await api("GET", "/v1/usage?from=" + from + "&to=" + to);
    } catch (e) {
      const off = /^404:/.test(e.message);
      render(title, controls, off ? el("div", { class: "panel muted", text: "Usage metering is off on this server (KOSHA_USAGE_METERING=on enables it)." }) : errorBox(e.message));
      return;
    }
    const total = data.total || {};
    const namespaces = data.namespaces || [];
    render(
      title,
      controls,
      el(
        "div",
        { class: "panel" },
        kv(USAGE_COLUMNS.map(([key, label, fmt]) => [label, fmt(total[key])]))
      ),
      el(
        "div",
        { class: "panel" },
        el("h2", { text: "Per namespace" }),
        namespaces.length
          ? el(
              "table",
              {},
              el(
                "thead",
                {},
                el("tr", {}, el("th", { text: "Namespace" }), USAGE_COLUMNS.map(([, label]) => el("th", { class: "num", text: label })))
              ),
              el(
                "tbody",
                {},
                namespaces.map((u) =>
                  el(
                    "tr",
                    {},
                    el("td", { class: "mono", text: u.namespace }),
                    USAGE_COLUMNS.map(([key, , fmt]) => el("td", { class: "num", text: fmt(u[key]) }))
                  )
                )
              )
            )
          : el("p", { class: "muted", text: "No usage recorded in this window." })
      ),
      el(
        "p",
        { class: "muted", text: "Stored figures are the latest hourly snapshot in the window. Usage still spooled on other pods appears after their next flush." }
      )
    );
  }

  // ── Router ────────────────────────────────────────────────────────────
  function route() {
    const hash = location.hash.replace(/^#/, "") || "/";
    const parts = hash.split("/").filter(Boolean);
    const section = parts[0] || "";
    const arg = parts[1] ? decodeURIComponent(parts[1]) : "";
    document.querySelectorAll("nav a").forEach((a) => {
      const current = section === "console" || section === "usage" ? section : "namespaces";
      const active = a.dataset.nav === current;
      a.classList.toggle("active", active);
    });
    if (section === "ns" && arg) return showNamespace(arg);
    if (section === "console") return showConsole(arg);
    if (section === "usage") return showUsage(arg);
    return showNamespaces();
  }

  document.getElementById("key-form").addEventListener("submit", (event) => {
    event.preventDefault();
    const input = document.getElementById("api-key");
    const key = input.value.trim();
    if (key) setKey(key);
    input.value = "";
    route();
  });
  document.getElementById("forget-key").addEventListener("click", () => {
    setKey("");
    route();
  });
  window.addEventListener("hashchange", route);
  showKeyState();
  route();
})();
