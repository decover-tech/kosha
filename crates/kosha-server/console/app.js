// Kosha self-serve console (#345). Vanilla JS, no build step.
//
// Talks only to this server's /console/api/, sending the console session
// as `Authorization: Bearer`. All server data is rendered as text nodes,
// never as HTML.
"use strict";

(function () {
  const SESSION_KEY = "kosha.console.session";
  const ROLES = ["owner", "admin", "member", "read-only"];
  const view = document.getElementById("view");
  const who = document.getElementById("who");
  const signoutButton = document.getElementById("signout");

  // ── Session ───────────────────────────────────────────────────────────
  let memorySession = "";
  function getSession() {
    try {
      return localStorage.getItem(SESSION_KEY) || memorySession;
    } catch (_) {
      return memorySession;
    }
  }
  function setSession(token) {
    memorySession = token || "";
    try {
      if (token) localStorage.setItem(SESSION_KEY, token);
      else localStorage.removeItem(SESSION_KEY);
    } catch (_) {
      /* storage unavailable: the session lasts until reload */
    }
  }

  // ── API ───────────────────────────────────────────────────────────────
  async function api(method, path, body) {
    const headers = { "content-type": "application/json" };
    const session = getSession();
    if (session) headers.authorization = "Bearer " + session;
    const response = await fetch("/console/api/" + path, {
      method,
      headers,
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    let data = null;
    try {
      data = await response.json();
    } catch (_) {
      data = null;
    }
    if (!response.ok) {
      const message =
        (data && data.error && (data.error.message || data.error)) ||
        "request failed (" + response.status + ")";
      const error = new Error(typeof message === "string" ? message : JSON.stringify(message));
      error.status = response.status;
      throw error;
    }
    return data;
  }

  // ── DOM helpers ───────────────────────────────────────────────────────
  function el(tag, attrs, ...children) {
    const node = document.createElement(tag);
    for (const [k, v] of Object.entries(attrs || {})) {
      if (k === "onsubmit" || k === "onclick") node[k] = v;
      else if (v !== undefined && v !== null && v !== false) node.setAttribute(k, v === true ? "" : v);
    }
    for (const child of children.flat()) {
      if (child === null || child === undefined) continue;
      node.append(typeof child === "string" ? document.createTextNode(child) : child);
    }
    return node;
  }
  function when(ms) {
    return ms ? new Date(ms).toLocaleString() : "";
  }
  function table(columns, rows) {
    if (!rows.length) return el("p", { class: "muted" }, "None yet.");
    return el(
      "table",
      {},
      el("thead", {}, el("tr", {}, columns.map((c) => el("th", {}, c[0])))),
      el("tbody", {}, rows.map((r) => el("tr", {}, columns.map((c) => el("td", {}, c[1](r))))))
    );
  }
  function errorLine() {
    return el("p", { class: "error", role: "alert" });
  }
  function form(fields, label, onSubmit) {
    const error = errorLine();
    const f = el("form", { class: "inline" }, fields, el("button", { type: "submit" }, label));
    f.onsubmit = async (event) => {
      event.preventDefault();
      error.textContent = "";
      const values = {};
      for (const input of f.querySelectorAll("input, select")) values[input.name] = input.value.trim();
      try {
        await onSubmit(values);
      } catch (e) {
        if (e.status === 401) return signedOut();
        error.textContent = e.message;
      }
    };
    return el("div", {}, f, error);
  }
  function show(...nodes) {
    view.replaceChildren(...nodes);
  }

  // ── Sign-in ───────────────────────────────────────────────────────────
  function signedOut() {
    setSession("");
    who.textContent = "";
    signoutButton.hidden = true;
    const sent = el("p", { class: "muted" });
    show(
      el(
        "section",
        {},
        el("h1", {}, "Sign in or sign up"),
        el("p", { class: "muted" }, "We email you a link. New addresses get an account."),
        form(
          [
            el("input", { name: "email", type: "email", placeholder: "you@example.com", required: true }),
            el("input", { name: "display_name", placeholder: "Your name (optional)" }),
          ],
          "Email me a link",
          async (v) => {
            await api("POST", "signin", { email: v.email, display_name: v.display_name });
            sent.textContent = "Check " + v.email + " for a sign-in link. It works once, for 15 minutes.";
          }
        ),
        sent
      )
    );
  }

  async function redeem(token) {
    history.replaceState(null, "", location.pathname + "#/");
    try {
      const signedIn = await api("POST", "signin/verify", { token });
      setSession(signedIn.session_token);
    } catch (e) {
      signedOut();
      view.prepend(el("p", { class: "error", role: "alert" }, e.message));
      return false;
    }
    return true;
  }

  // ── Views ─────────────────────────────────────────────────────────────
  async function home() {
    const me = await api("GET", "me");
    who.textContent = me.user.email;
    signoutButton.hidden = false;
    show(
      el(
        "section",
        {},
        el("h1", {}, "Organizations"),
        table(
          [
            ["Name", (o) => el("a", { href: "#/orgs/" + encodeURIComponent(o.id) }, o.name)],
            ["Slug", (o) => o.slug],
            ["Your role", (o) => o.role],
          ],
          me.orgs
        ),
        el("h2", {}, "New organization"),
        form(
          [
            el("input", { name: "name", placeholder: "Name", required: true }),
            el("input", { name: "slug", placeholder: "slug (a-z, 0-9, -)", required: true }),
          ],
          "Create",
          async (v) => {
            const org = await api("POST", "orgs", { name: v.name, slug: v.slug });
            location.hash = "#/orgs/" + encodeURIComponent(org.id);
          }
        )
      ),
      el(
        "section",
        {},
        el("h2", {}, "You"),
        el("p", {}, me.user.display_name || me.user.email, " ", el("span", { class: "muted" }, "user id "), el("code", {}, me.user.id)),
        el("p", { class: "muted" }, "Share your user id with an organization admin to be added to it.")
      )
    );
  }

  async function org(orgId) {
    const base = "orgs/" + encodeURIComponent(orgId);
    const [info, members, projects, tenants] = await Promise.all([
      api("GET", base),
      api("GET", base + "/members"),
      api("GET", base + "/projects"),
      api("GET", base + "/tenants"),
    ]);
    let audit = null;
    try {
      audit = await api("GET", base + "/audit-log?limit=50");
    } catch (e) {
      if (e.status !== 403) throw e;
    }
    const projectName = (id) => (projects.projects.find((p) => p.id === id) || {}).name || "";
    const keyOut = el("div");
    const reload = () => route();

    const sections = [
      el(
        "section",
        {},
        el("p", {}, el("a", { href: "#/" }, "← Organizations")),
        el("h1", {}, info.name),
        el("p", { class: "muted" }, info.slug, " · ", info.id)
      ),
      el(
        "section",
        {},
        el("h2", {}, "Projects"),
        table(
          [
            ["Name", (p) => p.name],
            ["Slug", (p) => p.slug],
            ["Created", (p) => when(p.created_at_ms)],
          ],
          projects.projects
        ),
        form(
          [
            el("input", { name: "name", placeholder: "Name", required: true }),
            el("input", { name: "slug", placeholder: "slug", required: true }),
          ],
          "Create project",
          async (v) => {
            await api("POST", base + "/projects", v);
            reload();
          }
        )
      ),
      el(
        "section",
        {},
        el("h2", {}, "Tenants and API keys"),
        el("p", { class: "muted" }, "A tenant owns the namespaces its keys write to: ", el("code", {}, "{tenant}/..."), "."),
        table(
          [
            ["Tenant", (t) => el("code", {}, t.tenant_id)],
            ["Project", (t) => projectName(t.project_id)],
            ["Created", (t) => when(t.created_at_ms)],
            [
              "",
              (t) =>
                form([el("input", { name: "description", placeholder: "key description" })], "New API key", async (v) => {
                  const key = await api("POST", base + "/tenants/" + encodeURIComponent(t.tenant_id) + "/api-keys", v);
                  keyOut.replaceChildren(
                    el("p", {}, "New key for ", el("code", {}, key.tenant_id), ". Copy it now; it is not shown again."),
                    el("div", { class: "secret" }, key.api_key)
                  );
                }),
            ],
          ],
          tenants.tenants
        ),
        keyOut,
        form(
          [
            el(
              "select",
              { name: "project_id" },
              el("option", { value: "" }, "no project"),
              projects.projects.map((p) => el("option", { value: p.id }, p.name))
            ),
          ],
          "Create tenant",
          async (v) => {
            await api("POST", base + "/tenants", v.project_id ? { project_id: v.project_id } : {});
            reload();
          }
        )
      ),
      el(
        "section",
        {},
        el("h2", {}, "Members"),
        table(
          [
            ["Email", (m) => m.email],
            ["Name", (m) => m.display_name],
            ["Role", (m) => m.role],
            ["User id", (m) => el("code", {}, m.user_id)],
          ],
          members.members
        ),
        form(
          [
            el("input", { name: "user_id", placeholder: "user id (usr_...)", required: true }),
            el("select", { name: "role" }, ROLES.map((r) => el("option", { value: r, selected: r === "member" }, r))),
          ],
          "Add or change member",
          async (v) => {
            await api("POST", base + "/members", v);
            reload();
          }
        )
      ),
    ];
    if (audit) {
      sections.push(
        el(
          "section",
          {},
          el("h2", {}, "Audit log"),
          table(
            [
              ["When", (e) => when(e.created_at_ms)],
              ["Actor", (e) => e.actor],
              ["Action", (e) => e.action],
              ["Target", (e) => e.target_type + " " + e.target_id],
            ],
            audit.events
          )
        )
      );
    }
    show(...sections);
  }

  // ── Routing ───────────────────────────────────────────────────────────
  async function route() {
    const hash = location.hash || "#/";
    if (hash.startsWith("#signin=")) {
      if (!(await redeem(decodeURIComponent(hash.slice("#signin=".length))))) return;
    }
    if (!getSession()) return signedOut();
    try {
      const match = /^#\/orgs\/([^/]+)$/.exec(location.hash);
      if (match) await org(decodeURIComponent(match[1]));
      else await home();
    } catch (e) {
      if (e.status === 401) return signedOut();
      show(el("section", {}, el("p", { class: "error", role: "alert" }, e.message), el("p", {}, el("a", { href: "#/" }, "Back"))));
    }
  }

  signoutButton.onclick = async () => {
    try {
      await api("POST", "signout");
    } catch (_) {
      /* signed out locally either way */
    }
    signedOut();
  };
  window.addEventListener("hashchange", route);
  route();
})();
