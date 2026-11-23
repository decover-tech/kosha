// Kosha self-service console (#345). Vanilla JS, no build step, no
// third-party code.
//
// Every call goes to /v1/console/* on this server. The session is an
// HTTP-only cookie the page never sees; state-changing calls carry the
// `X-Kosha-Console: 1` header the server requires against CSRF. All server
// data is rendered as text nodes, never as HTML.
"use strict";

(function () {
  const view = document.getElementById("view");
  let config = null;
  let me = null;

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
  function render(...nodes) {
    view.replaceChildren(...nodes);
  }
  function notice(kind, text) {
    return el("div", { class: "notice " + kind, role: kind === "error" ? "alert" : "status" }, text);
  }
  function fmtNum(n) {
    return n === undefined || n === null ? "-" : Number(n).toLocaleString();
  }
  function fmtBytes(n) {
    if (n === undefined || n === null) return "-";
    const units = ["B", "KB", "MB", "GB", "TB"];
    let v = Number(n);
    let i = 0;
    while (v >= 1024 && i < units.length - 1) {
      v /= 1024;
      i++;
    }
    return (i === 0 ? v : v.toFixed(1)) + " " + units[i];
  }
  function fmtTime(ms) {
    if (!ms) return "-";
    return new Date(Number(ms)).toLocaleString();
  }
  function fmtSecs(s) {
    return s ? fmtTime(Number(s) * 1000) : "-";
  }
  function table(headers, rows, emptyText) {
    if (!rows.length) return el("p", { class: "empty" }, emptyText || "Nothing yet.");
    return el(
      "table",
      {},
      el("thead", {}, el("tr", {}, headers.map((h) => el("th", { class: h.num ? "num" : "" }, h.label || h)))),
      el("tbody", {}, rows.map((cells) => el("tr", {}, cells.map((c) => (c instanceof Node && c.tagName === "TD" ? c : el("td", {}, c))))))
    );
  }
  function num(text) {
    return el("td", { class: "num" }, text);
  }
  function field(label, input) {
    return el("label", {}, label, input);
  }
  function input(attrs) {
    return el("input", Object.assign({ spellcheck: "false" }, attrs));
  }
  function values(form) {
    const out = {};
    for (const node of form.querySelectorAll("input, select, textarea")) {
      if (!node.name) continue;
      out[node.name] = node.type === "checkbox" ? node.checked : node.value;
    }
    return out;
  }
  /// A form whose submit runs `action(values)`; errors show above it.
  function form(cls, children, action, submitText) {
    const status = el("div");
    const button = el("button", { type: "submit" }, submitText || "Save");
    const node = el("form", { class: cls }, status, children, el("div", { class: "row" }, button));
    node.addEventListener("submit", async (event) => {
      event.preventDefault();
      button.disabled = true;
      status.replaceChildren();
      try {
        const message = await action(values(node), node);
        if (message) status.replaceChildren(notice("ok", message));
      } catch (e) {
        status.replaceChildren(notice("error", e.message));
      } finally {
        button.disabled = false;
      }
    });
    return node;
  }

  // ── API ───────────────────────────────────────────────────────────────
  async function api(method, path, body) {
    const options = { method, credentials: "same-origin", headers: {} };
    if (method !== "GET") options.headers["X-Kosha-Console"] = "1";
    if (body !== undefined) {
      options.headers["Content-Type"] = "application/json";
      options.body = JSON.stringify(body);
    }
    const response = await fetch("/v1/console" + path, options);
    let data = null;
    try {
      data = await response.json();
    } catch (_) {
      data = null;
    }
    if (!response.ok) {
      const error = new Error((data && data.error) || "request failed (HTTP " + response.status + ")");
      error.status = response.status;
      error.data = data;
      throw error;
    }
    return data;
  }

  async function loadMe() {
    try {
      me = await api("GET", "/me");
    } catch (e) {
      if (e.status !== 401) throw e;
      me = null;
    }
    renderHeader();
    return me;
  }

  // ── Routing ───────────────────────────────────────────────────────────
  function parseHash() {
    const hash = location.hash.replace(/^#/, "") || "/";
    const [path, query] = hash.split("?");
    const params = new URLSearchParams(query || "");
    return { parts: path.split("/").filter(Boolean), params };
  }
  function go(path) {
    location.hash = "#" + path;
  }

  function renderHeader() {
    const who = document.getElementById("whoami");
    const switcher = document.getElementById("org-switcher");
    switcher.replaceChildren();
    if (!me) {
      who.replaceChildren(el("a", { href: "#/login" }, "Sign in"));
      return;
    }
    const logout = el("button", { class: "secondary", type: "button" }, "Sign out");
    logout.addEventListener("click", async () => {
      await api("POST", "/logout").catch(() => {});
      me = null;
      renderHeader();
      go("/login");
    });
    who.replaceChildren(el("span", {}, me.user.email), logout);
    if (me.orgs.length) {
      const current = parseHash().parts[1];
      const select = el(
        "select",
        { "aria-label": "Organization" },
        el("option", { value: "" }, "Organizations"),
        me.orgs.map((o) => el("option", { value: o.id, selected: o.id === current }, o.name + " (" + o.role + ")"))
      );
      select.addEventListener("change", () => {
        if (select.value) go("/orgs/" + select.value);
        else go("/");
      });
      switcher.append(select);
    }
  }

  async function route() {
    const { parts, params } = parseHash();
    const page = parts[0] || "";
    try {
      if (!config) config = await api("GET", "/config");
      // Pages that work signed out.
      if (page === "login") return loginPage(params);
      if (page === "signup") return signupPage(params);
      if (page === "verify") return verifyPage(params);
      if (page === "reset") return resetPage(params);
      if (page === "invite") return invitePage(params);
      await loadMe();
      if (!me) return go("/login");
      if (page === "orgs" && parts[1]) return orgPage(parts[1], parts[2] || "namespaces");
      return homePage();
    } catch (e) {
      render(el("div", { class: "panel" }, notice("error", e.message)));
    }
  }

  // ── Signed-out pages ──────────────────────────────────────────────────
  function loginPage(params) {
    const panel = el("div", { class: "panel narrow" }, el("h1", {}, "Sign in to Kosha"));
    if (params.get("error")) panel.append(notice("error", params.get("error")));
    const resend = el("div");
    panel.append(
      form(
        "stack",
        [
          field("Email", input({ name: "email", type: "email", autocomplete: "username", required: true })),
          field("Password", input({ name: "password", type: "password", autocomplete: "current-password", required: true })),
        ],
        async (v) => {
          try {
            await api("POST", "/login", { email: v.email, password: v.password });
          } catch (e) {
            if (e.data && e.data.reason === "email_unverified") {
              const again = el("button", { type: "button", class: "secondary" }, "Send the link again");
              again.addEventListener("click", async () => {
                await api("POST", "/resend-verification", { email: v.email }).catch(() => {});
                resend.replaceChildren(notice("ok", "If the address needs it, a new link is on its way."));
              });
              resend.replaceChildren(notice("warn", "Verify your email address first: open the link we mailed you."), again);
              return "";
            }
            throw e;
          }
          await loadMe();
          go("/");
          return "";
        },
        "Sign in"
      ),
      resend
    );
    const providers = [];
    if (config.providers.google) {
      providers.push(el("a", { href: "/v1/console/oauth/google/start" }, el("button", { type: "button", class: "secondary" }, "Continue with Google")));
    }
    if (config.providers.github) {
      providers.push(el("a", { href: "/v1/console/oauth/github/start" }, el("button", { type: "button", class: "secondary" }, "Continue with GitHub")));
    }
    if (providers.length) panel.append(el("p", { class: "divider" }, "or"), el("div", { class: "row" }, providers));
    if (config.sso) {
      const sso = el(
        "form",
        { class: "inline" },
        field("Organization slug (single sign-on)", input({ name: "org", required: true, placeholder: "acme" })),
        el("button", { type: "submit", class: "secondary" }, "Sign in with SSO")
      );
      sso.addEventListener("submit", (event) => {
        event.preventDefault();
        location.href = "/v1/console/sso/start?org=" + encodeURIComponent(values(sso).org.trim());
      });
      panel.append(sso);
    }
    panel.append(
      el(
        "p",
        { class: "muted" },
        el("a", { href: "#/reset" }, "Forgot your password?"),
        config.signup ? " · " : "",
        config.signup ? el("a", { href: "#/signup" }, "Create an account") : ""
      )
    );
    render(panel);
  }

  function signupPage(params) {
    const invite = params.get("invite") || "";
    const panel = el("div", { class: "panel narrow" }, el("h1", {}, "Create your Kosha account"));
    if (!config.signup && !invite) {
      panel.append(notice("warn", "Sign-up is not open on this server. Ask your organization for an invite."));
      return render(panel);
    }
    if (config.signup_email_domains && config.signup_email_domains.length && !invite) {
      panel.append(el("p", { class: "muted" }, "Open to addresses at: " + config.signup_email_domains.join(", ")));
    }
    panel.append(
      form(
        "stack",
        [
          field("Email", input({ name: "email", type: "email", autocomplete: "email", required: true })),
          field("Name (optional)", input({ name: "display_name", autocomplete: "name" })),
          field(
            "Password (at least " + config.min_password_length + " characters)",
            input({ name: "password", type: "password", autocomplete: "new-password", minlength: config.min_password_length, required: true })
          ),
        ],
        async (v, node) => {
          const body = { email: v.email, password: v.password, display_name: v.display_name };
          if (invite) body.invite_token = invite;
          await api("POST", "/signup", body);
          node.reset();
          return "Check your inbox: we sent a link to verify the address. Then sign in.";
        },
        "Create account"
      ),
      el("p", { class: "muted" }, "Already have an account? ", el("a", { href: "#/login" }, "Sign in"))
    );
    render(panel);
  }

  async function verifyPage(params) {
    const panel = el("div", { class: "panel narrow" }, el("h1", {}, "Verify your email"));
    render(panel);
    try {
      const result = await api("POST", "/verify-email", { token: params.get("token") || "" });
      panel.append(notice("ok", result.email + " is verified."), el("a", { href: "#/login" }, el("button", { type: "button" }, "Sign in")));
    } catch (e) {
      panel.append(notice("error", e.message));
    }
  }

  function resetPage(params) {
    const token = params.get("token");
    const panel = el("div", { class: "panel narrow" });
    if (!token) {
      panel.append(
        el("h1", {}, "Reset your password"),
        form(
          "stack",
          [field("Email", input({ name: "email", type: "email", autocomplete: "email", required: true }))],
          async (v) => {
            await api("POST", "/password-reset", { email: v.email });
            return "If an account uses this address, a reset link is on its way.";
          },
          "Send reset link"
        )
      );
    } else {
      panel.append(
        el("h1", {}, "Choose a new password"),
        form(
          "stack",
          [
            field(
              "New password (at least " + config.min_password_length + " characters)",
              input({ name: "password", type: "password", autocomplete: "new-password", minlength: config.min_password_length, required: true })
            ),
          ],
          async (v) => {
            await api("POST", "/password-reset/confirm", { token, password: v.password });
            setTimeout(() => go("/login"), 1200);
            return "Password changed. Every other session was signed out.";
          },
          "Set password"
        )
      );
    }
    render(panel);
  }

  async function invitePage(params) {
    const token = params.get("token") || "";
    const panel = el("div", { class: "panel narrow" }, el("h1", {}, "Join an organization"));
    render(panel);
    await loadMe();
    if (!me) {
      panel.append(
        el("p", {}, "Sign in with the invited address to accept, or create an account with it."),
        el(
          "div",
          { class: "row" },
          el("a", { href: "#/signup?invite=" + encodeURIComponent(token) }, el("button", { type: "button" }, "Create account")),
          el("a", { href: "#/login" }, el("button", { type: "button", class: "secondary" }, "Sign in"))
        ),
        el("p", { class: "muted" }, "After signing in, open the invite link again.")
      );
      return;
    }
    panel.append(
      el("p", {}, "Accept the invite as " + me.user.email + "?"),
      form(
        "stack",
        [],
        async () => {
          const joined = await api("POST", "/invites/accept", { token });
          await loadMe();
          go("/orgs/" + joined.org.id);
          return "";
        },
        "Accept invite"
      )
    );
  }

  // ── Home ──────────────────────────────────────────────────────────────
  function homePage() {
    const list = table(
      ["Organization", "Slug", "Your role", "SSO"],
      me.orgs.map((o) => [el("a", { href: "#/orgs/" + o.id }, o.name), o.slug, o.role, o.sso_required ? "required" : o.sso_configured ? "on" : "-"]),
      "You are not in any organization yet. Create one below."
    );
    const create = form(
      "inline",
      [
        field("Name", input({ name: "name", required: true, placeholder: "Acme" })),
        field("Slug (a-z, 0-9, -)", input({ name: "slug", required: true, pattern: "[a-z0-9]([a-z0-9-]*[a-z0-9])?", placeholder: "acme" })),
      ],
      async (v) => {
        const org = await api("POST", "/orgs", { name: v.name, slug: v.slug });
        await loadMe();
        go("/orgs/" + org.id);
        return "";
      },
      "Create organization"
    );
    render(
      el("div", { class: "panel" }, el("h1", {}, "Your organizations"), list),
      el("div", { class: "panel" }, el("h2", {}, "New organization"), el("p", { class: "muted" }, "You will be its owner."), create)
    );
  }

  // ── Organization ──────────────────────────────────────────────────────
  const TABS = [
    ["namespaces", "Namespaces"],
    ["keys", "API keys"],
    ["usage", "Usage"],
    ["billing", "Plan & billing"],
    ["members", "Members"],
    ["projects", "Projects & tenants"],
    ["audit", "Audit log"],
    ["sso", "Single sign-on"],
  ];

  async function orgPage(orgId, tab) {
    const base = "/orgs/" + encodeURIComponent(orgId);
    const info = await api("GET", base);
    const role = info.membership ? info.membership.role : "";
    const manage = role === "owner" || role === "admin";
    const tabs = el(
      "nav",
      { class: "tabs" },
      TABS.map(([id, label]) => el("a", { href: "#/orgs/" + orgId + "/" + id, class: id === tab ? "active" : "" }, label))
    );
    const body = el("div");
    render(el("h1", {}, info.org.name, el("span", { class: "muted" }, "  " + info.org.slug + " · " + role)), tabs, body);
    const ctx = { orgId, base, role, manage, body };
    const pages = {
      namespaces: namespacesTab,
      keys: keysTab,
      usage: usageTab,
      billing: billingTab,
      members: membersTab,
      projects: projectsTab,
      audit: auditTab,
      sso: ssoTab,
    };
    try {
      await (pages[tab] || namespacesTab)(ctx);
    } catch (e) {
      body.replaceChildren(el("div", { class: "panel" }, notice("error", e.message)));
    }
  }

  async function tenantsOf(ctx) {
    return (await api("GET", ctx.base + "/tenants")).tenants;
  }

  async function namespacesTab(ctx) {
    const data = await api("GET", ctx.base + "/namespaces");
    ctx.body.replaceChildren(
      el(
        "div",
        { class: "panel" },
        el("h2", {}, "Namespaces"),
        table(
          ["Tenant", "Namespace", { label: "Documents", num: true }, { label: "Segments", num: true }, { label: "Stored", num: true }],
          data.namespaces.map((n) => [el("code", {}, n.tenant_id), n.name, num(fmtNum(n.documents)), num(fmtNum(n.segments)), num(fmtBytes(n.stored_bytes))]),
          "No namespaces yet. Create an API key, then index documents with it: a namespace is created on its first write."
        )
      )
    );
  }

  // `created`: a key just made, shown once above the form.
  async function keysTab(ctx, created) {
    const [keys, tenants] = await Promise.all([api("GET", ctx.base + "/api-keys"), tenantsOf(ctx)]);
    const shown = el("div");
    if (created) {
      shown.append(
        notice("warn", "Copy this key now. It is not shown again."),
        el("code", { class: "secret" }, created.api_key),
        el("p", { class: "muted" }, "Use it as Authorization: Bearer <key> on the /v1/namespaces API.")
      );
    }
    const rows = keys.api_keys.map((k) => {
      const mine = k.created_by && k.created_by === me.user.id;
      const revoke = el("button", { type: "button", class: "danger", disabled: !(ctx.manage || (mine && ctx.role !== "read-only")) }, "Revoke");
      revoke.addEventListener("click", async () => {
        if (!confirm("Revoke this key? Requests using it will start failing.")) return;
        try {
          await api("POST", ctx.base + "/api-keys/" + encodeURIComponent(k.key_id) + "/revoke");
          keysTab(ctx);
        } catch (e) {
          shown.replaceChildren(notice("error", e.message));
        }
      });
      return [el("code", {}, k.key_id.slice(0, 12)), el("code", {}, k.tenant_id), k.description || "-", k.scopes.join(", "), fmtSecs(k.created_at), fmtSecs(k.expires_at), fmtSecs(k.last_used_at), revoke];
    });
    const create =
      ctx.role === "read-only" || !tenants.length
        ? el("p", { class: "muted" }, tenants.length ? "Read-only members cannot create keys." : "Create a tenant first (Projects & tenants).")
        : form(
            "inline",
            [
              field("Tenant", el("select", { name: "tenant_id" }, tenants.map((t) => el("option", { value: t.tenant_id }, t.tenant_id)))),
              field("Description", input({ name: "description", placeholder: "ci" })),
              field("Scopes", el("select", { name: "scopes" }, el("option", { value: "read,write,admin" }, "read, write, admin"), el("option", { value: "read,write" }, "read, write"), el("option", { value: "read" }, "read"))),
              field("Expires in days (optional)", input({ name: "expires", type: "number", min: 1, max: 3650 })),
            ],
            async (v) => {
              const body = { tenant_id: v.tenant_id, description: v.description, scopes: v.scopes.split(",") };
              if (v.expires) body.expires_in_days = Number(v.expires);
              const created = await api("POST", ctx.base + "/api-keys", body);
              await keysTab(ctx, created);
              return "";
            },
            "Create key"
          );
    ctx.body.replaceChildren(
      el("div", { class: "panel" }, el("h2", {}, "New API key"), shown, create),
      el("div", { class: "panel" }, el("h2", {}, "Active keys"), table(["Id", "Tenant", "Description", "Scopes", "Created", "Expires", "Last used", ""], rows, "No keys."))
    );
  }

  async function tenantPicker(ctx, onPick) {
    const tenants = await tenantsOf(ctx);
    if (!tenants.length) {
      ctx.body.replaceChildren(el("div", { class: "panel" }, el("p", { class: "muted" }, "No tenants yet.")));
      return null;
    }
    const select = el("select", { "aria-label": "Tenant" }, tenants.map((t) => el("option", { value: t.tenant_id }, t.tenant_id)));
    const out = el("div");
    select.addEventListener("change", () => onPick(select.value, out));
    ctx.body.replaceChildren(el("div", { class: "panel" }, el("div", { class: "row" }, field("Tenant", select)), out));
    onPick(select.value, out);
    return select;
  }

  function usageTab(ctx) {
    return tenantPicker(ctx, async (tenant, out) => {
      try {
        const u = await api("GET", ctx.base + "/usage?tenant=" + encodeURIComponent(tenant));
        out.replaceChildren(
          el("p", { class: "muted" }, "From " + fmtSecs(u.from) + " to " + fmtSecs(u.to) + "."),
          table(
            ["Namespace", "Metric", { label: "Value", num: true }],
            u.namespaces.flatMap((n) =>
              Object.entries(n)
                .filter(([k, v]) => typeof v === "number" && k !== "tenant")
                .map(([k, v]) => [n.namespace || "-", k.replace(/_/g, " "), num(fmtNum(v))])
            ),
            "No usage recorded in this period."
          )
        );
      } catch (e) {
        out.replaceChildren(notice(e.status === 404 ? "warn" : "error", e.message));
      }
    });
  }

  function billingTab(ctx) {
    return tenantPicker(ctx, async (tenant, out) => {
      // #349: with self-service billing on, the server's answer carries
      // `actions`; each button asks the server for a hosted provider page
      // (Stripe Checkout or the customer portal) and goes there.
      const placeholder = el(
        "div",
        { class: "notice warn" },
        "Online payment is not enabled on this server. Contact support to change your plan."
      );
      const go = (action) => async (ev) => {
        ev.target.disabled = true;
        try {
          const s = await api("POST", ctx.base + "/billing/" + action, { tenant });
          window.location.assign(s.url);
        } catch (e) {
          ev.target.disabled = false;
          out.prepend(notice("error", e.message));
        }
      };
      try {
        const b = await api("GET", ctx.base + "/billing?tenant=" + encodeURIComponent(tenant));
        const actions = el("div", { class: "row" });
        if (b.actions && b.actions.checkout && ctx.manage) actions.append(el("button", { type: "button", onclick: go("checkout") }, "Upgrade / add payment method"));
        if (b.actions && b.actions.portal && ctx.manage) actions.append(el("button", { type: "button", class: "secondary", onclick: go("portal") }, "Manage billing"));
        const summary = table(
          ["", ""],
          [
            ["Plan", String(b.plan || "-")],
            ["Kind", String(b.kind || "-")],
            ["Standing", String(b.state || "-")],
          ]
        );
        out.replaceChildren(summary, b.actions ? actions : placeholder, el("details", {}, el("summary", {}, "Details"), el("pre", { class: "mono" }, JSON.stringify(b, null, 2))));
      } catch (e) {
        out.replaceChildren(notice(e.status === 404 ? "warn" : "error", e.message), placeholder);
      }
    });
  }

  async function membersTab(ctx) {
    const data = await api("GET", ctx.base + "/members");
    const status = el("div");
    const rows = data.members.map((m) => {
      const cells = [m.email, m.display_name || "-", m.role, fmtTime(m.created_at_ms)];
      if (ctx.manage || m.user_id === me.user.id) {
        const controls = el("div", { class: "row" });
        if (ctx.manage) {
          const select = el("select", { "aria-label": "Role" }, ["owner", "admin", "member", "read-only"].map((r) => el("option", { value: r, selected: r === m.role }, r)));
          select.addEventListener("change", async () => {
            try {
              await api("POST", ctx.base + "/members/" + encodeURIComponent(m.user_id) + "/role", { role: select.value });
              status.replaceChildren(notice("ok", m.email + " is now " + select.value + "."));
            } catch (e) {
              status.replaceChildren(notice("error", e.message));
              select.value = m.role;
            }
          });
          controls.append(select);
        }
        const keep = el("input", { type: "checkbox" });
        const remove = el("button", { type: "button", class: "danger" }, m.user_id === me.user.id ? "Leave" : "Remove");
        remove.addEventListener("click", async () => {
          if (!confirm("Remove " + m.email + " from the organization?")) return;
          try {
            const result = await api("DELETE", ctx.base + "/members/" + encodeURIComponent(m.user_id) + (keep.checked ? "?revoke_keys=false" : ""));
            status.replaceChildren(notice("ok", "Removed. Keys revoked: " + result.revoked_keys.length + "."));
            if (m.user_id === me.user.id) {
              await loadMe();
              return go("/");
            }
            membersTab(ctx);
          } catch (e) {
            status.replaceChildren(notice("error", e.message));
          }
        });
        controls.append(el("label", { class: "check" }, keep, "keep their keys"), remove);
        cells.push(controls);
      } else {
        cells.push("");
      }
      return cells;
    });
    const invite = ctx.manage
      ? el(
          "div",
          { class: "panel" },
          el("h2", {}, "Invite someone"),
          form(
            "inline",
            [
              field("Email", input({ name: "email", type: "email", required: true })),
              field("Role", el("select", { name: "role" }, ["member", "read-only", "admin", "owner"].map((r) => el("option", { value: r }, r)))),
            ],
            async (v, node) => {
              const r = await api("POST", ctx.base + "/invites", { email: v.email, role: v.role });
              node.reset();
              return "Invite sent to " + r.invited + ".";
            },
            "Send invite"
          )
        )
      : null;
    ctx.body.replaceChildren(
      el("div", { class: "panel" }, el("h2", {}, "Members"), status, table(["Email", "Name", "Role", "Since", ""], rows)),
      invite || el("span")
    );
  }

  async function projectsTab(ctx) {
    const [projects, tenants] = await Promise.all([api("GET", ctx.base + "/projects"), tenantsOf(ctx)]);
    const names = Object.fromEntries(projects.projects.map((p) => [p.id, p.name]));
    const newProject = ctx.manage
      ? form(
          "inline",
          [field("Name", input({ name: "name", required: true })), field("Slug", input({ name: "slug", required: true, pattern: "[a-z0-9]([a-z0-9-]*[a-z0-9])?" }))],
          async (v) => {
            await api("POST", ctx.base + "/projects", { name: v.name, slug: v.slug });
            projectsTab(ctx);
            return "";
          },
          "Create project"
        )
      : null;
    const newTenant = ctx.manage
      ? form(
          "inline",
          [field("Project (optional)", el("select", { name: "project_id" }, el("option", { value: "" }, "none"), projects.projects.map((p) => el("option", { value: p.id }, p.name))))],
          async (v) => {
            const body = v.project_id ? { project_id: v.project_id } : {};
            const t = await api("POST", ctx.base + "/tenants", body);
            await projectsTab(ctx);
            return "Tenant " + t.tenant_id + " created.";
          },
          "Create tenant"
        )
      : null;
    ctx.body.replaceChildren(
      el("div", { class: "panel" }, el("h2", {}, "Projects"), table(["Name", "Slug", "Created"], projects.projects.map((p) => [p.name, p.slug, fmtTime(p.created_at_ms)]), "No projects."), newProject || ""),
      el(
        "div",
        { class: "panel" },
        el("h2", {}, "Tenants"),
        el("p", { class: "muted" }, "A tenant owns namespaces; its API keys reach only its namespaces."),
        table(["Tenant id", "Project", "Created"], tenants.map((t) => [el("code", {}, t.tenant_id), t.project_id ? names[t.project_id] || t.project_id : "-", fmtTime(t.created_at_ms)]), "No tenants."),
        newTenant || ""
      )
    );
  }

  async function auditTab(ctx, before) {
    const data = await api("GET", ctx.base + "/audit-log?limit=50" + (before ? "&before=" + before : ""));
    const more = data.next_before ? el("button", { type: "button", class: "secondary" }, "Older") : null;
    if (more) more.addEventListener("click", () => auditTab(ctx, data.next_before));
    ctx.body.replaceChildren(
      el(
        "div",
        { class: "panel" },
        el("h2", {}, "Audit log"),
        table(
          ["When", "Who", "Action", "Target", "Detail"],
          data.events.map((e) => [fmtTime(e.created_at_ms), el("code", {}, e.actor), e.action, e.target_type + " " + e.target_id, el("code", {}, JSON.stringify(e.detail))]),
          "No events."
        ),
        more || ""
      )
    );
  }

  async function ssoTab(ctx) {
    if (!ctx.manage) {
      ctx.body.replaceChildren(el("div", { class: "panel" }, el("p", { class: "muted" }, "Only owners and admins manage single sign-on.")));
      return;
    }
    if (!config.sso) {
      ctx.body.replaceChildren(el("div", { class: "panel" }, notice("warn", "Single sign-on is not enabled on this server (KOSHA_CONSOLE_SECRET_KEY).")));
      return;
    }
    const sso = await api("GET", ctx.base + "/sso");
    const remove = sso.configured ? el("button", { type: "button", class: "danger" }, "Remove SSO") : null;
    if (remove)
      remove.addEventListener("click", async () => {
        if (!confirm("Remove single sign-on for this organization?")) return;
        await api("DELETE", ctx.base + "/sso");
        ssoTab(ctx);
      });
    ctx.body.replaceChildren(
      el(
        "div",
        { class: "panel" },
        el("h2", {}, "OpenID Connect single sign-on"),
        el("p", { class: "muted" }, "Register this redirect URI with your identity provider: ", el("code", {}, sso.redirect_uri)),
        el("p", { class: "muted" }, "Members sign in at the login page with the organization slug. SAML is not supported yet."),
        form(
          "stack",
          [
            field("Issuer URL", input({ name: "issuer", type: "url", required: true, value: sso.issuer || "", placeholder: "the issuer, e.g. login.example.com" })),
            field("Client id", input({ name: "client_id", required: true, value: sso.client_id || "" })),
            field(sso.configured ? "Client secret (leave empty to keep the current one)" : "Client secret", input({ name: "client_secret", type: "password", autocomplete: "off", required: !sso.configured })),
            field("Allowed email domains, comma-separated (people from these domains can join as read-only)", input({ name: "allowed_domains", value: (sso.allowed_domains || []).join(", ") })),
            el("label", { class: "check" }, el("input", { type: "checkbox", name: "sso_required", checked: !!sso.sso_required }), "Require SSO for members (owners can still use other sign-in methods)"),
          ],
          async (v) => {
            const body = {
              issuer: v.issuer.trim(),
              client_id: v.client_id.trim(),
              allowed_domains: v.allowed_domains.split(",").map((d) => d.trim()).filter(Boolean),
              sso_required: v.sso_required,
            };
            if (v.client_secret) body.client_secret = v.client_secret;
            await api("POST", ctx.base + "/sso", body);
            await ssoTab(ctx);
            return "";
          },
          "Save"
        ),
        remove || ""
      )
    );
  }

  window.addEventListener("hashchange", route);
  route();
})();
