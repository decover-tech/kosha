const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const { setImmediate } = require("node:timers/promises");
const { test } = require("node:test");
const { JSDOM } = require("jsdom");

test("clearing the usage month removes stale totals without fetching", async (t) => {
  const html = fs.readFileSync(path.join(__dirname, "index.html"), "utf8");
  const dom = new JSDOM(html, {
    url: "http://localhost/dashboard#/usage",
    runScripts: "outside-only",
  });
  const { window } = dom;
  t.after(() => window.close());
  window.sessionStorage.setItem("kosha.dashboard.apiKey", "test-key");
  window.fetch = t.mock.fn(async () => ({
    ok: true,
    text: async () => JSON.stringify({
      namespaces: [{ namespace: "example", queries: 42 }],
      total: { queries: 42 },
    }),
  }));
  window.eval(fs.readFileSync(path.join(__dirname, "app.js"), "utf8"));
  await setImmediate();

  const view = window.document.getElementById("view");
  assert.equal(view.querySelector("tbody td").textContent, "example");
  assert.equal(view.querySelector("tfoot th").textContent, "Tenant total");
  assert.equal(window.fetch.mock.callCount(), 1);

  const month = view.querySelector('input[type="month"]');
  month.value = "";
  month.dispatchEvent(new window.Event("change"));
  await setImmediate();

  assert.equal(view.querySelector("table"), null);
  assert.match(view.textContent, /Choose a month to see usage/);
  assert.equal(window.fetch.mock.callCount(), 1);
});
