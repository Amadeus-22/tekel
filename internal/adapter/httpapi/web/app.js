// Tekel status page: reads /v1/streams and /v1/incidents every 10 s.
// Text is always set with textContent, never as HTML.
"use strict";

const REFRESH_MS = 10000;
const STALE_MS = 60000;          // no data for a minute: warning
const RECENT_MS = 60 * 60000;    // incidents in the last hour: warning
const SEVERE = new Set(["sequence_gap", "stale_stream"]);

const el = (tag, cls, text) => {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  if (text !== undefined) e.textContent = text;
  return e;
};

function ago(iso, now) {
  if (!iso || iso.startsWith("0001-")) return "never";
  const s = Math.max(0, Math.round((now - Date.parse(iso)) / 1000));
  if (s < 60) return s + "s ago";
  if (s < 3600) return Math.floor(s / 60) + "m ago";
  return Math.floor(s / 3600) + "h ago";
}

function streamState(s, recent, now) {
  if (!s.connected) return ["down", "disconnected"];
  const last = Date.parse(s.last_received_time || "");
  if (!last || now - last > STALE_MS) return ["warn", "no data for " + ago(s.last_received_time, now).replace(" ago", "")];
  const key = s.exchange + "\u0000" + s.symbol;
  if (recent.has(key)) return ["warn", recent.get(key) + " incident(s) in the last hour"];
  return ["ok", "healthy"];
}

function renderSources(streams, recent, now) {
  const groups = new Map();
  for (const s of streams) {
    if (!groups.has(s.exchange)) groups.set(s.exchange, []);
    groups.get(s.exchange).push(s);
  }
  const box = document.getElementById("sources");
  box.replaceChildren();
  const worst = { ok: 0, warn: 0, down: 0 };
  for (const [exchange, list] of [...groups].sort()) {
    const card = el("article", "card");
    const head = el("div", "card-head");
    head.append(el("span", "card-title", exchange), el("span", "kind", list[0].kind === "blocks" ? "chain blocks" : "trades"));
    card.append(head);
    for (const s of list) {
      const [state, why] = streamState(s, recent, now);
      worst[state]++;
      const row = el("div", "stream");
      const unit = s.kind === "blocks" ? "block" : "trade";
      row.append(el("span", "dot " + state), el("span", "sym", s.symbol),
        el("span", "val", s.connected ? Math.round((s.lag_seconds || 0) * 1000) + " ms" : "—"));
      const meta = el("div", "meta",
        why + " · last " + unit + " " + ago(s.last_received_time, now) +
        (s.last_trade_id ? " · #" + s.last_trade_id : "") +
        " · " + (s.trades || 0).toLocaleString("en-US") + " " + unit + "s since start");
      row.append(meta);
      if (s.reference) row.append(referenceLine(s));
      card.append(row);
    }
    box.append(card);
  }
  return worst;
}

// referenceLine shows the last trade next to CoinMarketCap's price for the
// same asset and how far apart they are.
function referenceLine(s) {
  const r = s.reference;
  const pct = r.deviation * 100;
  const line = el("div", "meta ref");
  line.append(el("span", "", "CoinMarketCap " + r.asset + " " + Number(r.price).toLocaleString("en-US", { maximumFractionDigits: 8 }) + " · "));
  line.append(el("span", "dev" + (Math.abs(pct) >= 1 ? " off" : ""), (pct >= 0 ? "+" : "") + pct.toFixed(3) + "%"));
  return line;
}

// group merges runs of the same source and kind detected within a minute of
// each other (a clock jump can raise hundreds at once) into one row.
function group(incidents) {
  const rows = [];
  for (const i of incidents) {
    const prev = rows[rows.length - 1];
    const t = Date.parse(i.detected_at);
    if (prev && prev.exchange === i.exchange && prev.symbol === i.symbol && prev.kind === i.kind && prev.first - t <= 60000) {
      prev.count++;
      prev.first = t;
      continue;
    }
    rows.push({ exchange: i.exchange, symbol: i.symbol, kind: i.kind, detail: i.detail, last: t, first: t, count: 1 });
  }
  return rows;
}

const SHOW = 25;
let showAll = false;
let lastIncidents = [];

function renderIncidents(incidents) {
  lastIncidents = incidents;
  const body = document.getElementById("incidents");
  body.replaceChildren();
  const rows = group(incidents);
  const byKind = new Map();
  for (const i of incidents) byKind.set(i.kind, (byKind.get(i.kind) || 0) + 1);
  const summary = [...byKind].sort((a, b) => b[1] - a[1]).map(([k, n]) => n + " " + k.replace("_", " ")).join(" · ");
  document.getElementById("inc-count").textContent = incidents.length ? summary : "";
  if (rows.length === 0) {
    const tr = el("tr");
    const td = el("td", "muted", "No incidents in the last 24 hours.");
    td.colSpan = 4;
    tr.append(td);
    body.append(tr);
  }
  for (const r of showAll ? rows : rows.slice(0, SHOW)) {
    const tr = el("tr");
    const when = new Date(r.last).toISOString().replace("T", " ").slice(0, 19);
    const kindCell = el("td");
    kindCell.append(el("span", "pill" + (SEVERE.has(r.kind) ? " down" : ""), r.kind.replace("_", " ")));
    if (r.count > 1) kindCell.append(el("span", "times", "×" + r.count));
    tr.append(el("td", "when", when), el("td", "", r.exchange + " " + r.symbol), kindCell,
      el("td", "", r.count > 1 ? r.detail + " (latest of " + r.count + " within a minute)" : r.detail || ""));
    body.append(tr);
  }
  const more = document.getElementById("more");
  more.hidden = rows.length <= SHOW;
  more.textContent = showAll ? "Show fewer" : "Show all " + rows.length;
}

function renderBanner(worst, streams) {
  const b = document.getElementById("banner");
  const title = document.getElementById("banner-title");
  const sub = document.getElementById("banner-sub");
  let state = "ok";
  if (worst.down > 0) {
    state = "down";
    title.textContent = worst.down + " feed" + (worst.down > 1 ? "s" : "") + " down";
  } else if (worst.warn > 0) {
    state = "warn";
    title.textContent = "Degraded: " + worst.warn + " feed" + (worst.warn > 1 ? "s need" : " needs") + " attention";
  } else {
    title.textContent = "All feeds healthy";
  }
  sub.textContent = streams.length + " streams monitored, refreshed every " + REFRESH_MS / 1000 + " seconds";
  b.className = "banner " + state;
}

async function refresh() {
  const now = Date.now();
  try {
    const since = new Date(now - 24 * 3600 * 1000).toISOString();
    const [sr, ir] = await Promise.all([
      fetch("/v1/streams", { cache: "no-store" }),
      fetch("/v1/incidents?limit=500&since=" + encodeURIComponent(since), { cache: "no-store" }),
    ]);
    if (!sr.ok || !ir.ok) throw new Error("HTTP " + sr.status + "/" + ir.status);
    const streams = (await sr.json()).streams || [];
    const incidents = (await ir.json()).incidents || [];
    const recent = new Map();
    for (const i of incidents) {
      if (now - Date.parse(i.detected_at) <= RECENT_MS && SEVERE.has(i.kind)) {
        const key = i.exchange + "\u0000" + i.symbol;
        recent.set(key, (recent.get(key) || 0) + 1);
      }
    }
    renderBanner(renderSources(streams, recent, now), streams);
    renderIncidents(incidents);
    document.getElementById("updated").textContent = "Updated " + new Date(now).toISOString().slice(11, 19) + " UTC";
  } catch (err) {
    const b = document.getElementById("banner");
    b.className = "banner down";
    document.getElementById("banner-title").textContent = "Status unavailable";
    document.getElementById("banner-sub").textContent = "Could not reach Tekel (" + err.message + "); retrying.";
  }
}

document.getElementById("more").addEventListener("click", () => {
  showAll = !showAll;
  renderIncidents(lastIncidents);
});

refresh();
setInterval(refresh, REFRESH_MS);
