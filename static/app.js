/* eslint-disable no-console */
"use strict";

// Fixed axis ranges keep the live plots visually stable frame to frame
// instead of the scale jumping around as the rolling window slides.
const AXIS_DIVISIONS = 5;
const AXIS_RANGES = {
  percent: [0, 100],
  rpm: [0, 10000],
  speed: [0, 200],
  temp: [0, 150],
};

function axisSplits(group) {
  const [min, max] = AXIS_RANGES[group];
  const step = (max - min) / AXIS_DIVISIONS;
  return Array.from({ length: AXIS_DIVISIONS + 1 }, (_, i) => min + i * step);
}

const DRIVER_PALETTE = ["#ff8c3c", "#4d8fff", "#35c98f", "#ef4a5e", "#b366ff", "#ffd23c"];

let FIELDS = {};        // name -> {unit, axis_group, color, pid}
let FIELD_NAMES = [];

// Rolling live buffer, arrays kept parallel & sorted by ts ascending.
const buffer = { ts: [] };
let windowSeconds = 60;

let lastServerTs = null;
let liveSource = null;

const livePanels = [];  // {id, fields:Set, uplot, el}
let liveStagedFields = new Set(); // which chips are toggled "on" for the *next* Add chart click

// ---------------------------------------------------------------- helpers

const $ = (sel, root = document) => root.querySelector(sel);
const $all = (sel, root = document) => Array.from(root.querySelectorAll(sel));

function fmtDuration(sec) {
  if (sec == null || Number.isNaN(sec)) return "—";
  const s = Math.max(0, Math.round(sec));
  const m = Math.floor(s / 60);
  const r = s % 60;
  return `${m}:${String(r).padStart(2, "0")}`;
}

function fieldLabel(name) {
  const f = FIELDS[name];
  return `${name.replace(/_/g, " ")} (${f.unit})`;
}

// ---------------------------------------------------------------- init

async function init() {
  const res = await fetch("/api/fields");
  const data = await res.json();
  FIELDS = data.fields;
  FIELD_NAMES = Object.keys(FIELDS);
  FIELD_NAMES.forEach((n) => (buffer[n] = []));

  FIELD_NAMES.forEach((n) => liveStagedFields.add(n)); // default: all on, ready to overlay
  buildMetricPicker($("#metricPicker"), liveStagedFields, null);

  buildMetricPicker($("#compareMetricPicker"), compareStagedFields, null);

  $("#addChartBtn").addEventListener("click", () => {
    if (liveStagedFields.size === 0) return;
    addLivePanel(new Set(liveStagedFields));
  });

  $("#windowSelect").addEventListener("change", (e) => {
    windowSeconds = Number(e.target.value);
    trimBuffer();
    livePanels.forEach(redrawPanel);
  });

  $all(".tab-btn").forEach((btn) =>
    btn.addEventListener("click", () => switchView(btn.dataset.view))
  );

  connectSSE();
  setInterval(tickAge, 250);

  // One starter chart so the dashboard isn't empty on first load.
  addLivePanel(new Set(FIELD_NAMES));

  await loadStints();
  $("#compareBtn").addEventListener("click", runComparison);
}

function switchView(view) {
  $all(".tab-btn").forEach((b) => b.classList.toggle("active", b.dataset.view === view));
  $("#view-live").classList.toggle("active", view === "live");
  $("#view-compare").classList.toggle("active", view === "compare");
  if (view === "compare") loadStints();
}

// ---------------------------------------------------------------- metric chips

function buildMetricPicker(container, stagedSet, onToggle) {
  container.innerHTML = "";
  FIELD_NAMES.forEach((name) => {
    const chip = document.createElement("div");
    chip.className = "chip" + (stagedSet.has(name) ? " on" : "");
    chip.style.color = stagedSet.has(name) ? FIELDS[name].color : "";
    chip.innerHTML = `<span class="swatch" style="background:${FIELDS[name].color}"></span>${fieldLabel(name)}`;
    chip.addEventListener("click", () => {
      if (stagedSet.has(name)) stagedSet.delete(name);
      else stagedSet.add(name);
      chip.classList.toggle("on");
      chip.style.color = stagedSet.has(name) ? FIELDS[name].color : "";
      if (onToggle) onToggle(name, stagedSet.has(name));
    });
    container.appendChild(chip);
  });
}

// ---------------------------------------------------------------- live feed (SSE)
//
// EventSource, not WebSocket: the client only ever receives here, never
// sends, and the browser reconnects to /api/live on its own after any drop
// (no backoff loop or "last position" tracking to write) -- on reconnect
// the server just replays a fresh short backfill (see main.py).

function connectSSE() {
  liveSource = new EventSource("/api/live");

  liveSource.onopen = () => setConnState("live");
  liveSource.onerror = () => setConnState("dead"); // EventSource retries this same connection itself

  liveSource.onmessage = (ev) => {
    const msg = JSON.parse(ev.data);
    if (msg.type === "backfill") {
      msg.points.forEach(pushPoint);
      trimBuffer();
      livePanels.forEach(redrawPanel);
      setConnState("live");
    } else if (msg.type === "live") {
      pushPoint(msg);
      trimBuffer();
      livePanels.forEach(redrawPanel);
      setConnState("live");
    }
  };
}

function pushPoint(p) {
  buffer.ts.push(p.server_ts);
  FIELD_NAMES.forEach((n) => buffer[n].push(p[n]));
  lastServerTs = p.server_ts;
}

function trimBuffer() {
  const cutoff = Date.now() / 1000 - windowSeconds;
  let i = 0;
  while (i < buffer.ts.length && buffer.ts[i] < cutoff) i++;
  if (i > 0) {
    buffer.ts.splice(0, i);
    FIELD_NAMES.forEach((n) => buffer[n].splice(0, i));
  }
}

function setConnState(state) {
  const dot = $("#connDot");
  const label = $("#connLabel");
  dot.className = "dot " + (state === "live" ? "live" : "dead");
  label.textContent = state === "live" ? "connected" : "reconnecting…";
}

function tickAge() {
  const el = $("#ageValue");
  const dot = $("#connDot");
  if (lastServerTs == null) {
    el.textContent = "—";
    return;
  }
  const age = Date.now() / 1000 - lastServerTs;
  el.textContent = `${age.toFixed(2)}s`;
  if (dot.classList.contains("live") || dot.classList.contains("stale")) {
    if (age < 1.5) dot.className = "dot live";
    else if (age < 3) dot.className = "dot stale";
    else dot.className = "dot dead";
  }
}

// ---------------------------------------------------------------- live chart panels

let panelCounter = 0;

function addLivePanel(fieldSet) {
  const id = `panel-${++panelCounter}`;
  const wrap = document.createElement("div");
  wrap.className = "chart-panel";
  wrap.id = id;

  const head = document.createElement("div");
  head.className = "chart-panel-head";
  const removeBtn = document.createElement("button");
  removeBtn.className = "chart-panel-remove";
  removeBtn.innerHTML = "&times;";
  removeBtn.title = "Remove chart";
  head.appendChild(removeBtn);

  const canvasWrap = document.createElement("div");
  canvasWrap.className = "chart-canvas-wrap";

  wrap.appendChild(head);
  wrap.appendChild(canvasWrap);
  $("#chartPanels").prepend(wrap);

  const panel = { id, fields: fieldSet, uplot: null, el: canvasWrap };

  // Per-panel chips let the user overlay/remove any series on THIS chart
  // independently of every other chart -- this is what makes "any and all
  // graphs may overlay" a real, live-editable feature rather than a fixed
  // preset.
  FIELD_NAMES.forEach((name) => {
    const chip = document.createElement("div");
    chip.className = "chip" + (fieldSet.has(name) ? " on" : "");
    chip.style.color = fieldSet.has(name) ? FIELDS[name].color : "";
    chip.innerHTML = `<span class="swatch" style="background:${FIELDS[name].color}"></span>${name.replace(/_/g, " ")}`;
    chip.addEventListener("click", () => {
      if (fieldSet.has(name)) fieldSet.delete(name);
      else fieldSet.add(name);
      chip.classList.toggle("on");
      chip.style.color = fieldSet.has(name) ? FIELDS[name].color : "";
      rebuildPanelChart(panel);
    });
    head.insertBefore(chip, removeBtn);
  });

  removeBtn.addEventListener("click", () => {
    wrap.remove();
    const idx = livePanels.indexOf(panel);
    if (idx >= 0) livePanels.splice(idx, 1);
  });

  livePanels.push(panel);
  rebuildPanelChart(panel);
  return panel;
}

function scalesAndAxesFor(fieldNames) {
  const groups = [];
  fieldNames.forEach((n) => {
    const g = FIELDS[n].axis_group;
    if (!groups.includes(g)) groups.push(g);
  });

  const scales = { x: { time: true } };
  groups.forEach((g) => {
    scales[g] = { range: AXIS_RANGES[g] || null };
  });

  const axes = [{ scale: "x", side: 2, grid: { stroke: "#2c3038", width: 1 } }];
  groups.forEach((g, i) => {
    axes.push({
      scale: g,
      side: i === 0 ? 3 : 1,
      label: g,
      stroke: "#8b909a",
      splits: () => axisSplits(g),
      grid: { show: i === 0, stroke: "#2c3038", width: 1 },
      ticks: { stroke: "#454c57" },
    });
  });
  return { scales, axes };
}

function rebuildPanelChart(panel) {
  panel.el.innerHTML = "";
  if (panel.uplot) {
    panel.uplot.destroy();
    panel.uplot = null;
  }
  const fieldNames = Array.from(panel.fields);
  if (fieldNames.length === 0) {
    panel.el.innerHTML = '<div class="empty-state">No channels selected — click a chip above to add one.</div>';
    return;
  }
  const { scales, axes } = scalesAndAxesFor(fieldNames);
  const series = [{ label: "time" }].concat(
    fieldNames.map((n) => ({
      label: fieldLabel(n),
      stroke: FIELDS[n].color,
      width: 2,
      scale: FIELDS[n].axis_group,
      points: { show: false },
    }))
  );

  const width = Math.max(320, panel.el.clientWidth || panel.el.parentElement.clientWidth - 24);
  panel.uplot = new uPlot(
    { width, height: 260, scales, axes, series, legend: { show: true } },
    buildPanelData(panel),
    panel.el
  );
}

function buildPanelData(panel) {
  const fieldNames = Array.from(panel.fields);
  return [buffer.ts].concat(fieldNames.map((n) => buffer[n]));
}

function redrawPanel(panel) {
  if (!panel.uplot) return;
  panel.uplot.setData(buildPanelData(panel), true);
}

window.addEventListener("resize", () => {
  livePanels.forEach((p) => {
    if (!p.uplot) return;
    const w = Math.max(320, p.el.clientWidth || p.el.parentElement.clientWidth - 24);
    p.uplot.setSize({ width: w, height: 260 });
  });
});

// ---------------------------------------------------------------- driver stints

let stintsCache = [];
const selectedStintIds = new Set();
const compareStagedFields = new Set(["engine_rpm", "vehicle_speed"]);

async function loadStints() {
  const res = await fetch("/api/stints");
  const data = await res.json();
  stintsCache = data.stints;
  renderStintSlots();
  renderStintTable();
}

function renderStintSlots() {
  const container = $("#stintSlots");
  container.innerHTML = "";
  for (let n = 1; n <= 6; n++) {
    const forSlot = stintsCache.filter((s) => s.driver_number === n);
    const active = forSlot.find((s) => s.end_ts === null);
    const lastKnown = forSlot[forSlot.length - 1];

    const row = document.createElement("div");
    row.className = "stint-slot" + (active ? " active" : "");

    const badge = document.createElement("div");
    badge.className = "slot-num";
    badge.textContent = n;
    badge.style.color = active ? DRIVER_PALETTE[n - 1] : "";

    const input = document.createElement("input");
    input.className = "slot-name";
    input.placeholder = `driver ${n}`;
    input.value = active ? active.driver_name : lastKnown ? lastKnown.driver_name : "";
    input.disabled = !!active;

    const btn = document.createElement("button");
    btn.className = "slot-btn" + (active ? " end" : "");
    btn.textContent = active ? "End" : "Start";
    btn.addEventListener("click", async () => {
      if (active) {
        await fetch("/api/stints/end", { method: "POST" });
      } else {
        const name = input.value.trim() || `Driver ${n}`;
        await fetch("/api/stints/start", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ driver_number: n, driver_name: name }),
        });
      }
      loadStints();
    });

    row.appendChild(badge);
    row.appendChild(input);
    row.appendChild(btn);
    container.appendChild(row);
  }
}

function renderStintTable() {
  const tbody = $("#stintTable tbody");
  tbody.innerHTML = "";
  stintsCache
    .slice()
    .reverse()
    .forEach((s) => {
      const tr = document.createElement("tr");
      if (selectedStintIds.has(s.id)) tr.classList.add("selected");

      const cb = document.createElement("input");
      cb.type = "checkbox";
      cb.checked = selectedStintIds.has(s.id);
      cb.disabled = !cb.checked && selectedStintIds.size >= 6;
      cb.addEventListener("change", () => {
        if (cb.checked) selectedStintIds.add(s.id);
        else selectedStintIds.delete(s.id);
        renderStintTable();
      });

      const tdCb = document.createElement("td");
      tdCb.appendChild(cb);

      const tdDriver = document.createElement("td");
      const dur = s.end_ts ? s.end_ts - s.start_ts : (Date.now() / 1000 - s.start_ts);
      tdDriver.innerHTML = `<div class="driver-cell"><span class="driver-swatch" style="background:${DRIVER_PALETTE[(s.driver_number - 1) % 6]}"></span>${s.driver_name} <span style="color:var(--text-low)">#${s.driver_number}</span></div>`;

      const tdDur = document.createElement("td");
      tdDur.textContent = fmtDuration(dur) + (s.end_ts ? "" : " (active)");

      tr.appendChild(tdCb);
      tr.appendChild(tdDriver);
      tr.appendChild(tdDur);
      tbody.appendChild(tr);
    });
}

// ---------------------------------------------------------------- comparison view

async function runComparison() {
  const ids = Array.from(selectedStintIds);
  const metrics = Array.from(compareStagedFields);
  const container = $("#comparePanels");
  container.innerHTML = "";

  if (ids.length === 0 || metrics.length === 0) {
    container.innerHTML = '<div class="empty-state">Select at least one stint and one channel.</div>';
    return;
  }

  const perStint = {};
  const downsampleNotes = [];
  await Promise.all(
    ids.map(async (id) => {
      // max_points bounds the response regardless of how long the stint
      // is -- a 13hr/5Hz stint (~230K rows) still comes back capped, via
      // server-side LTTB downsampling per channel (see db.py).
      const res = await fetch(`/api/history?stint_id=${id}&max_points=4000`);
      const data = await res.json();
      perStint[id] = data.points;
      if (data.downsampled) {
        const stint = stintsCache.find((s) => s.id === id);
        downsampleNotes.push(
          `${stint ? stint.driver_name : "stint " + id}: showing ${data.returned_count.toLocaleString()} of ${data.raw_count.toLocaleString()} points (downsampled)`
        );
      }
    })
  );

  if (downsampleNotes.length) {
    const note = document.createElement("div");
    note.className = "section-label";
    note.style.marginBottom = "12px";
    note.textContent = downsampleNotes.join(" · ");
    container.appendChild(note);
  }

  metrics.forEach((metric) => {
    const panelEl = document.createElement("div");
    panelEl.className = "chart-panel";
    const head = document.createElement("div");
    head.className = "chart-panel-head";
    head.innerHTML = `<strong style="font-family: var(--mono); font-size: 12.5px; padding: 2px 4px;">${fieldLabel(metric)}</strong>`;
    const body = document.createElement("div");
    body.className = "chart-canvas-wrap";
    panelEl.appendChild(head);
    panelEl.appendChild(body);
    container.appendChild(panelEl);

    const series = [{ label: "elapsed" }];
    const dataArrays = [[]];
    let maxLen = 0;

    ids.forEach((id) => {
      const stint = stintsCache.find((s) => s.id === id);
      const pts = perStint[id];
      if (!pts || pts.length === 0) return;
      const t0 = stint.start_ts;
      const xs = pts.map((p) => p.source_ts - t0);
      const ys = pts.map((p) => p[metric]);
      maxLen = Math.max(maxLen, xs.length);
      series.push({
        label: `${stint.driver_name} #${stint.driver_number}`,
        stroke: DRIVER_PALETTE[(stint.driver_number - 1) % 6],
        width: 2,
        points: { show: false },
      });
      dataArrays.push({ xs, ys });
    });

    // uPlot needs one shared x array; merge all stint timelines onto a
    // single sorted axis and let each series carry nulls where it has no
    // sample at that x (uPlot draws gaps for null, which is what we want).
    const allXs = new Set();
    dataArrays.slice(1).forEach((d) => d.xs.forEach((x) => allXs.add(Math.round(x * 4) / 4)));
    const xAxis = Array.from(allXs).sort((a, b) => a - b);
    const xIndex = new Map(xAxis.map((x, i) => [x, i]));

    const finalData = [xAxis];
    dataArrays.slice(1).forEach((d) => {
      const arr = new Array(xAxis.length).fill(null);
      d.xs.forEach((x, i) => {
        const key = Math.round(x * 4) / 4;
        const idx = xIndex.get(key);
        if (idx !== undefined) arr[idx] = d.ys[i];
      });
      finalData.push(arr);
    });

    const width = Math.max(320, body.clientWidth || body.parentElement.clientWidth - 24);
    new uPlot(
      {
        width,
        height: 260,
        scales: { x: { time: false } },
        axes: [
          { scale: "x", side: 2, values: (u, vals) => vals.map(fmtDuration), grid: { stroke: "#2c3038" } },
          { scale: "y", side: 3, stroke: "#8b909a", grid: { stroke: "#2c3038" }, ticks: { stroke: "#454c57" } },
        ],
        series,
        legend: { show: true },
      },
      finalData,
      body
    );
  });
}

init();
