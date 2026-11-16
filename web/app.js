// Avro 仪器事件格式升级平台 - 前端逻辑
const $ = (id) => document.getElementById(id);

async function api(path, opts = {}) {
  const r = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...opts,
    body: opts.body ? JSON.stringify(opts.body) : undefined,
  });
  const data = await r.json().catch(() => ({}));
  if (!r.ok || data.ok === false) {
    const err = new Error(data.error || `HTTP ${r.status}`);
    err.payload = data;
    throw err;
  }
  return data;
}

function esc(s) {
  return String(s ?? "").replace(/[&<>"]/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
}
function pretty(obj) {
  return JSON.stringify(obj, null, 2);
}
function badge(status) {
  const map = {
    pending: "待确认 pending", confirmed: "已确认 confirmed",
    rejected: "已拒绝 rejected",
  };
  return `<span class="badge ${esc(status)}">${esc(map[status] || status)}</span>`;
}

async function refresh() {
  let s;
  try {
    s = await api("/api/status");
  } catch (e) {
    $("writer-state").innerHTML = `<span class="badge offline">后端不可达</span>`;
    return;
  }
  renderStates(s);
  renderSwitches(s);
  renderEvents(s);
  renderDecodes(s);
  renderHistory(s);
  if (s.qc) renderQc(s.qc);
}

function renderStates(s) {
  const w = s.effective_writer;
  $("writer-state").innerHTML =
    `<div><b>v${w.version}</b> <span class="pill">fp ${esc(w.fingerprint.slice(0, 10))}</span></div>
     <pre>${esc(pretty(w.schema))}</pre>`;
  for (const [id, key] of [["reader-a", "analyzer-a"], ["reader-b", "analyzer-b"]]) {
    const a = s.analyzers[key];
    if (!a.reachable) {
      $(id).innerHTML = `<span class="badge offline">不可达：${esc(a.error || "")}</span>`;
      continue;
    }
    $(id).innerHTML =
      `<div><b>v${a.effective_version}</b> <span class="pill">${esc(a.analyzer_id)}</span></div>
       <pre>${esc(pretty(a.reader_schema))}</pre>`;
  }
}

function renderHistory(s) {
  const rows = s.writer_schemas.map((x) =>
    `<div class="state" style="margin-bottom:6px">
       <b>v${x.version}</b> <span class="pill">fp ${esc(x.fp.slice(0, 10))}</span>
       <span class="meta">${esc(x.created_at)}</span>
       <pre>${esc(pretty(x.schema))}</pre>
     </div>`).join("");
  $("schema-history").innerHTML = rows || "<div class='hint'>无</div>";
}

function switchRow(sw, isReader) {
  const target = isReader ? `目标：${esc(sw.analyzer_id)} · ` : "";
  return `<div class="switch-row">
      <div class="meta">${target}${esc(sw.switch_id)} · 版本 v${sw.base_version} → v${sw.version}
        · ${esc(sw.updated_at)} ${badge(sw.status)}</div>
      ${sw.last_error ? `<div class="err">${esc(sw.last_error)}</div>` : ""}
      <pre>${esc(pretty(sw.schema))}</pre>
    </div>`;
}

function renderSwitches(s) {
  $("writer-switches").innerHTML =
    s.writer_switches.map((x) => switchRow(x, false)).join("") ||
    "<div class='hint'>暂无切换记录</div>";
  $("reader-switches").innerHTML =
    s.reader_switches.map((x) => switchRow(x, true)).join("") ||
    "<div class='hint'>暂无切换记录</div>";
}

function renderEvents(s) {
  const tb = document.querySelector("#events-table tbody");
  tb.innerHTML = s.events.map((e) => `<tr>
      <td>${esc(e.id)}</td>
      <td>v${e.writer_version}<div class="pill">${esc(e.writer_fp.slice(0, 8))}</div></td>
      <td><pre>${esc(pretty(e.payload))}</pre></td>
      <td>${esc(e.ingested_at)}</td>
      <td>
        <button onclick="decodeNow('${esc(e.id)}','analyzer-a')">A 解码</button>
        <button onclick="decodeNow('${esc(e.id)}','analyzer-b')">B 解码</button>
      </td>
    </tr>`).join("") || `<tr><td colspan="5" class="hint">尚无留存事件</td></tr>`;
}

function renderDecodes(s) {
  const tb = document.querySelector("#decodes-table tbody");
  tb.innerHTML = s.decode_results.map((d) => `<tr>
      <td>${esc(d.decoded_at)}</td>
      <td>${esc(d.event_id)}</td>
      <td>${esc(d.analyzer_id)}</td>
      <td>v${esc(d.reader_version)}</td>
      <td>${d.decoded_ok
        ? '<span class="badge ok">成功</span>'
        : '<span class="badge fail">失败</span>'}</td>
      <td><pre>${esc(d.error || pretty(d.datum))}</pre></td>
    </tr>`).join("") || `<tr><td colspan="6" class="hint">暂无解码结果</td></tr>`;
}

window.decodeNow = async function (eventId, analyzerId) {
  try {
    await api("/api/events/decode", {
      method: "POST",
      body: { event_id: eventId, analyzer_id: analyzerId },
    });
    await refresh();
  } catch (e) { alert("解码请求失败：" + e.message); }
};

function showMsg(id, text, ok) {
  const el = $(id);
  el.className = ok ? "msg-ok" : "msg-err";
  el.textContent = text;
}

// ---------------- 事件入仓 ----------------
async function ingest(body) {
  try {
    const r = await api("/api/events", { method: "POST", body });
    let msg = `事件已入仓：${r.event_id}（写入 v${r.writer_version}，${r.size_bytes} 字节 Avro）`;
    for (const d of r.decodes || []) {
      msg += d.error
        ? `\n[${d.analyzer_id}] 解码失败: ${d.error}`
        : `\n[${d.analyzer_id}] 解码${d.decoded_ok ? "成功" : "失败"} reader=v${d.reader_version}`;
    }
    const allOk = (r.decodes || []).every(
      (d) => !d.error && d.decoded_ok === true);
    showMsg("ingest-msg", msg, allOk);
    await refresh();
  } catch (e) {
    showMsg("ingest-msg", "入仓被拒：" + e.message, false);
  }
}

$("btn-ingest-simple").onclick = () => {
  const raw = $("ev-measurement").value.trim();
  const measurement = Number(raw);
  ingest({
    sample_id: $("ev-sample").value,
    project: $("ev-project").value,
    measurement: Number.isFinite(measurement) ? measurement : raw,
    decode_on: [
      ...($("ev-dec-a").checked ? ["analyzer-a"] : []),
      ...($("ev-dec-b").checked ? ["analyzer-b"] : []),
    ],
  });
};
$("btn-ingest-json").onclick = () => {
  try {
    const payload = JSON.parse($("ev-payload").value);
    ingest({
      payload,
      decode_on: [
        ...($("ev-dec-a").checked ? ["analyzer-a"] : []),
        ...($("ev-dec-b").checked ? ["analyzer-b"] : []),
      ],
    });
  } catch (e) {
    showMsg("ingest-msg", "JSON 解析失败：" + e.message, false);
  }
};

// ---------------- 格式切换 ----------------
$("btn-writer-switch").onclick = async () => {
  let schema;
  try { schema = JSON.parse($("writer-schema").value); }
  catch (e) { return showMsg("switch-msg", "Schema JSON 解析失败：" + e.message, false); }
  try {
    const r = await api("/api/switches/writer", {
      method: "POST", body: { schema },
    });
    showMsg("switch-msg",
      `写入切换 ${r.switch.switch_id} 状态：${r.switch.status}` +
      (r.switch.last_error ? "\n" + r.switch.last_error : ""),
      r.switch.status === "confirmed");
  } catch (e) { showMsg("switch-msg", "写入切换被阻止：\n" + e.message, false); }
  await refresh();
};

$("btn-reader-switch").onclick = async () => {
  let schema;
  try { schema = JSON.parse($("reader-schema").value); }
  catch (e) { return showMsg("switch-msg", "Schema JSON 解析失败：" + e.message, false); }
  try {
    const r = await api("/api/switches/reader", {
      method: "POST",
      body: { analyzer_id: $("reader-target").value, schema },
    });
    showMsg("switch-msg",
      `读取切换 ${r.switch.switch_id} 状态：${r.switch.status}` +
      (r.switch.last_error ? "\n" + r.switch.last_error : ""),
      r.switch.status === "confirmed");
  } catch (e) { showMsg("switch-msg", "读取切换被阻止：\n" + e.message, false); }
  await refresh();
};

// ---------------- 对账 ----------------
$("btn-reconcile").onclick = async () => {
  try {
    const r = await api("/api/reconcile", { method: "POST" });
    $("reconcile-msg").textContent =
      "对账完成：writer " + JSON.stringify(r.result.writer) +
      " / reader " + JSON.stringify(r.result.reader);
    $("reconcile-msg").className = "msg-ok";
  } catch (e) {
    $("reconcile-msg").textContent = "对账失败：" + e.message;
    $("reconcile-msg").className = "msg-err";
  }
  await refresh();
};

// ---------------- 故障注入 ----------------
$("btn-sim").onclick = async () => {
  try {
    const r = await api("/api/simulate", {
      method: "POST",
      body: {
        delay_ms: Number($("sim-delay").value || 0),
        drop_writer_switch: $("sim-drop-writer").checked,
        drop_reader_switch: $("sim-drop-reader").checked,
        analyzer_id: $("sim-analyzer").value || null,
        drop_analyzer_switch: $("sim-drop-sw").checked,
        drop_analyzer_decode: $("sim-drop-dec").checked,
      },
    });
    $("sim-msg").textContent = "已应用：" + JSON.stringify(r.simulation);
    $("sim-msg").className = "msg-ok";
  } catch (e) {
    $("sim-msg").textContent = "故障注入失败：" + e.message;
    $("sim-msg").className = "msg-err";
  }
};

// ---------------- 项目质控 ----------------
function qcStatusBadge(run) {
  if (run.status === "scanning") {
    return '<span class="badge pending">扫描中 scanning</span>';
  }
  if (run.status === "failed") {
    return '<span class="badge rejected">已失败 failed（未发布）</span>';
  }
  if (run.is_current) {
    return '<span class="badge confirmed">当前有效 published</span>';
  }
  return '<span class="badge stale">待重算 stale</span>';
}

function renderQcLimits(qc) {
  const tb = document.querySelector("#qc-limits-table tbody");
  tb.innerHTML = qc.limits.map((l) => `<tr>
      <td>${esc(l.project)}</td>
      <td>v${l.version}</td>
      <td>${esc(l.lower_limit)}</td>
      <td>${esc(l.upper_limit)}</td>
      <td>${esc(l.updated_at)}</td>
    </tr>`).join("") || `<tr><td colspan="5" class="hint">尚未配置任何项目限值</td></tr>`;
}

function renderQcRuns(qc) {
  const box = $("qc-runs");
  if (!qc.runs.length) {
    box.innerHTML = "<div class='hint'>暂无核查任务</div>";
    return;
  }
  box.innerHTML = qc.runs.map((r) => {
    const stale = (r.stale_reasons || [])
      .map((x) => `<div class="err">${esc(x)}</div>`).join("");
    const err = r.error ? `<div class="err">${esc(r.error)}</div>` : "";
    let rows = "";
    if (r.status === "published" && r.events && r.events.length) {
      rows = `<table class="qc-events">
        <thead><tr><th>#</th><th>事件号</th><th>结论</th><th>A 测值 (v${esc(r.reader_a_version)})</th>
        <th>B 测值 (v${esc(r.reader_b_version)})</th><th>原因</th></tr></thead>
        <tbody>${r.events.map((e) => `<tr>
          <td>${e.seq + 1}</td>
          <td>${esc(e.event_id)}</td>
          <td>${e.verdict === "usable"
            ? '<span class="badge ok">可用</span>'
            : '<span class="badge fail">问题</span>'}</td>
          <td>${e.a_value === null ? "—" : esc(e.a_value)}</td>
          <td>${e.b_value === null ? "—" : esc(e.b_value)}</td>
          <td>${(e.reasons || []).map((x) => esc(x)).join("<br/>") || ""}</td>
        </tr>`).join("")}</tbody></table>`;
    } else if (r.status === "published" && (!r.events || !r.events.length)) {
      rows = "<div class='hint'>该项目在核查时没有留存事件（空集合结论）。</div>";
    }
    return `<div class="switch-row qc-run">
      <div class="meta">${esc(r.run_id)} · 项目：${esc(r.project)}
        · 限值 v${esc(r.limits_version)} [${esc(r.lower_limit)}, ${esc(r.upper_limit)}]
        · 事件 ${r.event_count} 个（hash ${esc((r.event_set_hash || "").slice(0, 10))}）
        · A v${esc(r.reader_a_version)} / B v${esc(r.reader_b_version)}
        · 可用 ${r.usable_count} / 问题 ${r.issue_count}
        · ${esc(r.updated_at)} ${qcStatusBadge(r)}</div>
      ${stale}${err}${rows}
    </div>`;
  }).join("");
}

function renderQc(qc) {
  renderQcLimits(qc);
  renderQcRuns(qc);
}

function qcMsg(text, ok) {
  const el = $("qc-msg");
  el.className = ok ? "msg-ok inline-msg" : "msg-err inline-msg";
  el.textContent = text;
}

$("btn-qc-limits").onclick = async () => {
  const project = $("qc-project").value.trim();
  const lower = Number($("qc-lower").value);
  const upper = Number($("qc-upper").value);
  if (!project) return qcMsg("项目名不能为空", false);
  if (!Number.isFinite(lower) || !Number.isFinite(upper) || lower < 0 || upper < 0) {
    return qcMsg("限值必须是非负有限数值", false);
  }
  if (lower > upper) return qcMsg("下限不能大于上限", false);
  try {
    const r = await api("/api/qc/limits", {
      method: "POST",
      body: { project, lower_limit: lower, upper_limit: upper },
    });
    qcMsg(`已保存限值（v${r.limits.version}）`, true);
  } catch (e) { qcMsg("限值保存失败：" + e.message, false); }
  await refresh();
};

$("btn-qc-scan").onclick = async () => {
  const project = $("qc-project").value.trim();
  if (!project) return qcMsg("项目名不能为空", false);
  try {
    const r = await api("/api/qc/scan", {
      method: "POST",
      body: { project },
    });
    qcMsg(`核查任务已启动：${r.run.run_id}（扫描中，页面将自动刷新进度）`, true);
  } catch (e) { qcMsg("核查启动失败：" + e.message, false); }
  await refresh();
};

// ---------------- 初始化：填入示例 schema ----------------
async function initForms() {
  const base = await api("/api/default-schema");
  const v2 = structuredClone(base);
  v2.fields.push({ name: "instrument_id", type: "string", default: "UNKNOWN" });
  v2.fields.push({ name: "quality_flag", type: ["null", "string"], default: null });
  $("writer-schema").value = pretty(v2);

  const readerV2 = structuredClone(base);
  readerV2.fields.push({ name: "temperature", type: "double", default: 0.0 });
  $("reader-schema").value = pretty(readerV2);
}

initForms();
refresh();
setInterval(refresh, 3000);
