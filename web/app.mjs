// app.mjs — 页面逻辑：事件编排、Worker 回放、单步查看
import { replay, ValidationError, MAX_EVENTS } from './engine.mjs';

const $ = (s) => document.querySelector(s);

const els = {
  channels: $('#channels'),
  rows: $('#event-rows'),
  count: $('#event-count'),
  validation: $('#validation'),
  run: $('#run'),
  clear: $('#clear'),
  workerState: $('#worker-state'),
  preset: $('#preset'),
  prev: $('#prev'),
  next: $('#next'),
  play: $('#play'),
  scrub: $('#scrub'),
  stage: $('#stage'),
  totals: $('#totals'),
  buffer: $('#buffer'),
  checkpoints: $('#checkpoints'),
  storage: $('#storage'),
  notes: $('#notes'),
  healthDot: $('#health-dot'),
  healthText: $('#health-text'),
};

// ---- 事件编辑表 ----
let events = [];

function typeLabel(t) {
  return { data: '数据', barrier: '屏障', crash: '故障', reopen: '重开' }[t] || t;
}

function renderRows() {
  const channels = Number(els.channels.value);
  els.rows.innerHTML = '';
  events.forEach((ev, i) => {
    const tr = document.createElement('tr');
    tr.dataset.index = i;

    const tdNum = document.createElement('td');
    tdNum.className = 'row-num';
    tdNum.textContent = i;
    tr.appendChild(tdNum);

    const tdType = document.createElement('td');
    const selType = document.createElement('select');
    for (const t of ['data', 'barrier', 'crash', 'reopen']) {
      const o = document.createElement('option');
      o.value = t;
      o.textContent = typeLabel(t);
      if (ev.type === t) o.selected = true;
      selType.appendChild(o);
    }
    selType.onchange = () => {
      events[i] = freshEvent(selType.value, channels);
      renderRows();
    };
    tdType.appendChild(selType);
    tr.appendChild(tdType);

    const tdCh = document.createElement('td');
    if (ev.type === 'data' || ev.type === 'barrier') {
      const selCh = document.createElement('select');
      for (let c = 1; c <= channels; c++) {
        const o = document.createElement('option');
        o.value = String(c);
        o.textContent = '通道 ' + c;
        if (Number(ev.channel) === c) o.selected = true;
        selCh.appendChild(o);
      }
      selCh.onchange = () => { ev.channel = Number(selCh.value); };
      tdCh.appendChild(selCh);
    } else {
      tdCh.innerHTML = '<span class="muted">—</span>';
    }
    tr.appendChild(tdCh);

    const tdNum2 = document.createElement('td');
    if (ev.type === 'data') {
      const inp = document.createElement('input');
      inp.type = 'number'; inp.min = '1'; inp.value = ev.seq;
      inp.oninput = () => { ev.seq = inp.value === '' ? '' : Number(inp.value); };
      tdNum2.appendChild(inp);
    } else if (ev.type === 'barrier') {
      const inp = document.createElement('input');
      inp.type = 'number'; inp.min = '1'; inp.value = ev.checkpoint;
      inp.oninput = () => { ev.checkpoint = inp.value === '' ? '' : Number(inp.value); };
      tdNum2.appendChild(inp);
    } else {
      tdNum2.innerHTML = '<span class="muted">—</span>';
    }
    tr.appendChild(tdNum2);

    const tdVal = document.createElement('td');
    if (ev.type === 'data') {
      const inp = document.createElement('input');
      inp.type = 'number'; inp.step = 'any'; inp.value = ev.value;
      inp.oninput = () => { ev.value = inp.value === '' ? '' : Number(inp.value); };
      tdVal.appendChild(inp);
    } else {
      tdVal.innerHTML = '<span class="muted">—</span>';
    }
    tr.appendChild(tdVal);

    const tdStage = document.createElement('td');
    if (ev.type === 'crash') {
      const sel = document.createElement('select');
      for (const [v, label] of [['', '立即'], ['intent', '意图后'], ['snapshot', '快照后（半完成）']]) {
        const o = document.createElement('option');
        o.value = v; o.textContent = label;
        if ((ev.stage || '') === v) o.selected = true;
        sel.appendChild(o);
      }
      sel.onchange = () => { ev.stage = sel.value || null; };
      tdStage.appendChild(sel);
    } else {
      tdStage.innerHTML = '<span class="muted">—</span>';
    }
    tr.appendChild(tdStage);

    const tdDel = document.createElement('td');
    tdDel.innerHTML = '<span class="del" title="删除">✕</span>';
    tdDel.querySelector('.del').onclick = () => { events.splice(i, 1); renderRows(); updateCount(); };
    tr.appendChild(tdDel);

    els.rows.appendChild(tr);
  });
  updateCount();
}

function updateCount() {
  els.count.textContent = `${events.length} / ${MAX_EVENTS} 项`;
  els.count.style.color = events.length > MAX_EVENTS ? 'var(--err)' : '';
}

function freshEvent(type, channels) {
  if (type === 'data') return { type, channel: 1, seq: 1, value: 1 };
  if (type === 'barrier') return { type, channel: 1, checkpoint: 1 };
  if (type === 'crash') return { type, stage: null };
  return { type };
}

document.querySelectorAll('[data-add]').forEach((btn) => {
  btn.onclick = () => {
    if (events.length >= MAX_EVENTS) return;
    const channels = Number(els.channels.value);
    const ev = freshEvent(btn.dataset.add, channels);
    if (ev.type === 'data') {
      // 沿用上一条数据的通道，序号取该通道已用最大序号 +1
      const lastData = [...events].reverse().find((e) => e.type === 'data');
      if (lastData) ev.channel = Number(lastData.channel);
      const maxSeq = events.reduce(
        (m, e) => (e.type === 'data' && Number(e.channel) === ev.channel ? Math.max(m, Number(e.seq) || 0) : m),
        0
      );
      ev.seq = maxSeq + 1;
    }
    if (ev.type === 'barrier') {
      const maxCp = events.reduce((m, e) => (e.type === 'barrier' ? Math.max(m, Number(e.checkpoint) || 0) : m), 0);
      ev.checkpoint = maxCp + 1;
    }
    events.push(ev);
    renderRows();
  };
});

els.channels.onchange = () => renderRows();
els.clear.onclick = () => { events = []; renderRows(); hideValidation(); resetOutput(); };

// ---- 预设 ----
const PRESETS = {
  basic: {
    channels: 2,
    events: [
      { type: 'data', channel: 1, seq: 1, value: 10 },
      { type: 'data', channel: 2, seq: 1, value: 1 },
      { type: 'barrier', channel: 1, checkpoint: 1 },
      { type: 'barrier', channel: 2, checkpoint: 1 },
      { type: 'data', channel: 1, seq: 2, value: 5 },
      { type: 'data', channel: 2, seq: 2, value: 2 },
    ],
  },
  buffer: {
    channels: 2,
    events: [
      { type: 'data', channel: 1, seq: 1, value: 10 },
      { type: 'barrier', channel: 1, checkpoint: 1 },
      { type: 'data', channel: 1, seq: 2, value: 7 },   // 被缓存
      { type: 'data', channel: 2, seq: 1, value: 3 },
      { type: 'data', channel: 1, seq: 3, value: 4 },   // 被缓存
      { type: 'barrier', channel: 2, checkpoint: 1 },   // 对齐：按原序释放 7 再 4
      { type: 'data', channel: 2, seq: 2, value: 2 },
    ],
  },
  'crash-snap': {
    channels: 2,
    events: [
      { type: 'data', channel: 1, seq: 1, value: 10 },
      { type: 'data', channel: 2, seq: 1, value: 5 },
      { type: 'barrier', channel: 1, checkpoint: 1 },
      { type: 'barrier', channel: 2, checkpoint: 1 },   // cp1 完整发布
      { type: 'data', channel: 1, seq: 2, value: 9 },
      { type: 'crash', stage: 'snapshot' },             // 在下一检查点快照后故障
      { type: 'barrier', channel: 2, checkpoint: 2 },
      { type: 'barrier', channel: 1, checkpoint: 2 },   // 写了快照但未发布 -> 中断
      { type: 'reopen' },                                // 丢弃半成品 cp2，从 cp1 后重放
      { type: 'data', channel: 1, seq: 2, value: 9 },   // 重放，不重复计入
      { type: 'barrier', channel: 2, checkpoint: 2 },
      { type: 'barrier', channel: 1, checkpoint: 2 },
      { type: 'data', channel: 2, seq: 2, value: 6 },
    ],
  },
  'crash-intent': {
    channels: 2,
    events: [
      { type: 'data', channel: 1, seq: 1, value: 4 },
      { type: 'crash', stage: 'intent' },
      { type: 'barrier', channel: 1, checkpoint: 1 },   // 写意图后立即中断
      { type: 'reopen' },                                // 无任何已发布检查点，从空状态开始
      { type: 'data', channel: 1, seq: 1, value: 4 },
      { type: 'data', channel: 2, seq: 1, value: 2 },
      { type: 'barrier', channel: 1, checkpoint: 1 },
      { type: 'barrier', channel: 2, checkpoint: 1 },
    ],
  },
  seqgap: {
    channels: 2,
    events: [
      { type: 'data', channel: 1, seq: 1, value: 3 },
      { type: 'data', channel: 1, seq: 3, value: 3 }, // 跳号，定位本事件
    ],
  },
  dupbarrier: {
    channels: 2,
    events: [
      { type: 'barrier', channel: 1, checkpoint: 1 },
      { type: 'barrier', channel: 1, checkpoint: 1 }, // 通道 1 重复屏障
      { type: 'barrier', channel: 2, checkpoint: 1 },
    ],
  },
  earlyrelease: {
    channels: 2,
    events: [
      { type: 'barrier', channel: 1, checkpoint: 1 },
      { type: 'barrier', channel: 1, checkpoint: 2 }, // cp1 未对齐就到 cp2：交叉对齐
    ],
  },
};

els.preset.onchange = () => {
  const p = PRESETS[els.preset.value];
  if (!p) return;
  els.channels.value = String(p.channels);
  events = p.events.map((e) => ({ ...e }));
  renderRows();
  hideValidation();
};

// ---- Worker 回放（Worker 不可用时回退主线程纯引擎，并明确标注） ----
let current = null;
let step = 0;
let playTimer = null;
let useWorker = false;

function plan() {
  return { channels: Number(els.channels.value), events: events.map((e) => ({ ...e })) };
}

function hideValidation() {
  els.validation.classList.add('hidden');
  els.rows.querySelectorAll('tr').forEach((tr) => (tr.style.outline = ''));
}

function showValidation(err) {
  els.validation.classList.remove('hidden');
  els.validation.innerHTML =
    `🛑 <span class="loc">首个出错事件 #${err.index}</span>（${err.index >= 0 && events[err.index] ? typeLabel(events[err.index].type) : '配置'}）：${err.message}` +
    ` <span class="muted">[code=${err.code}]</span>`;
  if (err.index >= 0) {
    const tr = els.rows.querySelector(`tr[data-index="${err.index}"]`);
    if (tr) {
      tr.style.outline = '2px solid var(--err)';
      tr.scrollIntoView({ block: 'nearest' });
    }
  }
}

function runOnMainThread() {
  // 主线程回退：纯引擎产出 frames，并逐帧累积出精确的存储视图（与 Worker 写入同构）
  const r = replay(plan());
  const acc = new Map();
  for (const frame of r.frames) {
    for (const w of frame.persistence) acc.set(w.key, { key: w.key, stage: w.stage, checkpoint: w.checkpoint });
    if (frame.cleanup) for (const k of frame.cleanup.deleted) acc.delete(k);
    const published = new Set([...acc.values()].filter((v) => v.stage === 'published').map((v) => v.checkpoint));
    frame.storageAfter = [...acc.values()]
      .map((v) => ({
        ...v,
        complete: v.stage === 'published' || (v.stage === 'snapshot' && published.has(v.checkpoint)),
      }))
      .sort((a, b) => a.key.localeCompare(b.key, undefined, { numeric: true }));
  }
  return r;
}

function sendWorker(msg) {
  return new Promise((resolve, reject) => {
    const w = new Worker('./worker.mjs', { type: 'module' });
    const timer = setTimeout(() => { w.terminate(); reject(new Error('worker-timeout')); }, 4000);
    w.onmessage = (e) => {
      clearTimeout(timer);
      w.terminate();
      const m = e.data;
      if (m.type === 'result') resolve({ result: m.result, worker: true });
      else if (m.type === 'validation-error') reject(Object.assign(new Error(m.message), { index: m.index, code: m.code, validation: true }));
      else reject(new Error(m.message || 'worker error'));
    };
    w.onerror = (e) => { clearTimeout(timer); reject(new Error(e.message)); };
    w.postMessage(msg);
  });
}

els.run.onclick = async () => {
  hideValidation();
  els.workerState.textContent = '回放中…';
  try {
    let result;
    try {
      const out = await sendWorker({ type: 'run', plan: plan() });
      result = out.result;
      useWorker = true;
      els.workerState.textContent = '✓ 已在 Worker 内完成三阶段持久化';
      els.workerState.style.color = 'var(--ok)';
    } catch (wErr) {
      if (wErr && wErr.validation) throw wErr;
      // Worker 不可用 -> 主线程回退
      result = runOnMainThread();
      useWorker = false;
      els.workerState.textContent = '⚠ Worker 不可用，已用主线程引擎同构回放（持久化视图为模拟）';
      els.workerState.style.color = 'var(--warn)';
    }
    current = result;
    step = result.frames.length - 1;
    render();
  } catch (err) {
    if (err.validation || err instanceof ValidationError) {
      showValidation({ index: err.index, message: err.message, code: err.code });
    } else {
      showValidation({ index: -1, message: String(err.message || err), code: 'FATAL' });
    }
    els.workerState.textContent = '';
  }
};

// ---- 单步渲染 ----
function resetOutput() {
  current = null;
  els.scrub.disabled = true;
  els.scrub.max = 0;
  els.stage.className = 'stage empty';
  els.stage.textContent = '尚未运行 —— 编排事件后点击「整段回放」';
  els.totals.innerHTML = '';
  els.buffer.innerHTML = '';
  els.checkpoints.innerHTML = '<span class="muted">尚无</span>';
  els.storage.innerHTML = '<span class="muted">—</span>';
  els.notes.innerHTML = '';
}

els.scrub.oninput = () => { step = Number(els.scrub.value); render(); };
els.prev.onclick = () => { if (current && step > 0) { step--; render(); } };
els.next.onclick = () => { if (current && step < current.frames.length - 1) { step++; render(); } };
els.play.onclick = () => {
  if (!current) return;
  if (playTimer) { clearInterval(playTimer); playTimer = null; els.play.textContent = '自动播放'; return; }
  els.play.textContent = '暂停';
  playTimer = setInterval(() => {
    if (step < current.frames.length - 1) { step++; render(); }
    else { clearInterval(playTimer); playTimer = null; els.play.textContent = '自动播放'; }
  }, 850);
};

function evDesc(ev) {
  if (ev.type === 'data') return `数据 通道${ev.channel} #${ev.seq} = ${ev.value}`;
  if (ev.type === 'barrier') return `屏障 通道${ev.channel} → 检查点 ${ev.checkpoint}`;
  if (ev.type === 'crash') return `故障 CRASH${ev.stage ? '（注入：' + (ev.stage === 'intent' ? '意图后' : '快照后') + '）' : '（立即）'}`;
  return '重开 REOPEN';
}

function render() {
  if (!current) return;
  const n = current.frames.length;
  els.scrub.disabled = false;
  els.scrub.max = String(n - 1);
  els.scrub.value = String(step);
  const f = current.frames[step];

  // 事件条
  els.stage.className = 'stage';
  const phase = f.crashed ? 'crashed' : f.phase;
  const phaseText = { idle: '运行中', intent: '阶段1 意图', snapshot: '阶段2 快照', published: '阶段3 已发布', crashed: '已故障' }[phase] || phase;
  els.stage.innerHTML =
    `<div class="stage-ev"><span class="tag ${f.event.type}">${typeLabel(f.event.type)}</span><span class="big">${evDesc(f.event)}</span></div>` +
    `<div class="stage-result">步 ${step}/${n - 1} <span class="badge ${phase}">${phaseText}</span>` +
    (f.skippedDead ? ' <span class="badge crashed">故障态·未处理</span>' : '') +
    (f.reopened ? ' <span class="badge published">已重开</span>' : '') + `</div>`;

  // 累计值
  const ch = current.channels;
  let th = '';
  for (let c = 1; c <= ch; c++) {
    th += `<div class="tch"><span>通道 ${c}（下一应到 #${f.nextSeq[c]}）</span><b>${fmt(f.perChannel[c])}</b></div>`;
  }
  th += `<div class="tgrand"><span>累计总值</span><b>${fmt(f.total)}</b></div>`;
  els.totals.innerHTML = th;

  // 缓存
  els.buffer.innerHTML = f.buffered.length
    ? f.buffered.map((b) =>
        `<div class="buf-item"><span>通道${b.channel} #${b.seq} = ${b.value}</span><span class="muted">${b.reason.replace(/检查点 (\d+) 的屏障已在通道 (\d+) 先到.*/, '屏障先到·待 cp$1 对齐')}</span></div>`).join('')
    : '<span class="muted">空</span>';

  // 检查点卡片：纳入范围 + 释放缓存 + 恢复起点
  const cp = f.checkpoints;
  if (!cp.length) {
    els.checkpoints.innerHTML = '<span class="muted">尚无已发布检查点</span>';
  } else {
    els.checkpoints.innerHTML = cp.map((c) => {
      const ranges = c.included
        .map((r) => r.lastSeq == null ? `通道${r.channel}: —` : `通道${r.channel}: #${r.firstSeq}–#${r.lastSeq}`)
        .join('；');
      const rel = c.released.length ? `释放缓存 ${c.released.map((r) => `${r.channel}#${r.seq}`).join('、')}` : '无缓存';
      return `<div class="cp-item">
        <div class="cp-head"><span class="badge published">CP ${c.id} 已发布</span>
        <span class="cp-range">封存于事件 #${c.sealedAtEventIndex}；纳入 ${ranges}；${rel}</span></div>
        <b>${fmt(c.total)}</b></div>`;
    }).join('');
  }
  if (f.recoveryStart) {
    const r = f.recoveryStart;
    els.checkpoints.innerHTML +=
      `<div class="cp-item" style="border-top:1px solid var(--line);margin-top:6px;padding-top:8px">
        <span>↻ 恢复起点：${r.checkpoint == null ? '空状态（无已发布检查点）' : `检查点 ${r.checkpoint} 之后（事件 #${r.afterEventIndex} 之后）`}，各通道序号 ${JSON.stringify(r.inputSeq.slice(1))}</span></div>`;
  }

  // 存储层
  const store = f.storageAfter || [];
  els.storage.innerHTML = store.length
    ? '<div class="storage-kv">' + store.map((s) => {
        const cls = s.complete ? 'good' : 'bad';
        const mark = s.complete ? '✓完整' : '⚠半成品';
        return `<div><span class="k">${s.key}</span> <span class="${cls}">${mark}</span></div>`;
      }).join('') + '</div>'
    : '<span class="muted">（空）</span>';

  // 日志
  els.notes.innerHTML = f.notes.length
    ? f.notes.map((t) => `<li>${t}</li>`).join('')
    : '<li class="muted">（本步无附加说明）</li>';
}

function fmt(x) { return Number.isInteger(x) ? String(x) : String(Number(x.toFixed(6))); }

// ---- 健康检查 ----
async function health() {
  try {
    const res = await fetch('./health', { cache: 'no-store' });
    if (res.ok) {
      const j = await res.json().catch(() => ({}));
      els.healthDot.className = 'dot ok';
      els.healthText.textContent = '健康 ' + (j.status || 'ok');
    } else {
      els.healthDot.className = 'dot bad';
      els.healthText.textContent = '健康检查失败 ' + res.status;
    }
  } catch {
    els.healthDot.className = 'dot bad';
    els.healthText.textContent = '健康端点不可达';
  }
}

resetOutput();
renderRows();
health();
setInterval(health, 10000);
