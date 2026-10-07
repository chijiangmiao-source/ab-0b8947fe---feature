// app.mjs — 页面逻辑：事件编排、Worker 回放、单步查看、累计值来源分解
import { replay, ValidationError, MAX_EVENTS, sourceBreakdown } from './engine.mjs';

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
  bdCp: $('#bd-cp'),
  bdStep: $('#bd-step'),
  breakdown: $('#breakdown'),
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
  resetBreakdown();
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
        <span class="cp-range">封存于事件 #${c.sealedAtEventIndex}；纳入 ${ranges}；${rel}</span>
        <button class="btn btn-mini" data-bd-cp="${c.id}" title="以该完整发布检查点为分界，分解当前累计值来源">🧩 来源分解</button></div>
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

  refreshBreakdown();
}

// ---- 累计值来源分解 ----
let bdState = { cpId: null, stepIndex: null };

function resetBreakdown() {
  bdState = { cpId: null, stepIndex: null };
  els.bdCp.innerHTML = '';
  els.bdStep.innerHTML = '';
  els.breakdown.className = 'muted';
  els.breakdown.innerHTML = '整段回放后在此选择已完整发布的检查点与步骤';
}

els.bdCp.onchange = () => {
  bdState.cpId = els.bdCp.value === '' ? null : Number(els.bdCp.value);
  renderBreakdown();
};
els.bdStep.onchange = () => {
  bdState.stepIndex = els.bdStep.value === '' ? null : Number(els.bdStep.value);
  renderBreakdown();
};
// 检查点卡片上的「来源分解」入口：选中该检查点，步骤默认取当前查看步
els.checkpoints.addEventListener('click', (e) => {
  const btn = e.target.closest('[data-bd-cp]');
  if (!btn || !current) return;
  bdState.cpId = Number(btn.dataset.bdCp);
  bdState.stepIndex = step;
  refreshBreakdown();
});

function defaultCpFor(frame) {
  // 故障重开后的帧默认选中恢复采用的检查点；否则选最新已发布检查点
  const rc = frame.recoveryStart?.checkpoint;
  if (rc != null && frame.checkpoints.some((c) => c.id === rc)) return rc;
  return frame.checkpoints.length ? frame.checkpoints[frame.checkpoints.length - 1].id : null;
}

function refreshBreakdown() {
  if (!current) { resetBreakdown(); return; }

  // 分解步骤以下拉所选为准（跨主时间轴拖动保持稳定），首次进入跟随当前查看步
  let s = Number.isInteger(bdState.stepIndex) && bdState.stepIndex >= 0 && bdState.stepIndex < current.frames.length
    ? bdState.stepIndex
    : step;

  // 检查点须在所选步骤那一帧已完整发布；步骤不得早于封存步骤。至多迭代两轮收敛
  let cpVal = null;
  let sealIdx = null;
  for (let pass = 0; pass < 2; pass++) {
    const fr = current.frames[s];
    const cps = fr.checkpoints;
    cpVal = cps.some((c) => c.id === bdState.cpId)
      ? bdState.cpId
      : defaultCpFor(fr);
    sealIdx = cpVal == null ? null : cps.find((c) => c.id === cpVal)?.sealedAtEventIndex ?? null;
    const clamped = Number.isInteger(sealIdx) ? Math.max(s, sealIdx) : s;
    if (clamped === s) break;
    s = clamped;
  }
  bdState.cpId = cpVal;
  bdState.stepIndex = s;

  // 检查点下拉：列出所选步骤那一帧可见的已完整发布检查点；半成品快照永不出现
  const cps = current.frames[s].checkpoints;
  els.bdCp.innerHTML =
    (cps.length ? '' : '<option value="">（无已发布检查点）</option>') +
    cps.map((c) => `<option value="${c.id}"${c.id === cpVal ? ' selected' : ''}>检查点 ${c.id}（封存于事件 #${c.sealedAtEventIndex}）</option>`).join('');

  // 步骤下拉：列出全部步；故障冻结步与早于封存步禁用
  els.bdStep.innerHTML = current.frames.map((fr, i) => {
    const frozen = fr.alive === false;
    const beforeSeal = Number.isInteger(sealIdx) && i < sealIdx;
    const disabled = frozen || beforeSeal;
    return `<option value="${i}"${i === s ? ' selected' : ''}${disabled ? ' disabled' : ''}>步骤 #${i}${frozen ? '（故障冻结）' : beforeSeal ? '（早于封存）' : ''}</option>`;
  }).join('');

  renderBreakdown();
}

function rangeText(ranges) {
  if (!ranges.length) return '—';
  return ranges.map((r) => (r.first === r.last ? `#${r.first}` : `#${r.first}–#${r.last}`)).join('、');
}

function renderBreakdown() {
  if (!current || bdState.cpId == null || !Number.isInteger(bdState.stepIndex)) {
    els.breakdown.className = 'muted';
    els.breakdown.innerHTML = '请选择已完整发布的检查点与不早于它的步骤';
    return;
  }
  const res = sourceBreakdown(current, bdState.cpId, bdState.stepIndex);
  if (!res.ok) {
    // 明确原因，且不残留旧分解
    els.breakdown.className = 'bd-reason';
    els.breakdown.innerHTML =
      `⛔ 无法生成来源分解：${res.reason}` +
      (res.detail ? `<div class="muted" style="margin-top:4px">${res.detail}</div>` : '') +
      ` <span class="code">[${res.code}]</span>`;
    return;
  }

  const rows = res.channels.map((x) => {
    const items = x.post.items.length
      ? x.post.items.map((it) => `#${it.seq}=${fmt(it.value)}`).join('、')
      : '<span class="muted">—</span>';
    const buffered = x.bufferedSeqs.length
      ? `<span class="muted">（缓存未释放：${x.bufferedSeqs.map((s) => `#${s}`).join('、')}，两部分均不含）</span>`
      : '';
    const mark = x.matches ? '<span class="bd-ok">✓</span>' : '<span class="bd-bad">✗</span>';
    return `<tr>
      <td>通道 ${x.channel}</td>
      <td><span class="bd-seq">${x.sealed.range ? `#${x.sealed.range.first}–#${x.sealed.range.last}` : '—'}</span></td>
      <td class="bd-num">${fmt(x.sealed.total)}</td>
      <td><span class="bd-seq">${rangeText(x.post.ranges)}</span></td>
      <td class="bd-items">${items}</td>
      <td class="bd-num">${fmt(x.post.total)}</td>
      <td class="bd-num">${fmt(x.combined)}</td>
      <td class="bd-num">${fmt(x.current)}</td>
      <td class="bd-num">${mark}</td>
      <td>${buffered}</td>
    </tr>`;
  }).join('');

  const verdict = res.matches
    ? '<span class="bd-ok">✓ 核对一致</span>'
    : `<span class="bd-bad">✗ 差异 ${fmt(res.diff)}</span>`;
  const recoveryNote = res.recoveredFrom
    ? `<p class="bd-note">↻ 本分界为故障重开（事件 #${res.reopenEventIndex}）恢复采用的完整发布检查点；重开前后相同序号只计一次，半完成快照不作为来源。</p>`
    : '';

  els.breakdown.className = '';
  els.breakdown.innerHTML =
    `<table class="bd-table">
      <thead><tr>
        <th>通道</th><th>封存序号范围</th><th class="bd-num">封存累计</th>
        <th>检查点后纳入序号</th><th>逐笔增量</th><th class="bd-num">增量小计</th>
        <th class="bd-num">两部分合计</th><th class="bd-num">当前累计</th><th class="bd-num">核对</th><th></th>
      </tr></thead>
      <tbody>${rows}</tbody>
    </table>
    <div class="bd-total">
      <span class="formula">封存 <b>${fmt(res.sealedTotal)}</b> ＋ 检查点后增量 <b>${fmt(res.postTotal)}</b> ＝ 合计 <b>${fmt(res.combinedTotal)}</b>；当前累计 <b>${fmt(res.currentTotal)}</b></span>
      ${verdict}
    </div>
    ${recoveryNote}`;
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
