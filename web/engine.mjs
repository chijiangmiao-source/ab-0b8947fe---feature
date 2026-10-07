// engine.mjs — 纯逻辑核心：事件校验、屏障对齐、检查点封存、崩溃恢复与逐帧状态
//
// 模型（对齐 Chandy–Lamport / Flink barrier alignment）：
//   DATA(c, seq, value)    通道 c 上序号 seq 的数据；seq 在该通道从 1 起连续递增
//   BARRIER(c, cp)         通道 c 收到检查点 cp 的屏障
//   CRASH                  故障：进程中断。可带 stage: 'intent' | 'snapshot'，
//                          表示故障注入到“下一个检查点写入到该持久化阶段”之后再中断；
//                          不带 stage 表示在本事件处立即中断。
//   REOPEN                 重开：丢弃所有未发布的半成品，从最新“完整发布”的检查点之后重放
//
// 对齐：某通道屏障先到后，该通道后续数据进入缓存；待全部通道到达同一屏障，
//       封存累计状态快照（含各通道已纳入输入序号），再按捕获原序释放缓存。
//
// Worker 持久化严格三阶段（帧上以 persistence[] 显式给出写操作，顺序即真实写入顺序）：
//   1. intent   每到达一条屏障写 intent:<cp>:<channel>
//   2. snapshot 全部对齐时写 snapshot:<cp>（累计值 + 各通道输入序号 + 纳入范围）
//   3. published 最后写 published:<cp> 发布标记
// 重开时：无 published 配对的 snapshot 与所有 intent 一律删除（帧 cleanup.deleted 给出清单）。

export const EVENT_TYPES = ['data', 'barrier', 'crash', 'reopen'];
export const STAGES = ['intent', 'snapshot', 'published'];
export const MIN_CHANNELS = 2;
export const MAX_CHANNELS = 4;
export const MAX_EVENTS = 48;

export class ValidationError extends Error {
  // index 为事件在整条事件流中的下标（0 起），即“首个出错事件”
  constructor(message, index, code) {
    super(message);
    this.name = 'ValidationError';
    this.index = index;
    this.code = code;
  }
}

/** 结构校验：通道数、事件数、字段合法性。返回标准化事件。 */
export function normalizePlan(plan) {
  const channels = Number(plan?.channels);
  if (!Number.isInteger(channels) || channels < MIN_CHANNELS || channels > MAX_CHANNELS) {
    throw new ValidationError(
      `输入通道数必须是 ${MIN_CHANNELS}–${MAX_CHANNELS} 的整数，收到 ${plan?.channels}`,
      -1,
      'BAD_CHANNELS'
    );
  }
  const raw = Array.isArray(plan?.events) ? plan.events : [];
  if (raw.length === 0) throw new ValidationError('事件序列不能为空', -1, 'EMPTY');
  if (raw.length > MAX_EVENTS) {
    throw new ValidationError(`事件数量不得超过 ${MAX_EVENTS} 项，收到 ${raw.length}`, -1, 'TOO_MANY_EVENTS');
  }

  const events = raw.map((e, index) => {
    if (!e || typeof e !== 'object') throw new ValidationError('事件必须是对象', index, 'BAD_EVENT');
    const type = String(e.type || '').toLowerCase();
    if (!EVENT_TYPES.includes(type)) throw new ValidationError(`未知事件类型 "${e.type}"`, index, 'BAD_TYPE');

    if (type === 'crash') {
      const stage = e.stage == null || e.stage === '' ? null : String(e.stage).toLowerCase();
      if (stage !== null && stage !== 'intent' && stage !== 'snapshot') {
        throw new ValidationError(
          `故障注入阶段只能是 intent 或 snapshot，收到 ${e.stage}`,
          index,
          'BAD_CRASH_STAGE'
        );
      }
      return { type, stage, index };
    }
    if (type === 'reopen') return { type, index };

    const channel = Number(e.channel);
    if (!Number.isInteger(channel) || channel < 1 || channel > channels) {
      throw new ValidationError(`事件通道必须是 1–${channels} 的整数，收到 ${e.channel}`, index, 'BAD_CHANNEL');
    }
    if (type === 'data') {
      const seq = Number(e.seq);
      const value = Number(e.value);
      if (!Number.isInteger(seq) || seq < 1) {
        throw new ValidationError(`数据序号必须是 ≥1 的整数，收到 ${e.seq}`, index, 'BAD_SEQ');
      }
      if (!Number.isFinite(value)) {
        throw new ValidationError(`数据值必须是数字，收到 ${e.value}`, index, 'BAD_VALUE');
      }
      return { type, channel, seq, value, index };
    }
    const checkpoint = Number(e.checkpoint);
    if (!Number.isInteger(checkpoint) || checkpoint < 1) {
      throw new ValidationError(`检查点编号必须是 ≥1 的整数，收到 ${e.checkpoint}`, index, 'BAD_CHECKPOINT');
    }
    return { type, channel, checkpoint, index };
  });

  return { channels, events };
}

function freshTotals(channels) {
  return { perChannel: Array.from({ length: channels + 1 }, () => 0), total: 0 };
}
function applyData(totals, channel, value) {
  totals.perChannel[channel] += value;
  totals.total += value;
}
function cloneTotals(t) {
  return { perChannel: t.perChannel.slice(), total: t.total };
}
function rangeList(a, b) {
  const out = [];
  for (let c = a; c <= b; c++) out.push(c);
  return out;
}

/**
 * 逐事件回放，返回有序帧。每帧描述执行该事件后的完整状态与持久化动作，
 * UI 可任选一帧查看，Worker 按帧中的 persistence 顺序写 IndexedDB。
 */
export function replay(plan) {
  const { channels, events } = normalizePlan(plan);

  const frames = [];
  let totals = freshTotals(channels);
  const expectedSeq = Array.from({ length: channels + 1 }, () => 1); // 每通道下一个应到序号
  const lastAppliedSeq = Array.from({ length: channels + 1 }, () => 0); // 已纳入累计值的最后序号
  const pending = new Map(); // cp -> { arrived:Set, blocked:Set }
  const blockedChannels = new Set();
  const buffer = []; // {event, order, checkpoint}
  const checkpoints = []; // 已发布快照
  // 每通道“已实际纳入累计值”的数据（应用序）：直接入账与封存后缓存释放都进入这里；
  // 仅在缓存中挂起、尚未释放的数据永不进入。重开后按检查点 inputSeq 重建为占位项（value=null）。
  const applied = Array.from({ length: channels + 1 }, () => []); // channel -> [{seq,value}]
  const writes = []; // 已执行持久化写操作 {key,stage,cp,channel?}，崩溃后保留
  const introducedCp = new Set(); // 历史出现过的检查点编号（编号只增不复用）
  let maxIntroduced = 0;
  let alive = true;
  let armedCrash = null; // null | 'intent' | 'snapshot'
  let phase = 'idle'; // idle | intent | snapshot | published | crashed
  let phaseDetail = null;
  let recoveryStart = null;

  const appliedView = () => applied.map((list) => list.map((x) => ({ ...x })));
  const bufferView = () =>
    buffer.map((b) => ({
      channel: b.event.channel,
      seq: b.event.seq,
      value: b.event.value,
      reason: `检查点 ${b.checkpoint} 的屏障已在通道 ${b.event.channel} 先到，等待全部通道对齐`,
    }));
  const alignmentView = () =>
    [...pending.entries()].map(([cp, s]) => ({
      checkpoint: cp,
      arrived: [...s.arrived].sort((a, b) => a - b),
      missing: rangeList(1, channels).filter((c) => !s.arrived.has(c)),
    }));
  // 入帧前统一刷新所有派生状态，保证逐帧查看时看到的是“执行完本事件后”的真实状态
  const commit = (frame) => {
    frame.total = totals.total;
    frame.perChannel = totals.perChannel.slice();
    frame.nextSeq = expectedSeq.slice();
    frame.buffered = bufferView();
    frame.alignment = alignmentView();
    frame.checkpoints = checkpoints.map(publicView);
    frame.appliedSnapshot = appliedView();
    frames.push(frame);
  };

  const die = (frame, reason) => {
    alive = false;
    armedCrash = null;
    phase = 'crashed';
    phaseDetail = null;
    frame.phase = 'crashed';
    frame.phaseDetail = null;
    frame.alive = false;
    pending.clear();
    blockedChannels.clear();
    buffer.length = 0;
    frame.alignment = [];
    frame.buffered = [];
    frame.crashed = true;
    frame.notes.push(reason);
  };

  for (let i = 0; i < events.length; i++) {
    const ev = events[i];
    const frame = {
      index: i,
      event: ev,
      alive,
      total: totals.total,
      perChannel: totals.perChannel.slice(),
      nextSeq: expectedSeq.slice(),
      buffered: buffer.map((b) => ({
        channel: b.event.channel,
        seq: b.event.seq,
        value: b.event.value,
        reason: `检查点 ${b.checkpoint} 的屏障已在通道 ${b.event.channel} 先到，等待全部通道对齐`,
      })),
      alignment: [...pending.entries()].map(([cp, s]) => ({
        checkpoint: cp,
        arrived: [...s.arrived].sort((a, b) => a - b),
        missing: rangeList(1, channels).filter((c) => !s.arrived.has(c)),
      })),
      checkpoints: checkpoints.map(publicView),
      phase,
      phaseDetail,
      recoveryStart,
      persistence: [],
      cleanup: null,
      crashed: false,
      reopened: false,
      skippedDead: false,
      notes: [],
    };

    // —— 进程已崩溃：数据/屏障被上游暂存，不做任何语义处理，直到 REOPEN ——
    if (!alive && (ev.type === 'data' || ev.type === 'barrier')) {
      frame.skippedDead = true;
      frame.notes.push('进程处于故障态：该输入被上游暂存，未纳入也不推进序号，等待重开后重放');
      commit(frame);
      continue;
    }

    if (ev.type === 'crash') {
      if (!alive) {
        throw new ValidationError('进程已处于故障态，不能重复注入故障（需先 REOPEN）', i, 'DOUBLE_CRASH');
      }
      if (ev.stage) {
        armedCrash = ev.stage;
        frame.notes.push(
          ev.stage === 'intent'
            ? '故障已注入：下一个检查点写入「意图」阶段后立即中断（不会产生快照）'
            : '故障已注入：下一个检查点写入「快照」阶段后、发布前立即中断（产生半成品快照）'
        );
      } else {
        die(frame, '进程故障（立即）：运行态冻结，未完成的检查点写入将在重开后废弃');
      }
      commit(frame);
      continue;
    }

    if (ev.type === 'reopen') {
      if (alive) {
        throw new ValidationError('REOPEN 前没有故障（CRASH）：重开事件只能跟随故障出现', i, 'REOPEN_WITHOUT_CRASH');
      }
      alive = true;
      armedCrash = null;
      // 清理存储层：无 published 配对的 snapshot、所有 intent
      const publishedCp = new Set(checkpoints.map((c) => c.id));
      const deleted = [];
      for (let k = writes.length - 1; k >= 0; k--) {
        const w = writes[k];
        if (w.stage === 'published') continue;
        if (w.stage === 'snapshot' && publishedCp.has(w.cp)) continue;
        deleted.push(w.key);
        writes.splice(k, 1);
      }
      frame.cleanup = { deleted };

      const latest = checkpoints.length ? checkpoints[checkpoints.length - 1] : null;
      if (latest) {
        totals = cloneTotals(latest.totals);
        for (let c = 1; c <= channels; c++) {
          lastAppliedSeq[c] = latest.inputSeq[c];
          expectedSeq[c] = latest.inputSeq[c] + 1;
          // 台账重建：检查点封存序号内的明细不可逐笔重放（值已由快照承载），
          // 以 value=null 的连续占位项标记“封存部分”；占位项永不参与任何增量求和。
          applied[c] = [];
          for (let s = 1; s <= latest.inputSeq[c]; s++) applied[c].push({ seq: s, value: null });
        }
        recoveryStart = {
          checkpoint: latest.id,
          afterEventIndex: latest.sealedAtEventIndex,
          inputSeq: latest.inputSeq.slice(),
          reason: `采用最新完整发布的检查点 ${latest.id}（封存于事件 #${latest.sealedAtEventIndex}），各通道从输入序号 ${JSON.stringify(
            latest.inputSeq.slice(1)
          )} 之后重放`,
        };
      } else {
        totals = freshTotals(channels);
        for (let c = 1; c <= channels; c++) {
          lastAppliedSeq[c] = 0;
          expectedSeq[c] = 1;
          applied[c] = [];
        }
        recoveryStart = {
          checkpoint: null,
          afterEventIndex: -1,
          inputSeq: Array.from({ length: channels + 1 }, () => 0),
          reason: '无完整发布的检查点：从空状态（各通道输入序号 0）开始重放',
        };
      }
      pending.clear();
      blockedChannels.clear();
      buffer.length = 0;
      phase = 'idle';
      phaseDetail = null;
      frame.alive = true;
      frame.phase = 'idle';
      frame.phaseDetail = null;
      frame.alignment = [];
      frame.buffered = [];
      frame.reopened = true;
      frame.recoveryStart = recoveryStart;
      frame.notes.push('REOPEN：' + (deleted.length ? `废弃 ${deleted.length} 个未完成写入（${deleted.join('、')}）；` : '') + recoveryStart.reason);
      commit(frame);
      continue;
    }

    if (ev.type === 'data') {
      if (ev.seq !== expectedSeq[ev.channel]) {
        throw new ValidationError(
          ev.seq < expectedSeq[ev.channel]
            ? `通道 ${ev.channel} 数据序号 ${ev.seq} 失配（重复/乱序）：下一个应为 ${expectedSeq[ev.channel]}，重复计入风险`
            : `通道 ${ev.channel} 数据序号 ${ev.seq} 失配（跳号）：下一个应为 ${expectedSeq[ev.channel]}`,
          i,
          ev.seq < expectedSeq[ev.channel] ? 'SEQ_DUPLICATE' : 'SEQ_GAP'
        );
      }
      expectedSeq[ev.channel] = ev.seq + 1;

      if (blockedChannels.has(ev.channel)) {
        let cp = null;
        for (const [id, st] of pending) if (st.blocked.has(ev.channel)) cp = id;
        buffer.push({ event: ev, order: i, checkpoint: cp });
        frame.notes.push(
          `缓存：通道 ${ev.channel} 检查点 ${cp} 的屏障已先到，数据 ${ev.channel}#${ev.seq}=${ev.value} 挂起，待全部通道对齐后按原序释放`
        );
      } else {
        applyData(totals, ev.channel, ev.value);
        lastAppliedSeq[ev.channel] = ev.seq;
        applied[ev.channel].push({ seq: ev.seq, value: ev.value });
      }
      commit(frame);
      continue;
    }

    // barrier
    const cp = ev.checkpoint;
    if (!introducedCp.has(cp)) {
      if (cp !== maxIntroduced + 1) {
        throw new ValidationError(
          `检查点编号 ${cp} 非法：编号须从 1 起严格递增（崩溃后也不复用），下一个应为 ${maxIntroduced + 1}`,
          i,
          'CHECKPOINT_NOT_SEQUENTIAL'
        );
      }
      introducedCp.add(cp);
      maxIntroduced = cp;
    } else if (checkpoints.some((c) => c.id === cp)) {
      throw new ValidationError(`检查点 ${cp} 已完整发布，其屏障不得再次出现（重复屏障）`, i, 'DUPLICATE_BARRIER');
    }

    if (blockedChannels.has(ev.channel)) {
      let existing = null;
      for (const [id, st] of pending) if (st.blocked.has(ev.channel)) existing = id;
      if (existing === cp) {
        throw new ValidationError(`通道 ${ev.channel} 重复收到检查点 ${cp} 的屏障（重复屏障）`, i, 'DUPLICATE_BARRIER');
      }
      throw new ValidationError(
        `通道 ${ev.channel} 尚未对齐检查点 ${existing}，不能接收检查点 ${cp} 的屏障（错误释放/交叉对齐）`,
        i,
        'BARRIER_OVERLAP'
      );
    }

    // 阶段 1/3：意图
    if (!pending.has(cp)) pending.set(cp, { arrived: new Set(), blocked: new Set() });
    const st = pending.get(cp);
    st.arrived.add(ev.channel);
    st.blocked.add(ev.channel);
    blockedChannels.add(ev.channel);
    const intentKey = `intent:cp${cp}:ch${ev.channel}`;
    writes.push({ key: intentKey, stage: 'intent', cp, channel: ev.channel });
    frame.persistence.push({ op: 'put', key: intentKey, stage: 'intent', checkpoint: cp, channel: ev.channel });
    phase = 'intent';
    phaseDetail = { checkpoint: cp, stage: 'intent' };
    frame.notes.push(`持久化 1/3：写入检查点 ${cp} 的对齐意图（通道 ${ev.channel} 屏障到达），该通道进入缓存态`);

    if (armedCrash === 'intent') {
      die(frame, `按注入在检查点 ${cp} 的「意图」写入后中断：尚无快照，重开后该意图作废`);
      commit(frame);
      continue;
    }

    if (st.arrived.size === channels) {
      // 阶段 2/3：快照（累计值 + 输入序号 + 纳入范围）
      const inputSeq = lastAppliedSeq.slice();
      const prev = checkpoints.length ? checkpoints[checkpoints.length - 1] : null;
      const included = [];
      for (let c = 1; c <= channels; c++) {
        const base = prev ? prev.inputSeq[c] : 0;
        included.push({
          channel: c,
          firstSeq: inputSeq[c] > base ? base + 1 : null,
          lastSeq: inputSeq[c] > base ? inputSeq[c] : null,
        });
      }
      const sealed = {
        id: cp,
        sealedAtEventIndex: i,
        totals: cloneTotals(totals),
        inputSeq,
        included,
        released: [],
      };
      const snapKey = `snapshot:cp${cp}`;
      writes.push({ key: snapKey, stage: 'snapshot', cp });
      frame.persistence.push({
        op: 'put',
        key: snapKey,
        stage: 'snapshot',
        checkpoint: cp,
        snapshot: publicView(sealed),
      });
      phase = 'snapshot';
      phaseDetail = { checkpoint: cp, stage: 'snapshot' };
      frame.notes.push(
        `持久化 2/3：全部 ${channels} 条通道到达检查点 ${cp} 屏障，封存快照（各通道已纳入序号 ${JSON.stringify(
          inputSeq.slice(1)
        )}，累计 ${totals.total}）`
      );

      if (armedCrash === 'snapshot') {
        // 故意不写 published：存储中留下半成品快照
        die(frame, `按注入在检查点 ${cp} 的「快照」写入后、发布前中断：快照未发布，重开后必须删除且不得显示`);
        commit(frame);
        continue;
      }

      // 阶段 3/3：发布
      checkpoints.push(sealed);
      pending.delete(cp);
      for (const c of st.blocked) blockedChannels.delete(c);
      const pubKey = `published:cp${cp}`;
      writes.push({ key: pubKey, stage: 'published', cp });
      frame.persistence.push({ op: 'put', key: pubKey, stage: 'published', checkpoint: cp });
      phase = 'published';
      phaseDetail = { checkpoint: cp, stage: 'published' };
      frame.notes.push(`持久化 3/3：写入发布标记，检查点 ${cp} 生效；随后按捕获原序释放缓存`);

      buffer.sort((a, b) => a.order - b.order);
      while (buffer.length) {
        const b = buffer.shift();
        applyData(totals, b.event.channel, b.event.value);
        lastAppliedSeq[b.event.channel] = b.event.seq;
        applied[b.event.channel].push({ seq: b.event.seq, value: b.event.value });
        sealed.released.push({ channel: b.event.channel, seq: b.event.seq, value: b.event.value });
      }
      if (sealed.released.length) {
        frame.notes.push(
          `缓存按原序释放并入账：${sealed.released.map((r) => `${r.channel}#${r.seq}=${r.value}`).join('，')}`
        );
      }
    }

    commit(frame);
  }

  return {
    channels,
    frames,
    checkpoints: checkpoints.map(publicView),
    storage: storageView(writes, checkpoints),
  };
}

function publicView(s) {
  return {
    id: s.id,
    sealedAtEventIndex: s.sealedAtEventIndex,
    total: s.totals.total,
    perChannel: s.totals.perChannel.slice(),
    inputSeq: s.inputSeq.slice(),
    included: s.included.map((x) => ({ ...x })),
    released: (s.released || []).map((r) => ({ ...r })),
  };
}

/** 逻辑层视角的持久化层现状（与 Worker 的 IndexedDB 内容一致），含半成品标记。 */
function storageView(writes, checkpoints) {
  const published = new Set(checkpoints.map((c) => c.id));
  const byKey = new Map();
  for (const w of writes) {
    byKey.set(w.key, {
      key: w.key,
      stage: w.stage,
      checkpoint: w.cp,
      complete: w.stage === 'published' || (w.stage === 'snapshot' && published.has(w.cp)),
    });
  }
  return [...byKey.values()].sort((a, b) => a.key.localeCompare(b.key, undefined, { numeric: true }));
}

/** 跑到终态；校验失败抛 ValidationError（含首个出错事件下标）。 */
export function finalState(plan) {
  const r = replay(plan);
  const last = r.frames[r.frames.length - 1];
  return { channels: r.channels, total: last.total, perChannel: last.perChannel, checkpoints: r.checkpoints };
}

/** 来源分解失败原因码（UI 必须展示原因且不得残留旧分解）。 */
export const BREAKDOWN_REASONS = {
  BAD_STEP: '所选步骤不存在',
  BAD_CHECKPOINT: '未指定检查点',
  STEP_FROZEN: '所选步骤处于故障冻结态（首个错误处之后、重开之前）',
  CHECKPOINT_INCOMPLETE: '该检查点在所选步骤时尚未完整发布（可能只是半成品快照）',
  STEP_BEFORE_CHECKPOINT: '所选步骤早于该检查点的封存步骤',
  CHECKPOINT_NOT_RECOVERY: '故障重开后只能针对恢复采用的完整检查点生成分解',
  NO_PUBLISHED_CHECKPOINT: '本次恢复未采用任何完整发布的检查点（从空状态重放），无封存部分可分解',
  POST_HELD_BY_LATER_SNAPSHOT: '分界之后的数据由更晚发布的检查点承载，无法逐笔重建增量（请选择当前运行段恢复采用的检查点）',
  APPLIED_UNAVAILABLE: '缺少各通道已应用序号台账，无法重建分界',
};

function breakdownError(code, detail) {
  return { ok: false, code, reason: BREAKDOWN_REASONS[code] || code, detail: detail || '' };
}

function toRanges(seqs) {
  const sorted = seqs.slice().sort((a, b) => a - b);
  const ranges = [];
  for (const s of sorted) {
    const last = ranges[ranges.length - 1];
    if (last && last.last === s - 1) last.last = s;
    else ranges.push({ first: s, last: s });
  }
  return ranges;
}

/**
 * 来源分解：在回放结果上，为“检查点 checkpointId + 不早于其封存步骤的 stepIndex”
 * 重建当前累计值的两部分来源：
 *   A. 封存部分：该检查点快照封存的每通道连续序号 1..inputSeq[c] 及其累计值；
 *   B. 检查点后实际纳入：依据每通道“已应用连续序号台账”，序号大于 inputSeq[c] 且
 *      值真实（非快照占位）的逐笔数据，按实际纳入顺序给出序号与增量之和。
 * 仍在缓存中未释放的数据既不在台账中，也不出现在任一部分。
 * 故障重开后仅允许分解恢复采用的那个完整检查点；半成品快照永远不在 frame.checkpoints 中。
 *
 * 成功返回 { ok:true, stepIndex, checkpoint, recoveredFrom, channels[], sealedTotal,
 *   postTotal, combinedTotal, currentTotal, matches, diff }；
 * 失败返回 { ok:false, code, reason, detail }，调用方必须只展示原因。
 */
export function sourceBreakdown(replayResult, checkpointId, stepIndex) {
  const channels = replayResult?.channels;
  const frames = Array.isArray(replayResult?.frames) ? replayResult.frames : null;
  if (!Number.isInteger(channels) || !frames || !frames.length) {
    return breakdownError('BAD_STEP', '缺少有效的回放结果');
  }

  const step = Number(stepIndex);
  if (!Number.isInteger(step) || step < 0 || step >= frames.length) {
    return breakdownError('BAD_STEP', `步骤序号须为 0–${frames.length - 1} 的整数，收到 ${stepIndex}`);
  }
  const cpId = Number(checkpointId);
  if (!Number.isInteger(cpId) || cpId < 1) {
    return breakdownError('BAD_CHECKPOINT', `检查点编号须为 ≥1 的整数，收到 ${checkpointId}`);
  }

  const frame = frames[step];

  // 故障冻结帧（崩溃帧本身，或首个错误处之后被上游暂存、重开之前的帧）不展示任何旧分解
  if (frame.crashed || frame.alive === false) {
    return breakdownError('STEP_FROZEN', `步骤 #${step} 已在故障处冻结`);
  }

  // frame.checkpoints 只包含完整发布的检查点；半成品快照永远不在其中
  const snap = frame.checkpoints.find((c) => c.id === cpId);
  if (!snap) {
    return breakdownError(
      'CHECKPOINT_INCOMPLETE',
      `步骤 #${step} 的检查点 ${cpId} 无发布标记或尚未封存，半完成快照及其数据不得成为来源`
    );
  }
  if (frame.index < snap.sealedAtEventIndex) {
    return breakdownError(
      'STEP_BEFORE_CHECKPOINT',
      `检查点 ${cpId} 封存于步骤 #${snap.sealedAtEventIndex}，所选步骤 #${step} 早于它`
    );
  }

  // 故障/重开边界：扫描所选步骤之前的最后一次故障与其后的重开。
  // 仅“快照阶段故障”会留下半完成快照：该运行段内只允许分解恢复实际采用的完整检查点；
  // 意图阶段/立即故障不产生任何快照，重开（含从空状态）后新发布的检查点不受此限。
  let lastCrash = -1;
  let lastCrashStage = null;
  let lastReopen = -1;
  for (let i = 0; i <= step; i++) {
    if (frames[i].event?.type === 'crash') {
      lastCrash = i;
      lastCrashStage = frames[i].event.stage || null;
    }
    if (frames[i].reopened) lastReopen = i;
  }
  let recoveredFrom = false;
  if (lastCrash >= 0) {
    if (lastReopen <= lastCrash) {
      return breakdownError('STEP_FROZEN', `步骤 #${step} 位于故障（#${lastCrash}）之后、重开之前`);
    }
    const recoveryCp = frames[lastReopen].recoveryStart?.checkpoint ?? null;
    recoveredFrom = recoveryCp === cpId;
    if (lastCrashStage === 'snapshot') {
      // 存在半完成快照风险：只能以恢复采用的那个完整检查点为分界
      if (recoveryCp == null) {
        return breakdownError(
          'NO_PUBLISHED_CHECKPOINT',
          `步骤 #${step} 所在运行段是快照阶段故障后从空状态恢复的（重开于 #${lastReopen}），无封存部分可分解`
        );
      }
      if (recoveryCp !== cpId) {
        return breakdownError(
          'CHECKPOINT_NOT_RECOVERY',
          `快照阶段故障后的重开（#${lastReopen}）采用的是完整发布检查点 ${recoveryCp}，半完成快照及其数据不得成为来源，不能以检查点 ${cpId} 作为分界`
        );
      }
    }
  }

  const appliedSnapshot = frame.appliedSnapshot;
  if (!Array.isArray(appliedSnapshot)) {
    return breakdownError('APPLIED_UNAVAILABLE', `步骤 #${step} 缺少每通道已应用序号台账`);
  }

  const perChannel = [];
  let sealedTotal = 0;
  let postTotal = 0;
  let bufferedTotal = 0;
  for (let c = 1; c <= channels; c++) {
    const boundary = snap.inputSeq[c] | 0;
    const ledger = Array.isArray(appliedSnapshot[c]) ? appliedSnapshot[c] : null;
    if (!ledger) return breakdownError('APPLIED_UNAVAILABLE', `通道 ${c} 缺少已应用序号台账`);

    // B 部分：台账中序号越过封存边界、且值真实（非快照占位）的逐笔数据
    const postItems = [];
    const seen = new Set();
    for (const item of ledger) {
      if (!item || item.seq <= boundary) continue; // 封存序号（重开后为占位项）
      if (item.value === null || item.value === undefined) {
        // 分界之后的序号却是快照占位：该段数据由更晚的检查点承载，无法逐笔重建增量
        return breakdownError(
          'POST_HELD_BY_LATER_SNAPSHOT',
          `通道 ${c} 的序号 #${item.seq} 已由晚于检查点 ${cpId} 的快照承载（多次崩溃重开链），请选择当前运行段恢复采用的检查点`
        );
      }
      if (seen.has(item.seq)) continue; // 重开前后同一序号只承认一次
      seen.add(item.seq);
      postItems.push({ seq: item.seq, value: item.value });
    }
    const postValue = postItems.reduce((s, x) => s + x.value, 0);
    const sealedValue = Number(snap.perChannel[c]) || 0;
    const combined = sealedValue + postValue;

    const buffered = (frame.buffered || [])
      .filter((b) => b.channel === c)
      .map((b) => b.seq)
      .sort((a, b) => a - b);
    bufferedTotal += buffered.length;

    perChannel.push({
      channel: c,
      boundary,
      sealed: {
        range: boundary > 0 ? { first: 1, last: boundary } : null,
        total: sealedValue,
      },
      post: {
        seqOrder: postItems.map((x) => x.seq), // 实际纳入顺序
        ranges: toRanges(postItems.map((x) => x.seq)),
        items: postItems.map((x) => ({ ...x })),
        total: postValue,
      },
      bufferedSeqs: buffered, // 缓存中尚未释放：两部分都不含
      combined,
      current: Number(frame.perChannel[c]) || 0,
      matches: closeEnough(combined, Number(frame.perChannel[c]) || 0),
    });
    sealedTotal += sealedValue;
    postTotal += postValue;
  }

  const combinedTotal = sealedTotal + postTotal;
  const currentTotal = Number(frame.total) || 0;
  return {
    ok: true,
    stepIndex: step,
    checkpoint: {
      id: snap.id,
      sealedAtEventIndex: snap.sealedAtEventIndex,
    },
    recoveredFrom,
    reopenEventIndex: recoveredFrom ? lastReopen : null,
    channels: perChannel,
    sealedTotal,
    postTotal,
    combinedTotal,
    currentTotal,
    bufferedCount: bufferedTotal,
    matches: closeEnough(combinedTotal, currentTotal),
    diff: roundNum(combinedTotal - currentTotal),
  };
}

function roundNum(x) {
  return Number.isInteger(x) ? x : Number(x.toFixed(9));
}
function closeEnough(a, b) {
  return Math.abs(a - b) < 1e-9;
}
