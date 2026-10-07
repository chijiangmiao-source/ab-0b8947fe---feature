// test/engine.test.mjs — 屏障、缓存、恢复与错误边界
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { replay, finalState, breakdownSources, ValidationError, BreakdownError, MAX_CHANNELS, MAX_EVENTS } from '../src/engine.mjs';

const D = (channel, seq, value) => ({ type: 'data', channel, seq, value });
const B = (channel, checkpoint) => ({ type: 'barrier', channel, checkpoint });
const C = (stage = undefined) => (stage === undefined ? { type: 'crash' } : { type: 'crash', stage });
const R = () => ({ type: 'reopen' });

function expectError(plan, code) {
  try {
    replay(plan);
    assert.fail('应当抛出 ValidationError');
  } catch (e) {
    assert.ok(e instanceof ValidationError, '应为 ValidationError');
    if (code) assert.equal(e.code, code);
    return e;
  }
}

test('基本对齐：全部通道屏障到达才封存，快照含累计值与各通道输入序号', () => {
  const plan = {
    channels: 2,
    events: [D(1, 1, 10), D(2, 1, 1), B(1, 1), B(2, 1), D(1, 2, 5)],
  };
  const r = replay(plan);
  assert.equal(r.checkpoints.length, 1);
  const cp = r.checkpoints[0];
  assert.equal(cp.total, 11, '封存时缓存外累计 10+1');
  assert.deepEqual(cp.inputSeq, [0, 1, 1]);
  assert.deepEqual(cp.included, [
    { channel: 1, firstSeq: 1, lastSeq: 1 },
    { channel: 2, firstSeq: 1, lastSeq: 1 },
  ]);
  assert.equal(r.frames.at(-1).total, 16, '封存后数据继续入账');
});

test('屏障先到则缓存，全部对齐后按捕获原序释放', () => {
  const plan = {
    channels: 2,
    events: [
      D(1, 1, 10),
      B(1, 1),
      D(1, 2, 7), // 缓存
      D(2, 1, 3), // 通道 2 未阻塞，正常入账
      D(1, 3, 4), // 缓存
      B(2, 1), // 对齐：快照不含缓存，随后释放 7 再 4
    ],
  };
  const r = replay(plan);
  const cp = r.checkpoints[0];
  assert.equal(cp.total, 13, '快照只含屏障前已纳入数据 10+3');
  assert.deepEqual(cp.inputSeq, [0, 1, 1]);
  assert.deepEqual(cp.released, [
    { channel: 1, seq: 2, value: 7 },
    { channel: 1, seq: 3, value: 4 },
  ]);
  assert.equal(r.frames.at(-1).total, 24);

  // 缓存期间帧中应能看到缓存项及原因
  const bufferedFrame = r.frames.find((f) => f.buffered.length === 2);
  assert.ok(bufferedFrame);
  assert.match(bufferedFrame.buffered[0].reason, /屏障已在通道 1 先到/);
});

test('持久化三阶段严格有序：intent -> snapshot -> published，且每 cp 仅一份发布', () => {
  const plan = { channels: 2, events: [B(1, 1), D(2, 1, 2), B(2, 1)] };
  const r = replay(plan);
  const sealFrame = r.frames.find((f) => f.persistence.some((w) => w.stage === 'published'));
  const stages = sealFrame.persistence.map((w) => w.stage);
  // 封存帧：本通道 intent 先落，随后 snapshot，最后 published
  assert.deepEqual(stages, ['intent', 'snapshot', 'published']);
  // 更早的帧只写 intent
  assert.deepEqual(r.frames[0].persistence.map((w) => w.stage), ['intent']);
  // 终态存储：intent×2、snapshot×1、published×1，且快照完整
  const keys = r.storage.map((s) => s.key);
  assert.deepEqual(keys, ['intent:cp1:ch1', 'intent:cp1:ch2', 'published:cp1', 'snapshot:cp1']);
  assert.ok(r.storage.find((s) => s.key === 'snapshot:cp1').complete);
});

test('快照后故障：留下半成品快照，重开只采用已发布检查点且不重复计入', () => {
  const plan = {
    channels: 2,
    events: [
      D(1, 1, 10),
      D(2, 1, 5),
      B(1, 1),
      B(2, 1), // cp1 发布，累计 15
      D(1, 2, 9),
      C('snapshot'), // 注入：下一检查点快照后中断
      B(2, 2),
      B(1, 2), // 写 snapshot:cp2 但无 published -> 中断
      R(), // 重开：废弃 cp2 半成品，从 cp1 之后重放
      D(1, 2, 9), // 重放（序号从 cp1 输入序号之后续上，不重复）
      B(2, 2),
      B(1, 2),
      D(2, 2, 6),
    ],
  };
  const r = replay(plan);

  // 故障帧：快照存在但未发布
  const crashFrame = r.frames.find((f) => f.crashed);
  assert.ok(crashFrame);
  assert.ok(crashFrame.notes.some((n) => n.includes('快照') && n.includes('中断')));

  // 重开帧的清理清单必须包含半成品 snapshot 与两条 intent
  const reopenFrame = r.frames.find((f) => f.reopened);
  assert.ok(reopenFrame.cleanup.deleted.includes('snapshot:cp2'));
  assert.ok(reopenFrame.cleanup.deleted.some((k) => k.startsWith('intent:cp2')));
  assert.ok(!reopenFrame.cleanup.deleted.includes('published:cp1'), '已发布标记不得删除');

  // 恢复起点
  assert.equal(reopenFrame.recoveryStart.checkpoint, 1);
  assert.deepEqual(reopenFrame.recoveryStart.inputSeq, [0, 1, 1]);

  // 终态：cp1 与重新发布的 cp2；9 只计入一次，总值 30
  assert.deepEqual(r.checkpoints.map((c) => c.id), [1, 2]);
  assert.equal(r.frames.at(-1).total, 30);
  // 任何一帧展示的存储中都不得出现“未发布且完整”的快照
  for (const f of r.frames) {
    const bad = (f.storageAfter || []).filter((s) => s.stage === 'snapshot' && s.complete === false && !f.crashed);
    // 重开之后半成品必须消失
    if (f.index > reopenFrame.index) assert.equal(bad.length, 0, `帧 ${f.index} 不应残留半成品快照`);
  }
});

test('意图后故障：无快照产生，重开从空状态开始，intent 被清理', () => {
  const plan = {
    channels: 2,
    events: [D(1, 1, 4), C('intent'), B(1, 1), R(), D(1, 1, 4), D(2, 1, 2), B(1, 1), B(2, 1)],
  };
  const r = replay(plan);
  const crashFrame = r.frames.find((f) => f.crashed);
  assert.ok(crashFrame.persistence.some((w) => w.stage === 'intent'));
  assert.ok(!crashFrame.persistence.some((w) => w.stage === 'snapshot'));

  const reopenFrame = r.frames.find((f) => f.reopened);
  assert.equal(reopenFrame.recoveryStart.checkpoint, null);
  assert.deepEqual(reopenFrame.recoveryStart.inputSeq, [0, 0, 0]);
  assert.ok(reopenFrame.cleanup.deleted.includes('intent:cp1:ch1'));
  assert.equal(reopenFrame.total, 0);
  assert.equal(r.frames.at(-1).total, 6);
});

test('故障后到重开前的输入被暂存，不推进序号；重开后同序号重放不报错', () => {
  const plan = {
    channels: 2,
    events: [B(1, 1), B(2, 1), C(), D(1, 1, 3), R(), D(1, 1, 3)],
  };
  const r = replay(plan);
  const skipped = r.frames.find((f) => f.skippedDead);
  assert.ok(skipped);
  assert.equal(skipped.total, 0, '故障态数据不得入账');
  assert.equal(r.frames.at(-1).total, 3);
});

test('跳号定位首个事件', () => {
  const e = expectError({ channels: 2, events: [D(1, 1, 3), D(1, 3, 3)] }, 'SEQ_GAP');
  assert.equal(e.index, 1);
});

test('重复序号（重复计入风险）定位首个事件', () => {
  const e = expectError({ channels: 2, events: [D(1, 1, 3), D(1, 1, 3)] }, 'SEQ_DUPLICATE');
  assert.equal(e.index, 1);
});

test('同通道重复屏障定位首个事件', () => {
  const e = expectError(
    { channels: 2, events: [B(1, 1), B(1, 1), B(2, 1)] },
    'DUPLICATE_BARRIER'
  );
  assert.equal(e.index, 1);
});

test('已发布检查点的屏障再次出现即重复屏障', () => {
  const e = expectError(
    { channels: 2, events: [B(1, 1), B(2, 1), B(1, 1)] },
    'DUPLICATE_BARRIER'
  );
  assert.equal(e.index, 2);
});

test('交叉对齐：未对齐的通道收到下一检查点屏障，定位该事件（错误释放边界）', () => {
  const e = expectError({ channels: 2, events: [B(1, 1), B(1, 2)] }, 'BARRIER_OVERLAP');
  assert.equal(e.index, 1);
});

test('检查点编号必须从 1 起严格递增', () => {
  const e = expectError({ channels: 2, events: [B(1, 2)] }, 'CHECKPOINT_NOT_SEQUENTIAL');
  assert.equal(e.index, 0);
});

test('REOPEN 不能脱离 CRASH 出现', () => {
  const e = expectError({ channels: 2, events: [D(1, 1, 1), R()] }, 'REOPEN_WITHOUT_CRASH');
  assert.equal(e.index, 1);
});

test('不能在未重开时重复故障', () => {
  const e = expectError({ channels: 2, events: [C(), C()] }, 'DOUBLE_CRASH');
  assert.equal(e.index, 1);
});

test('结构边界：通道数 2–4、事件 ≤48、通道越界、坏字段', () => {
  expectError({ channels: 1, events: [D(1, 1, 1)] }, 'BAD_CHANNELS');
  expectError({ channels: 5, events: [D(1, 1, 1)] }, 'BAD_CHANNELS');
  expectError({ channels: 2, events: [] }, 'EMPTY');
  expectError(
    { channels: 2, events: Array.from({ length: MAX_EVENTS + 1 }, () => D(1, 1, 1)).map((e, i) => ({ ...e, seq: i + 1 })) },
    'TOO_MANY_EVENTS'
  );
  expectError({ channels: 2, events: [{ type: 'data', channel: 3, seq: 1, value: 1 }] }, 'BAD_CHANNEL');
  expectError({ channels: 2, events: [{ type: 'data', channel: 1, seq: 0, value: 1 }] }, 'BAD_SEQ');
  expectError({ channels: 2, events: [{ type: 'data', channel: 1, seq: 1, value: 'x' }] }, 'BAD_VALUE');
  expectError({ channels: 2, events: [{ type: 'barrier', channel: 1, checkpoint: 0 }] }, 'BAD_CHECKPOINT');
  expectError({ channels: 2, events: [{ type: 'bogus' }] }, 'BAD_TYPE');
  assert.ok(MAX_CHANNELS === 4);
});

test('三通道对齐与第二个检查点的相对纳入范围', () => {
  const plan = {
    channels: 3,
    events: [
      D(1, 1, 1), D(2, 1, 2), D(3, 1, 4),
      B(1, 1), B(2, 1), B(3, 1),
      D(1, 2, 10), D(2, 2, 20),
      B(2, 2),
      D(2, 3, 30), // 通道 2 屏障已到 -> 缓存
      B(1, 2), B(3, 2),
    ],
  };
  const r = replay(plan);
  const cp2 = r.checkpoints.find((c) => c.id === 2);
  assert.deepEqual(cp2.inputSeq, [0, 2, 2, 1]);
  assert.deepEqual(cp2.included, [
    { channel: 1, firstSeq: 2, lastSeq: 2 },
    { channel: 2, firstSeq: 2, lastSeq: 2 },
    { channel: 3, firstSeq: null, lastSeq: null },
  ]);
  assert.deepEqual(cp2.released, [{ channel: 2, seq: 3, value: 30 }]);
  assert.equal(cp2.total, 37, 'cp2 快照 = 1+2+4+10+20（30 在缓存中，快照后释放）');
  assert.equal(r.frames.at(-1).total, 67);
});

test('逐帧一致性：帧上状态即为执行完该事件后的状态（不滞后一帧）', () => {
  const plan = {
    channels: 2,
    events: [D(1, 1, 10), B(1, 1), D(1, 2, 7), D(2, 1, 3), B(2, 1)],
  };
  const r = replay(plan);
  // 帧 0：数据入账即刻可见
  assert.equal(r.frames[0].total, 10);
  assert.equal(r.frames[0].perChannel[1], 10);
  // 帧 1：屏障到达即刻进入对齐
  assert.deepEqual(r.frames[1].alignment.map((a) => [a.checkpoint, a.arrived, a.missing]), [[1, [1], [2]]]);
  // 帧 2：缓存数据在同一帧即出现在缓存区，且不计入累计值
  assert.equal(r.frames[2].buffered.length, 1);
  assert.equal(r.frames[2].buffered[0].seq, 2);
  assert.equal(r.frames[2].total, 10);
  // 帧 4：对齐帧封存并在同帧释放缓存（7）并入账通道 2 屏障前数据（3）
  const seal = r.frames[4];
  assert.equal(seal.checkpoints[0].id, 1);
  assert.equal(seal.total, 20);
  assert.deepEqual(seal.checkpoints[0].released, [{ channel: 1, seq: 2, value: 7 }]);
  assert.deepEqual(seal.buffered, []);
  assert.deepEqual(seal.alignment, []);
});

test('Worker 同构持久化回放（内存存储）：崩溃后存储中确无 published:cp2，重开后半成品被删', async () => {
  // 用内存 KV 复刻 worker.mjs 的写入循环，验证存储层不变量
  const mem = new Map();
  const store = {
    put: async (k, v) => void mem.set(k, v),
    del: async (k) => void mem.delete(k),
  };
  const plan = {
    channels: 2,
    events: [D(1, 1, 10), D(2, 1, 5), B(1, 1), B(2, 1), C('snapshot'), B(1, 2), B(2, 2), R(), D(1, 2, 9)],
  };
  const r = replay(plan);
  for (const f of r.frames) {
    for (const w of f.persistence) {
      if (w.stage === 'intent') await store.put(w.key, { stage: 'intent' });
      else if (w.stage === 'snapshot') await store.put(w.key, { stage: 'snapshot', snapshot: w.snapshot });
      else await store.put(w.key, { stage: 'published' });
    }
    if (f.cleanup) for (const k of f.cleanup.deleted) await store.del(k);
  }
  assert.ok(mem.has('published:cp1'));
  assert.ok(mem.has('snapshot:cp1'));
  assert.ok(!mem.has('published:cp2'), '故障检查点绝不允许出现发布标记');
  assert.ok(!mem.has('snapshot:cp2'), '重开后半成品快照必须删除');
  assert.ok(!mem.has('intent:cp2:ch1'));
  // finalState 便捷 API
  assert.equal(finalState({ channels: 2, events: [D(1, 1, 10), D(2, 1, 5)] }).total, 15);
});

// ============ 来源分解 breakdownSources ============
function expectBreakdown(r, cp, idx, code) {
  try {
    breakdownSources(r, cp, idx);
    assert.fail('应当抛出 BreakdownError');
  } catch (e) {
    assert.ok(e instanceof BreakdownError, '应为 BreakdownError，实际 ' + e.name);
    if (code) assert.equal(e.code, code);
    return e;
  }
}

test('来源分解：基本两检查点分段，两部分合计与当前累计值核对一致', () => {
  const plan = {
    channels: 2,
    events: [D(1, 1, 10), D(2, 1, 1), B(1, 1), B(2, 1), D(1, 2, 5), D(2, 2, 2)],
  };
  const r = replay(plan);
  const b = breakdownSources(r, 1, 5);
  assert.equal(b.checkpoint, 1);
  assert.equal(b.sealedAtEventIndex, 3);
  // 通道1：封存 #1=10，其后 #2=5
  assert.deepEqual(b.channels[0].sealed.seqs, [1]);
  assert.equal(b.channels[0].sealed.total, 10);
  assert.deepEqual(b.channels[0].after.seqs, [2]);
  assert.equal(b.channels[0].after.total, 5);
  // 通道2：封存 #1=1，其后 #2=2
  assert.deepEqual(b.channels[1].sealed.seqs, [1]);
  assert.equal(b.channels[1].sealed.total, 1);
  assert.deepEqual(b.channels[1].after.seqs, [2]);
  assert.equal(b.channels[1].after.total, 2);
  assert.equal(b.sealedGrand, 11);
  assert.equal(b.afterGrand, 7);
  assert.equal(b.combinedGrand, 18);
  assert.equal(b.currentGrand, 18);
  assert.equal(b.matched, true);
  assert.equal(b.difference, 0);
  assert.deepEqual(b.bufferedExcluded, []);
});

test('来源分解：封存同帧按原序释放的缓存归入"检查点后"，仍在缓存中的不出现', () => {
  const plan = {
    channels: 2,
    events: [D(1, 1, 10), B(1, 1), D(1, 2, 7), D(2, 1, 3), D(1, 3, 4), B(2, 1), D(2, 2, 2)],
  };
  const r = replay(plan);
  // 缓存尚未对齐帧（帧 2/3/4）：cp1 尚未发布，不能分解
  expectBreakdown(r, 1, 2, 'STEP_BEFORE_CHECKPOINT');
  expectBreakdown(r, 1, 4, 'STEP_BEFORE_CHECKPOINT');
  // 对齐发布帧（帧5）：同帧释放 7、4，归入"检查点后"
  let b = breakdownSources(r, 1, 5);
  assert.deepEqual(b.channels[0].after.seqs, [2, 3]);
  assert.equal(b.channels[0].after.total, 11);
  assert.deepEqual(b.channels[0].after.items, [{ seq: 2, value: 7 }, { seq: 3, value: 4 }]);
  assert.equal(b.sealedGrand, 13);
  assert.equal(b.matched, true);
  // 终帧
  b = breakdownSources(r, 1, 6);
  assert.equal(b.combinedGrand, 26);
  assert.equal(b.matched, true);
});

test('来源分解：步骤早于检查点封存事件 -> STEP_BEFORE_CHECKPOINT', () => {
  const plan = { channels: 2, events: [D(1, 1, 10), D(2, 1, 1), B(1, 1), B(2, 1)] };
  const r = replay(plan);
  expectBreakdown(r, 1, 0, 'STEP_BEFORE_CHECKPOINT');
  expectBreakdown(r, 1, 2, 'STEP_BEFORE_CHECKPOINT');
  // 封存发布当帧（帧3）允许，且其后部分为空
  const b = breakdownSources(r, 1, 3);
  assert.equal(b.afterGrand, 0);
  assert.equal(b.matched, true);
});

test('来源分解：半完成检查点（从未发布）-> CHECKPOINT_INCOMPLETE', () => {
  const plan = {
    channels: 2,
    events: [
      D(1, 1, 10), D(2, 1, 5), B(1, 1), B(2, 1),
      C('snapshot'), B(1, 2), B(2, 2), // cp2 只有半成品快照
      R(), D(1, 2, 9),
    ],
  };
  const r = replay(plan);
  // 故障帧本身冻结，先拦 STEP_FROZEN；但 cp2 在所有帧都不是完整检查点
  expectBreakdown(r, 2, 8, 'CHECKPOINT_INCOMPLETE');
  expectBreakdown(r, 2, 3, 'CHECKPOINT_INCOMPLETE');
});

test('来源分解：故障冻结帧与崩溃后暂存帧 -> STEP_FROZEN_AT_ERROR', () => {
  const plan = { channels: 2, events: [B(1, 1), B(2, 1), C(), D(1, 1, 3), R(), D(1, 1, 3)] };
  const r = replay(plan);
  expectBreakdown(r, 1, 2, 'STEP_FROZEN_AT_ERROR'); // 立即崩溃帧
  expectBreakdown(r, 1, 3, 'STEP_FROZEN_AT_ERROR'); // 崩溃后暂存数据帧
});

test('来源分解：快照阶段故障重开后只能分解恢复采用的完整检查点，同序号不双算', () => {
  const plan = {
    channels: 2,
    events: [
      D(1, 1, 10), D(2, 1, 5), B(1, 1), B(2, 1),
      D(1, 2, 9),
      C('snapshot'), B(2, 2), B(1, 2), // 半成品 cp2
      R(),
      D(1, 2, 9), B(2, 2), B(1, 2), D(2, 2, 6),
    ],
  };
  const r = replay(plan);
  const last = r.frames.length - 1;
  // 只能分解恢复检查点 cp1
  const b = breakdownSources(r, 1, last);
  assert.equal(b.reopened, true);
  assert.equal(b.sealedGrand, 15);
  // 9 只在重放后计入一次（崩溃前帧4的入账已被重开回滚，不进入来源）
  assert.deepEqual(b.channels[0].after.seqs, [2]);
  assert.equal(b.channels[0].after.total, 9);
  assert.deepEqual(b.channels[1].after.seqs, [2]);
  assert.equal(b.channels[1].after.total, 6);
  assert.equal(b.combinedGrand, 30);
  assert.equal(b.matched, true);
  // 重开后新发布的 cp2 不是恢复来源，拒绝
  expectBreakdown(r, 2, last, 'CHECKPOINT_NOT_RECOVERY');
  // 重开当帧（事件 #8）：状态已回到 cp1，可分解 cp1
  const atReopen = breakdownSources(r, 1, 8);
  assert.equal(atReopen.currentGrand, 15);
  assert.equal(atReopen.afterGrand, 0);
  assert.equal(atReopen.matched, true);
});

test('来源分解：意图阶段故障（无半成品快照）重开后，新发布的完整检查点可正常分解', () => {
  const plan = {
    channels: 2,
    events: [D(1, 1, 4), C('intent'), B(1, 1), R(), D(1, 1, 4), D(2, 1, 2), B(1, 1), B(2, 1)],
  };
  const r = replay(plan);
  const b = breakdownSources(r, 1, r.frames.length - 1);
  assert.equal(b.sealedGrand, 6);
  assert.equal(b.afterGrand, 0);
  assert.equal(b.matched, true);
});

test('来源分解：多通道第二检查点的封存范围与检查点后释放缓存', () => {
  const plan = {
    channels: 3,
    events: [
      D(1, 1, 1), D(2, 1, 2), D(3, 1, 4),
      B(1, 1), B(2, 1), B(3, 1),
      D(1, 2, 10), D(2, 2, 20),
      B(2, 2),
      D(2, 3, 30), // 缓存
      B(1, 2), B(3, 2), // 对齐：同帧释放 30
    ],
  };
  const r = replay(plan);
  const b = breakdownSources(r, 2, r.frames.length - 1);
  assert.deepEqual(b.channels[0].sealed.seqs, [1, 2]);
  assert.equal(b.channels[0].sealed.total, 11);
  assert.deepEqual(b.channels[1].sealed.seqs, [1, 2]);
  assert.equal(b.channels[1].sealed.total, 22);
  // 通道2 的 30 在 cp2 快照后释放，归入"检查点后"
  assert.deepEqual(b.channels[1].after.seqs, [3]);
  assert.equal(b.channels[1].after.total, 30);
  assert.deepEqual(b.channels[2].sealed.seqs, [1]);
  assert.equal(b.channels[2].sealed.total, 4);
  assert.equal(b.sealedGrand, 37);
  assert.equal(b.afterGrand, 30);
  assert.equal(b.combinedGrand, 67);
  assert.equal(b.matched, true);
});

test('来源分解：小数值按通道求和与交错累加顺序不同，容差内仍核对一致', () => {
  const plan = {
    channels: 2,
    events: [
      D(1, 1, 0.1), D(2, 1, 0.2), B(1, 1), B(2, 1),
      D(1, 2, 0.3), D(2, 2, 0.4),
    ],
  };
  const r = replay(plan);
  const b = breakdownSources(r, 1, 5);
  assert.ok(Math.abs(b.sealedGrand - 0.3) < 1e-9);
  assert.ok(Math.abs(b.afterGrand - 0.7) < 1e-9);
  assert.equal(b.matched, true, '0.1+0.2 与 0.3+0.4 顺序差不应误报不一致');
  assert.ok(Math.abs(b.difference) < 1e-9);
});

test('来源分解：参数非法 -> BAD_BREAKDOWN_ARGS', () => {
  const r = replay({ channels: 2, events: [D(1, 1, 1), B(1, 1), B(2, 1)] });
  expectBreakdown(r, 0, 2, 'BAD_BREAKDOWN_ARGS');
  expectBreakdown(r, -2, 2, 'BAD_BREAKDOWN_ARGS');
  expectBreakdown(r, 1, -1, 'BAD_BREAKDOWN_ARGS');
  expectBreakdown(r, 1, 99, 'BAD_BREAKDOWN_ARGS');
  expectBreakdown(r, 'x', 2, 'BAD_BREAKDOWN_ARGS');
});

