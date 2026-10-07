// worker.mjs — 回放器 Web Worker：所有持久化都在 Worker 内按帧顺序完成
// 顺序保证：对每一帧，严格按 frame.persistence 顺序写：
//   intent:*  -> snapshot:*（含累计状态与各通道输入序号） -> published:*
// REOPEN 帧按 frame.cleanup.deleted 删除所有无 published 配对的半成品。
// Worker 回报每帧执行后的真实存储清单，UI 不得展示任何未发布快照。

import { replay, ValidationError } from './engine.mjs';
import { createIDBStore } from './storage-idb.mjs';

const store = createIDBStore();

function parseKey(key) {
  if (key.startsWith('intent:')) {
    const m = /^intent:cp(\d+):ch(\d+)$/.exec(key);
    return { stage: 'intent', checkpoint: m ? Number(m[1]) : null, channel: m ? Number(m[2]) : null };
  }
  if (key.startsWith('snapshot:')) {
    return { stage: 'snapshot', checkpoint: Number(key.slice('snapshot:cp'.length)) };
  }
  if (key.startsWith('published:')) {
    return { stage: 'published', checkpoint: Number(key.slice('published:cp'.length)) };
  }
  return { stage: 'unknown', checkpoint: null };
}

async function describeStorage() {
  const entries = await store.all();
  const publishedCps = new Set(
    entries.filter((e) => e.key.startsWith('published:')).map((e) => parseKey(e.key).checkpoint)
  );
  return entries
    .map(({ key, value }) => {
      const meta = parseKey(key);
      const complete =
        meta.stage === 'published' || (meta.stage === 'snapshot' && publishedCps.has(meta.checkpoint));
      return {
        key,
        stage: meta.stage,
        checkpoint: meta.checkpoint,
        channel: meta.channel ?? null,
        complete,
        snapshot: meta.stage === 'snapshot' ? value.snapshot ?? null : null,
      };
    })
    .sort((a, b) => a.key.localeCompare(b.key, undefined, { numeric: true }));
}

async function runPlan(plan) {
  // 每次整段回放从空存储开始（模拟全新部署），三阶段写入全程可被故障注入打断
  await store.clear();
  const result = replay(plan);

  for (const frame of result.frames) {
    // 关键：必须先全部 intent、对齐时 snapshot、最后 published——顺序由引擎帧保证，这里逐行 await
    for (const w of frame.persistence) {
      if (w.stage === 'intent') {
        await store.put(w.key, { stage: 'intent', checkpoint: w.checkpoint, channel: w.channel, atEvent: frame.index });
      } else if (w.stage === 'snapshot') {
        await store.put(w.key, { stage: 'snapshot', checkpoint: w.checkpoint, snapshot: w.snapshot, atEvent: frame.index });
      } else {
        await store.put(w.key, { stage: 'published', checkpoint: w.checkpoint, atEvent: frame.index });
      }
    }
    if (frame.cleanup) {
      for (const key of frame.cleanup.deleted) await store.del(key);
    }
    frame.storageAfter = await describeStorage();
  }
  return result;
}

self.onmessage = async (e) => {
  const msg = e.data || {};
  try {
    if (msg.type === 'run') {
      const result = await runPlan(msg.plan);
      self.postMessage({ type: 'result', result });
    } else if (msg.type === 'storage') {
      self.postMessage({ type: 'storage', storage: await describeStorage() });
    } else if (msg.type === 'reset') {
      await store.clear();
      self.postMessage({ type: 'reset-done' });
    } else {
      self.postMessage({ type: 'error', message: `未知消息类型 ${msg.type}` });
    }
  } catch (err) {
    if (err instanceof ValidationError) {
      self.postMessage({
        type: 'validation-error',
        message: err.message,
        index: err.index,
        code: err.code,
      });
    } else {
      self.postMessage({ type: 'error', message: String(err && err.message ? err.message : err) });
    }
  }
};
