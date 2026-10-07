// test/worker.test.mjs — 在 Node 中模拟浏览器环境执行真实 worker.mjs：
// 提供 self/postMessage 与内存版 IndexedDB，验证 Worker 端三阶段写入、
// 故障注入半成品、重开清理与“仅采用已发布检查点”的存储不变量。
import { test } from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

// ---------- 内存 IndexedDB（仅实现应用用到的 API） ----------
function makeMemoryIndexedDB() {
  const databases = new Map();

  function makeRequest() {
    return { onsuccess: null, onerror: null, result: undefined, error: null };
  }
  function fire(req, action) {
    queueMicrotask(() => {
      try {
        action();
        if (req.onsuccess) req.onsuccess({ target: req });
      } catch (e) {
        req.error = e;
        if (req.onerror) req.onerror({ target: req });
      }
    });
  }

  function makeStore(data) {
    const entries = () => [...data.entries()].sort((a, b) => String(a[0]).localeCompare(String(b[0])));
    return {
      put(value, key) {
        const req = makeRequest();
        fire(req, () => {
          data.set(key, value);
          req.result = key;
        });
        return req;
      },
      delete(key) {
        const req = makeRequest();
        fire(req, () => {
          data.delete(key);
          req.result = undefined;
        });
        return req;
      },
      clear() {
        const req = makeRequest();
        fire(req, () => {
          data.clear();
          req.result = undefined;
        });
        return req;
      },
      openCursor() {
        const req = makeRequest();
        const all = entries();
        let i = 0;
        const makeCursor = () => ({
          get key() {
            return all[i]?.[0];
          },
          get value() {
            return all[i]?.[1];
          },
          continue() {
            i++;
            fire(req, () => {
              req.result = i < all.length ? makeCursor() : null;
            });
          },
        });
        fire(req, () => {
          req.result = all.length ? makeCursor() : null;
        });
        return req;
      },
    };
  }

  return {
    open(name, version) {
      const req = makeRequest();
      queueMicrotask(() => {
        let db = databases.get(name);
        const isNew = !db;
        if (isNew) {
          db = { stores: new Map(), __version: version };
          databases.set(name, db);
        }
        req.result = {
          objectStoreNames: { contains: (n) => db.stores.has(n) },
          createObjectStore(n) {
            db.stores.set(n, new Map());
          },
          transaction(storeName, mode) {
            return {
              objectStore(n) {
                const data = db.stores.get(n || storeName);
                return makeStore(data);
              },
            };
          },
        };
        if (isNew && req.onupgradeneeded) req.onupgradeneeded({ target: req });
        if (req.onsuccess) req.onsuccess({ target: req });
      });
      return req;
    },
    _databases: databases,
  };
}

let workerEnv = null;
async function loadWorker() {
  const workerPath = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../src/worker.mjs');
  if (!workerEnv) {
    const idb = makeMemoryIndexedDB();
    globalThis.indexedDB = idb;
    const inbox = [];
    // worker.mjs 在首次 import 时把 onmessage 挂到这个稳定的 self 对象上
    globalThis.self = { postMessage: (m) => inbox.push(m) };
    await import(pathToFileURL(workerPath).href);
    workerEnv = { inbox, idb };
  }
  const { inbox, idb } = workerEnv;
  return {
    send: (msg) =>
      new Promise((resolve, reject) => {
        inbox.length = 0;
        globalThis.self.onmessage({ data: msg });
        const t0 = Date.now();
        const tick = setInterval(() => {
          const m = inbox.find((x) => x.type === 'result' || x.type === 'validation-error' || x.type === 'error');
          if (m) {
            clearInterval(tick);
            resolve(m);
          } else if (Date.now() - t0 > 3000) {
            clearInterval(tick);
            reject(new Error('worker timeout'));
          }
        }, 5);
      }),
    raw: () => inbox,
    db: idb,
  };
}

const D = (channel, seq, value) => ({ type: 'data', channel, seq, value });
const B = (channel, checkpoint) => ({ type: 'barrier', channel, checkpoint });

test('Worker：完整发布流程后存储含 intent×2 + snapshot + published，且快照标记完整', async () => {
  const w = await loadWorker();
  const out = await w.send({
    type: 'run',
    plan: { channels: 2, events: [D(1, 1, 10), D(2, 1, 5), B(1, 1), B(2, 1)] },
  });
  assert.equal(out.type, 'result');
  const lastFrame = out.result.frames.at(-1);
  const keys = lastFrame.storageAfter.map((s) => s.key).sort();
  assert.deepEqual(keys, ['intent:cp1:ch1', 'intent:cp1:ch2', 'published:cp1', 'snapshot:cp1']);
  assert.ok(lastFrame.storageAfter.find((s) => s.key === 'snapshot:cp1').complete);
  const snap = lastFrame.storageAfter.find((s) => s.key === 'snapshot:cp1');
  assert.equal(snap.snapshot.total, 15);
  assert.deepEqual(snap.snapshot.inputSeq, [0, 1, 1]);
});

test('Worker：快照后故障 + 重开，存储删除半成品，重放后不重复计入', async () => {
  const w = await loadWorker();
  const plan = {
    channels: 2,
    events: [
      D(1, 1, 10), D(2, 1, 5), B(1, 1), B(2, 1),
      { type: 'crash', stage: 'snapshot' },
      B(1, 2), B(2, 2), // 半成品 cp2 -> 中断
      { type: 'reopen' },
      D(1, 2, 9),
      B(1, 2), B(2, 2),
      D(2, 2, 6),
    ],
  };
  const out = await w.send({ type: 'run', plan });
  assert.equal(out.type, 'result');

  // 崩溃当帧存储里确有半成品快照（供审查核对：故障后不得在 UI 当作完成快照）
  const crashFrame = out.result.frames.find((f) => f.crashed);
  const half = crashFrame.storageAfter.find((s) => s.key === 'snapshot:cp2');
  assert.ok(half, '故障帧应可见 snapshot:cp2');
  assert.equal(half.complete, false, '故障快照必须标记为未完成');

  // 终态：无 cp2 旧半成品残留；总值不重复
  const finalStore = out.result.frames.at(-1).storageAfter;
  const cps = new Set(finalStore.map((s) => s.checkpoint));
  assert.ok(cps.has(1) && cps.has(2));
  const snap2 = finalStore.find((s) => s.key === 'snapshot:cp2');
  assert.equal(snap2.complete, true, '重放后的 cp2 必须完整');
  assert.equal(out.result.frames.at(-1).total, 30, '10+5+9+6，9 不重复计入');
});

test('Worker：序号失配返回 validation-error 并定位首个事件下标', async () => {
  const w = await loadWorker();
  const out = await w.send({ type: 'run', plan: { channels: 2, events: [D(1, 1, 1), D(1, 2, 1), D(1, 2, 1)] } });
  assert.equal(out.type, 'validation-error');
  assert.equal(out.index, 2);
  assert.equal(out.code, 'SEQ_DUPLICATE');
});
