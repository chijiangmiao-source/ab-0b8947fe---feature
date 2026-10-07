// storage-idb.mjs — 浏览器侧 IndexedDB 持久化（仅在 Worker 内使用）
// 极简键值对象仓库 abyssal-db / kv：key 主键。
// 键约定：
//   intent:cpN:chM     检查点 N 通道 M 的对齐意图（阶段 1）
//   snapshot:cpN       检查点 N 的快照：累计状态 + 各通道输入序号（阶段 2，半成品）
//   published:cpN      检查点 N 的发布标记（阶段 3）。无此配对的 snapshot 一律不采用。

const DB_NAME = 'abyssal-checkpoint-db';
const DB_VERSION = 1;
const STORE = 'kv';

function openDB() {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(DB_NAME, DB_VERSION);
    req.onupgradeneeded = () => {
      const db = req.result;
      if (!db.objectStoreNames.contains(STORE)) db.createObjectStore(STORE);
    };
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
}

function txn(db, mode) {
  return db.transaction(STORE, mode).objectStore(STORE);
}

export function createIDBStore() {
  let dbPromise = null;
  const db = () => (dbPromise ??= openDB());

  return {
    name: 'indexeddb',
    async put(key, value) {
      const d = await db();
      await new Promise((resolve, reject) => {
        const req = txn(d, 'readwrite').put(value, key);
        req.onsuccess = () => resolve();
        req.onerror = () => reject(req.error);
      });
    },
    async del(key) {
      const d = await db();
      await new Promise((resolve, reject) => {
        const req = txn(d, 'readwrite').delete(key);
        req.onsuccess = () => resolve();
        req.onerror = () => reject(req.error);
      });
    },
    async all() {
      const d = await db();
      return new Promise((resolve, reject) => {
        const out = [];
        const req = txn(d, 'readonly').openCursor();
        req.onsuccess = () => {
          const cursor = req.result;
          if (cursor) {
            out.push({ key: cursor.key, value: cursor.value });
            cursor.continue();
          } else {
            resolve(out);
          }
        };
        req.onerror = () => reject(req.error);
      });
    },
    async clear() {
      const d = await db();
      await new Promise((resolve, reject) => {
        const req = txn(d, 'readwrite').clear();
        req.onsuccess = () => resolve();
        req.onerror = () => reject(req.error);
      });
    },
  };
}
