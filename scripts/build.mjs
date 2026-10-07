// scripts/build.mjs — 零依赖构建：将 Worker 依赖的 ES 模块置入 web 根并做加载冒烟
import { copyFile, mkdir, rm } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const root = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const distModules = ['engine.mjs', 'worker.mjs', 'storage-idb.mjs'];

console.log('[build] 清理并拷贝 Worker 模块到 web/ …');
for (const f of distModules) {
  await rm(path.join(root, 'web', f), { force: true });
}
await mkdir(path.join(root, 'web'), { recursive: true });
for (const f of distModules) {
  await copyFile(path.join(root, 'src', f), path.join(root, 'web', f));
  console.log(`[build]   src/${f} -> web/${f}`);
}

console.log('[build] 模块加载冒烟（import engine / storage-idb）…');
await import(pathToFileURL(path.join(root, 'web', 'engine.mjs')).href);
await import(pathToFileURL(path.join(root, 'web', 'storage-idb.mjs')).href);
console.log('[build] OK：构建通过');
