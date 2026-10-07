// scripts/verify.mjs — 容器 verify 服务入口：
//   1) 页面/模块构建（零依赖构建 + 加载冒烟）
//   2) 代码测试（node --test，覆盖屏障、缓存、恢复、错误边界）
//   3) HTTP 冒烟（/health、/、Worker 模块）
// 全部通过才以退出码 0 结束，任一步失败立即以非零退出码报告。
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import http from 'node:http';

const root = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const SMOKE_PORT = Number(process.env.SMOKE_PORT || 8099);
// 本地运行时自起服务；Compose 中由环境变量指向 web 服务（如 http://web:8080）
const WEB_URL = process.env.WEB_URL || '';

const log = (msg) => console.log(`[verify] ${msg}`);
const fail = (msg, code = 1) => {
  console.error(`[verify] ✗ ${msg}`);
  process.exit(code);
};

async function run(cmd, args) {
  log(`$ ${cmd} ${args.join(' ')}`);
  const child = spawn(cmd, args, { cwd: root, stdio: 'inherit' });
  const [code] = await once(child, 'exit');
  if (code !== 0) fail(`步骤失败（exit ${code}）：${cmd} ${args.join(' ')}`, code || 1);
}

function fetchText(url) {
  return new Promise((resolve, reject) => {
    const req = http.get(url, (res) => {
      let body = '';
      res.on('data', (d) => (body += d));
      res.on('end', () => resolve({ status: res.statusCode, headers: res.headers, body }));
    });
    req.on('error', reject);
    req.setTimeout(4000, () => req.destroy(new Error('timeout')));
  });
}

async function waitForHealth(base, tries = 40) {
  for (let i = 0; i < tries; i++) {
    try {
      const r = await fetchText(base + '/health');
      if (r.status === 200 && JSON.parse(r.body).status === 'ok') return;
    } catch {}
    await new Promise((r) => setTimeout(r, 250));
  }
  fail(`健康端点未就绪：${base}/health`);
}

async function smoke(base) {
  log(`HTTP 冒烟 -> ${base}`);
  await waitForHealth(base);

  const checks = [
    { path: '/health', test: (r) => r.status === 200 && /"status":"ok"/.test(r.body), label: '健康 JSON 200 status=ok' },
    { path: '/', test: (r) => r.status === 200 && /检查点回放器/.test(r.body), label: '首页 200 含应用标题' },
    { path: '/app.mjs', test: (r) => r.status === 200 && /text\/javascript/.test(r.headers['content-type'] || ''), label: 'app.mjs 以 JS MIME 提供' },
    { path: '/worker.mjs', test: (r) => r.status === 200 && /createIDBStore|storage-idb/.test(r.body), label: 'worker.mjs 已构建并可访问' },
    { path: '/engine.mjs', test: (r) => r.status === 200 && /export function replay/.test(r.body), label: 'engine.mjs 已构建并可访问' },
    { path: '/styles.css', test: (r) => r.status === 200, label: 'styles.css 200' },
    { path: '/no-such-path', test: (r) => r.status === 404, label: '未知路径 404' },
  ];
  for (const c of checks) {
    const r = await fetchText(base + c.path);
    if (!c.test(r)) fail(`冒烟失败：${c.label}（${c.path} 实际 ${r.status}）`);
    log(`  ✓ ${c.label}`);
  }
}

let server = null;
async function main() {
  log('== 阶段 1/3：构建 ==');
  await run(process.execPath, ['scripts/build.mjs']);

  log('== 阶段 2/3：代码测试（屏障 / 缓存 / 恢复 / 错误边界） ==');
  await run(process.execPath, ['--test', 'test/']);

  log('== 阶段 3/3：HTTP 冒烟 ==');
  let base = WEB_URL.replace(/\/$/, '');
  if (!base) {
    log(`自起静态服务于端口 ${SMOKE_PORT} …`);
    process.env.PORT = String(SMOKE_PORT);
    server = await import(pathToFileURL(path.join(root, 'server.mjs')).href);
    await new Promise((r) => setTimeout(r, 200));
    base = `http://127.0.0.1:${SMOKE_PORT}`;
  }
  await smoke(base);

  log('✓ 全部通过：构建、代码测试、HTTP 冒烟均成功');
  process.exit(0);
}

process.on('uncaughtException', (e) => fail('未捕获异常：' + (e.stack || e)));
main();
