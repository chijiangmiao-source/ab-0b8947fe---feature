// server.mjs — 零依赖静态服务 + /health 健康端点
import http from 'node:http';
import { readFile } from 'node:fs/promises';
import { existsSync, statSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const WEB_DIR = path.join(__dirname, 'web');
const PORT = Number(process.env.PORT || 8080);
const HOST = process.env.HOST || '0.0.0.0';
const STARTED = Date.now();

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.mjs': 'text/javascript; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon',
  '.png': 'image/png',
};

function send(res, status, body, type = 'text/plain; charset=utf-8') {
  res.writeHead(status, {
    'Content-Type': type,
    'X-Content-Type-Options': 'nosniff',
    'Cache-Control': 'no-store',
  });
  res.end(body);
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host || 'local'}`);
  const pathname = decodeURIComponent(url.pathname);

  if (pathname === '/health') {
    send(
      res,
      200,
      JSON.stringify({
        status: 'ok',
        service: 'abyss-checkpoint-replay',
        uptimeMs: Date.now() - STARTED,
        time: new Date().toISOString(),
      }),
      'application/json; charset=utf-8'
    );
    return;
  }

  let rel = pathname === '/' ? '/index.html' : pathname;
  const filePath = path.normalize(path.join(WEB_DIR, rel));
  if (!filePath.startsWith(WEB_DIR + path.sep) || !existsSync(filePath) || !statSync(filePath).isFile()) {
    send(res, 404, 'Not Found');
    return;
  }
  try {
    const body = await readFile(filePath);
    send(res, 200, body, MIME[path.extname(filePath)] || 'application/octet-stream');
  } catch {
    send(res, 500, 'Internal Server Error');
  }
});

server.listen(PORT, HOST, () => {
  console.log(`[web] 深海检查点回放器: http://${HOST}:${PORT}  (health: /health)`);
});

for (const sig of ['SIGINT', 'SIGTERM']) process.on(sig, () => server.close(() => process.exit(0)));
