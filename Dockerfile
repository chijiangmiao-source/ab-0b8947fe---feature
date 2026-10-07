# 零依赖：node:20-alpine 直接运行原生 ES Module，无需 npm install
FROM node:20-alpine AS base
WORKDIR /app
COPY . .
# 页面构建：把 Worker 依赖的 engine/worker/storage-idb 模块置入 web/
RUN node scripts/build.mjs
EXPOSE 8080

# 页面 + 健康响应服务
FROM base AS web
ENV NODE_ENV=production
CMD ["node", "server.mjs"]

# verify：代码测试 -> 页面构建 -> HTTP 冒烟，结束后以退出码报告（供 compose 判定）
FROM base AS verify
ENV WEB_URL=http://web:8080
CMD ["node", "scripts/verify.mjs"]
