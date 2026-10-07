# 深海探测阵列 · 屏障对齐检查点回放器

审查多条乱序回收链路时，确认**检查点不会把已越过屏障的数据混入此前状态**，且**浏览器中断重开后不会重复计入**的可视化回放器。

- 录入 **2–4 条输入通道**、按捕获顺序排列的 **≤48 项**事件：数据 / 检查点屏障 / 故障 / 重开
- **整段回放**或拖动查看**任一步**的通道状态（累计值、缓存区、对齐进度、持久化层）
- **累计值来源分解**：检查点列表提供入口，选择一个已完整发布的检查点与不早于它的步骤后，按通道展示封存序号范围与封存累计、检查点后实际纳入序号与逐笔增量，并给出两部分合计与当前累计值的核对结果；仍在缓存中的数据两部分都不含
- 严格屏障对齐：某通道屏障先到后，该通道后续数据**缓存**；全部通道到达同一屏障才**封存累计状态与各通道输入序号**，再**按原序释放缓存**
- 回放器在 **Web Worker 内按序持久化三阶段**：`intent`（意图）→ `snapshot`（快照+输入序号）→ `published`（发布标记，IndexedDB）
- 重开后**只采用带发布标记的最新检查点**，从其后按通道输入序号重放；无发布标记的半成品快照与游离意图一律删除，页面绝不展示半完成快照
- 序号失配、跳号、重复屏障、交叉对齐（错误释放）等均**定位首个出错事件**（下标 + 错误码）
- 来源分解的拒绝条件（均返回明确错误码且不残留旧分解）：所选步骤早于检查点封存、检查点在该步尚未完整发布（半成品快照永不出现）、步骤已在故障处冻结；快照阶段故障重开后只允许分解**恢复采用的那个完整检查点**，未发生故障的既有整段回放、逐步查看与错误定位结果不变

## 运行（Docker Compose）

```bash
# 默认宿主端口 8080；可配置：
WEB_PORT=9090 docker compose up --build
# 打开 http://localhost:9090 ，健康端点 http://localhost:9090/health
```

## verify 服务（测试 + 构建 + HTTP 冒烟，退出码报告）

```bash
docker compose run --rm verify
# 内部顺序：
#   1. node scripts/build.mjs    页面/模块构建与加载冒烟
#   2. node --test test/         屏障、缓存、恢复、错误边界、来源分解代码测试（30 项）
#   3. HTTP 冒烟                 /health、首页、worker/engine 模块、404
# 全部通过退出码 0，任一失败非零退出。
```

## 本地无 Docker 运行

零第三方依赖，仅需 Node ≥ 20：

```bash
npm run build     # 构建 Worker 模块到 web/
npm test          # 代码测试
npm start         # http://localhost:8080
npm run verify    # 构建 + 测试 + 冒烟（自起临时服务）
```

## 事件模型

| 事件 | 字段 | 语义 |
| --- | --- | --- |
| 数据 `data` | `channel, seq, value` | 该通道序号 `seq` 从 1 起**连续递增**；屏障已到时缓存，否则计入累计值 |
| 屏障 `barrier` | `channel, checkpoint` | 通道收到某检查点屏障；编号须 1,2,3… 严格递增且不得重复 |
| 故障 `crash` | `stage?` | 立即中断；或注入到下一检查点 `intent`/`snapshot` 阶段完成后中断（后者产生半完成快照） |
| 重开 `reopen` | — | 清理未发布写入，采用最新完整发布检查点，按各通道输入序号之后重放 |

持久化键：`intent:cpN:chM`、`snapshot:cpN`、`published:cpN`。仅当存在配对的 `published:cpN` 时，`snapshot:cpN` 才视为完整。

## 目录

```
src/engine.mjs       纯逻辑核心（校验、对齐、封存、恢复、逐帧、来源分解 sourceBreakdown）
src/worker.mjs       Worker：按帧顺序执行三阶段持久化与重开清理
src/storage-idb.mjs  IndexedDB 键值适配
web/                 页面（index.html / app.mjs / styles.css，构建产物同目录）
server.mjs           零依赖静态服务 + /health
scripts/build.mjs    零依赖构建
scripts/verify.mjs   verify 服务入口（退出码报告）
test/                node:test：引擎 27 项 + 真实 Worker/内存 IndexedDB 3 项
```
