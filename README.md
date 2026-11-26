# FD/EMFILE 采样网关接入演练

一个本地演练系统：FastAPI 控制面管理一个低 `RLIMIT_NOFILE` 的 Linux TCP 回显进程。回显进程只用**一个网络 I/O 线程**通过 `select(2)` 多路复用监听套接字和全部 TCP 连接（另有一个阻塞式 JSON 控制主线程，全进程共 2 个线程，硬性上限 4），因此值班员可以同时保留大量采样连接而不产生每连接线程开销。

通过 API 打开并持有样例日志文件描述符，直到新连接的 `accept(2)` 实际返回 `EMFILE`；同时验证：

- 默认 `RLIMIT_NOFILE=32` 下至少 20 条 TCP 连接同时保持并回显；
- 回显进程线程数始终不超过 4（实测为 2），空闲时阻塞在 `select()`，不忙等；
- 一条连接停止读取、发送被堵住时，只对它自己产生背压，其他连接照常回显；慢连接恢复读取后按原序收到完整回显；
- 描述符耗尽期间旧连接继续回显，单条新探测建连后 1 秒内收到 EOF 或 RST；
- EMFILE 计数只取自实际失败的 `accept(2)`；
- 释放样例日志后，原监听套接字（FD 3）不重启即自动恢复接入。

## 启动

```bash
docker compose up --build
```

- FastAPI/Swagger：<http://127.0.0.1:8000/docs>
- 健康检查：<http://127.0.0.1:8000/healthz>
- TCP 回显端口：`127.0.0.1:9000`

Compose 中的 `drill` 服务会启动 Uvicorn API，并由 API 拉起 `app/echo_worker.py`。回显进程在创建监听套接字前把自己的 `RLIMIT_NOFILE` 降到 `DRILL_NOFILE_LIMIT`（默认 `32`）。

## 一键演示全部关键断言

最快的服务端完整自检（一个请求完成 20 并发、线程上限、慢连接隔离与保序、旧连接、EMFILE 探测、计数、CPU 和恢复验证）：

```bash
curl -s -X POST http://127.0.0.1:8000/api/v1/drill/self-test | python3 -m json.tool
```

也可以从宿主机运行独立 TCP 演示脚本，验证 Compose 端口映射：

```bash
python3 scripts/demo.py
# 或依次执行服务端自检和宿主机 TCP 演示：
./scripts/demo-requests.sh
```

脚本只使用 Python 标准库，依次完成：

1. 检查 API 和回显进程健康。
2. 记录初始 EMFILE 计数。
3. 同时建立并保持 20 条 TCP 连接，逐条验证回显；通过 `/api/v1/status` 查验活动连接数、FD 数和线程数（上限 4）。
4. 建立一条“慢连接”：只发送不读取，直到回显进程为其累积发送队列；期间其余 20 条连接持续回显；慢连接恢复读取后，校验收到完整 4 MiB 回显且顺序与发送完全一致。
5. 调用 `/api/v1/drill/exhaust` 打开样例日志直到描述符表满。
6. 在故障期间向所有旧连接（含曾停读的慢连接）发送数据并验证回显一致。
7. 只建立一条新探测连接，断言其在连接建立后 1 秒内收到 EOF 或 RST。
8. 通过 API 查询真实 `accept(2)` 返回 `EMFILE` 的累计次数。
9. 采样 I/O 线程 1 秒 CPU，确认没有忙等。
10. 调用 `/api/v1/drill/release` 关闭样例文件，用全新连接验证回显，确认原监听自动恢复。

可选参数：

```bash
python3 scripts/demo.py --api-base http://127.0.0.1:8000 \
                        --echo-host 127.0.0.1 \
                        --echo-port 9000
                        # --skip-concurrency 可跳过 20 连接/慢连接阶段
```

## 演示请求

JetBrains/VS Code REST Client 可直接使用 [`requests/demo-requests.http`](requests/demo-requests.http)。

等价的 curl 顺序：

```bash
# 健康与初始状态（含 thread_count/thread_limit、connections 明细）
curl -s http://127.0.0.1:8000/healthz | python3 -m json.tool
curl -s http://127.0.0.1:8000/api/v1/status | python3 -m json.tool

# 占满回显进程的文件描述符名额
curl -s -X POST http://127.0.0.1:8000/api/v1/drill/exhaust | python3 -m json.tool

# API 从同容器发起一条新探测；故障期期望 outcome=eof 或 reset
curl -s -X POST http://127.0.0.1:8000/api/v1/probe \
  -H 'Content-Type: application/json' \
  -d '{"send_payload": false, "timeout_seconds": 1.0}' | python3 -m json.tool

# 故障期间查询实际 EMFILE 次数
curl -s http://127.0.0.1:8000/api/v1/metrics/emfile | python3 -m json.tool

# 采样 1 秒 I/O 线程 CPU；busy_spin_suspected 应为 false
curl -s 'http://127.0.0.1:8000/api/v1/drill/accept-loop-check?seconds=1.0' \
  | python3 -m json.tool

# 通过 API 关闭样例日志描述符
curl -s -X POST http://127.0.0.1:8000/api/v1/drill/release | python3 -m json.tool

# 恢复后新连接成功回显
curl -s -X POST http://127.0.0.1:8000/api/v1/probe \
  -H 'Content-Type: application/json' \
  -d '{"payload": "recovery-echo", "expect_echo": true}' | python3 -m json.tool
```

原始 TCP 行为可用 `nc 127.0.0.1 9000` 观察；完整的长连接保持、20 并发、慢连接保序和计时断言由 `scripts/demo.py` 执行。

## 主要 API

| 方法 | 路径 | 行为 |
| --- | --- | --- |
| `GET` | `/healthz` | API 与受管回显进程健康状态 |
| `GET` | `/api/v1/status` | RLIMIT、PID/TID、线程数与上限、监听 FD、样例 FD、每条连接的接收/排队/发送字节、FD 数、EMFILE 和回显指标 |
| `POST` | `/api/v1/drill/exhaust` | 打开样例日志直到 `open(2)` 返回 `EMFILE` |
| `POST` | `/api/v1/drill/release` | 关闭全部 API 打开的样例日志 FD |
| `POST` | `/api/v1/sample-files/open` | 打开指定数量，或 `{"fill_to_limit": true}` |
| `DELETE` | `/api/v1/sample-files` | 同 release |
| `GET` | `/api/v1/metrics/emfile` | 查询真实 accept EMFILE 累计次数 |
| `GET` | `/api/v1/drill/accept-loop-check?seconds=1` | 通过 `/proc` 采样 I/O 线程 CPU/状态 |
| `POST` | `/api/v1/probe` | 发起一条 TCP 探测并分类 `eof`/`reset`/`echo`/`timeout` |
| `POST` | `/api/v1/drill/self-test` | 服务端执行完整 20 并发/慢连接/EMFILE/EOF/非忙等/恢复断言 |
| `POST` | `/api/v1/drill/restart` | 重启受管回显进程并重置其进程内计数 |

为方便手工调用，也提供无版本别名：`/api/status`、`/api/emfile-count`、`/api/fill`、`/api/close`、`/api/probe`。

## 实现行为

### 线程模型（最多 4 个线程，实测 2 个）

```text
fd-drill-control      主线程：阻塞读取 stdin 上的行分隔 JSON 控制命令
fd-drill-network-io   唯一 I/O 线程：select() 多路复用 listener + 全部连接
```

不再为每条连接创建线程。20 条（乃至更多，受 FD 上限约束）连接共享同一个 I/O 线程，连接数增长不会带来线程增长。`/api/v1/status` 中的 `thread_count`、`thread_limit`、`within_thread_limit` 和 `threads` 可随时查验；线程枚举走 `threading.enumerate()`，不消耗描述符，因此描述符表满时仍可查询。

### 慢连接隔离与按序回显

每条连接维护一个 FIFO 字节发送队列（`deque[bytes]` + 头部偏移）：

- 可读事件把 `recv()` 到的原始字节按到达顺序追加到该连接自己的队列；
- 可写事件尽量 `send()`；遇到 `EAGAIN` 时保留队列原样返回，等下一次可写唤醒，数据不丢、不乱序；
- 因此一条停读的连接只会让它自己的队列增长（背压隔离），其他连接的读写事件在同一个 `select()` 循环中继续被服务；
- 慢连接恢复读取后，队列按 FIFO 排空，收到的回显与发送字节序列完全一致；
- 对端半关闭（FIN）时先排空队列再关闭，保证最后的数据也完成回显。

单次唤醒的 `recv()` 有预算（32 次），防止某条突发连接长期占用唯一 I/O 线程。

### FD 布局与真实 EMFILE

低上限下典型 FD 布局为：

```text
0,1,2      stdin/stdout/stderr（stdin/stdout 是 JSON 控制管道）
3          同一个 TCP listener，整个故障期间不重建
4          预留 /dev/null 描述符（rescue fd）
5..25      20 条采样连接（保持 20 条时 open_fd_count=26，见实测）
26..31     样例日志文件（RLIMIT_NOFILE=32、表满时）
```

关键机制：

1. **真实 EMFILE**：I/O 线程在非阻塞 listener 上经 `select()` 等待；可读后调用真实 `accept()`。描述符表满时内核返回 `EMFILE`，工作进程只在这个 `except errno.EMFILE` 分支增加 `emfile_total`。
2. **旧连接不受影响**：已分配的连接 FD 与监听 FD 互不相关，多路复用循环继续 `recv/send`。
3. **新探测快速结束**：发生 EMFILE 后，I/O 线程关闭预留 FD 4，用腾出的唯一名额接受排队中的探测连接，然后立即 `close()`。不发送数据的探测收到 FIN/EOF；若探测已写入未读数据，Linux 可能直接发 RST。两者均满足要求。
4. **不空转**：处理完一个 EMFILE 后回到阻塞式 `select()`。不能在表满时立刻再次 `accept()`：Linux 可能在发现队列为空前先遇到 FD 上限并持续返回 EMFILE，造成假象忙等。无新连接时线程状态为 `S`，CPU 采样为 0。
5. **自动恢复**：API 关闭样例文件后腾出大量 FD；listener 仍是 FD 3，`select()`/`accept()` 路径无需重启即可接受新连接。
6. **故障期间可查询**：FastAPI 进程不受低 NOFILE 影响；工作进程的状态请求走已有 stdin/stdout FD。状态读取 `/proc/self/fd` 若自身遇到 EMFILE，会捕获并报告“描述符表已满”，不会导致状态接口失败。

## 本地无第三方依赖验证

工作进程有一个纯标准库集成检查：

```bash
python3 tests/worker_smoke.py
```

它会使用随机本地端口、`RLIMIT_NOFILE=32` 启动工作进程，并自动执行并发连接、线程上限、慢连接保序、EMFILE 探测、非忙等和恢复断言。若环境中有 pytest，也可运行 `pytest tests/test_emfile_worker.py`。

## 交付前实测结果

本次交付环境有 Python 3.11.2，无 Docker/Podman CLI 或容器 socket；因此实际试跑采用等价的本地 Uvicorn + 受管 echo worker（同样的 `DRILL_*` 环境变量，`RLIMIT_NOFILE=32`）。FastAPI/Uvicorn/pytest 被临时安装到 `/tmp/pydeps`（pip 引导于 `/tmp/pybootstrap`），未写入系统 Python。

已通过的关键结果：

- `python3 tests/worker_smoke.py`：`WORKER SELFTEST PASSED`
  - 7 条并发连接同时保持并回显；`thread_count=2`、上限 4；表满时仍可查询线程数
  - 慢连接停读期间回显进程为其单独排队（实测约 131 KiB），其他连接持续回显；恢复后 4 MiB 完整按序回显，队列归零
  - 表满 `open_fd_count=32`、`fd_table_full=true`；旧连接（含慢连接）故障中回显一致
  - 新探测：`eof` 约 `0.1 ms`（建连后计时，远小于 1 秒）
  - 单次探测后 `emfile_total=1`、`reaped_after_emfile_total=1`
  - 空闲故障 1 秒 I/O 线程 CPU：`0.0 ms`
  - release 后新连接回显成功，`listener_fd` 保持 3 不变
- `POST /api/v1/drill/self-test` 对真实 FastAPI/Uvicorn 试跑：`ok=true`，11 项检查全部为 `true`
  - 20 条并发连接：`active_connections=20`、`max_active_connections=20`、`open_fd_count=26`（NOFILE=32 下尚余 6 个名额）
  - 全程 `thread_count=2`（`fd-drill-control`、`fd-drill-network-io`），`within_thread_limit=true`
  - 慢连接 4 MiB：观察到发送队列约 262 KiB，其余 20 条连接全程回显；恢复后完整按序回显，队列归零
  - 探测 `outcome=eof`、`elapsed_after_connect_ms≈0.10`；EMFILE 计数增加；CPU 采样 `0.0s`、状态 `S`；恢复回显匹配；监听 FD 不变
- `python3 scripts/demo.py` 宿主机 TCP 演示：`DEMO PASSED`（含 20 并发查验、慢连接保序、EMFILE/EOF/非忙等/恢复）
- 额外突发测试（20 条旧连接保持、表满）：
  - 12 条并发探测全部在约 0.06–0.12 ms 内收到 EOF，`emfile_total`/`reaped` 各增加 12，线程数仍为 2
  - 带数据探测：`outcome=reset`、`errno=104 (ECONNRESET)`、约 `0.17 ms`
  - 故障期间 20 条旧连接全部继续回显
  - 连续 3 次 exhaust/release 循环，`listener_fd` 始终为 3，循环后新连接回显正常
- `pytest tests/`：`1 passed`；`docker-compose.yml` 已用 PyYAML 解析，服务、端口和环境变量结构有效；Python 文件全部通过 `py_compile`

## 未完成项与边界

- 本沙箱未安装 Docker/Podman，因此没有实际执行 `docker compose up --build`；已验证 Compose YAML、Dockerfile 依赖安装项和等价本地进程链路（含相同 NOFILE 环境变量）。首次在有 Docker 的环境运行时仍建议执行一次 `docker compose up --build`、`python3 scripts/demo.py` 和 `POST /api/v1/drill/self-test`。
- 这是本地演练工具，未配置认证、TLS 或多副本；不应暴露到不可信网络。
- 单 I/O 线程模型对演练规模（NOFILE=32、20+ 连接）足够且刻意简化；生产级高吞吐场景需要多反应器/worker 进程，不在本演练范围内。
- EMFILE 计数是工作进程生命周期内的累计值；调用 `/api/v1/drill/restart` 会重置。
- 样例日志文件默认保留在 Docker 命名卷 `drill_sample-logs` 中作为现场证据；如需清理可 `docker compose down -v`。
