# ChaosProxy + PhotoTask：统一代码实现蓝图与长期教学提示词

> 版本：V2.1，2026-09-13；从空工程实现版，加入 6 GiB 开发机资源约束。
> 用途：指导架构、核心代码教学、增量实现、代码审查、故障实验与面试准备。
> 输入依据：原 ChaosProxy + PhotoTask 统一蓝图；以用户提供的 PhotoBridge 代码蓝图为工程细度参考。本版保留业务与故障契约，重写目录、接口、构建和教学入口。
> 当前状态：本文是设计与验收契约；未审查真实仓库，不代表代码、测试或性能已经完成。
> 项目目标：亲自实现可靠的 C++ TCP 故障注入代理，再用照片任务服务证明缓存、事务和异步任务在依赖故障下的行为。

## 0. 阅读顺序与决策权限

第一次阅读：第 1～5 节建立地图；第 23 节查类函数，第 30 节查调用链；然后按第 24 节阶段推进。进入编码时只加载当前模块及直接依赖，不反复发送完整仓库。

本文统一规划两条主线：

- **ChaosProxy**：网络字节流转发、故障策略、运行时控制、故障轨迹与资源调度。
- **PhotoTask**：照片资料查询、照片处理任务、MySQL 权威数据、Redis 缓存、Nginx 多实例入口，以及 Kafka 异步分发。

PhotoTask 是配套业务服务，不等同于 PhotoBridge 迁移引擎。PhotoBridge 可以提供已接收的图片作为输入；本项目不复制它的迁移计划、关联图和迁移恢复实现。

优先级：用户最新明确要求 > 已确认设计决定 > 本文 > 原教学文档的冲突条款。真实仓库与测试结果决定“已经实现什么”。禁止把未验证的设计写进已完成状态。

本文中的 C++、SQL、配置和命令均标明为接口或实施模板。它们要在对应阶段补齐头文件、链接配置、错误分支和 migration 才构成可运行交付。不能复制几个片段就宣称系统已实现。

## 1. 最终项目定位与业务闭环

### 1.1 对外介绍

ChaosProxy 是运行在 Linux 上的 TCP 故障注入工具；PhotoTask 是它的实际验证业务。通过可编排的网络异常，检查照片服务的缓存降级、写入幂等、任务恢复和异步消息处理，并输出可复查的实验结果。

项目提供两个可以独立运行的成果：

1. 通用 TCP 代理，能服务 Echo、Redis、MySQL、HTTP 和 Kafka 等连接。
2. 照片任务可靠性实验环境，可一键部署、加载样例、执行场景和检查业务不变量。

### 1.2 照片业务范围

| 能力 | 用户操作 | 实际结果 |
|---|---|---|
| 导入照片 | CLI 从本地样例或 PhotoBridge 已完成目录导入 | 输入成为服务拥有的不可变文件，MySQL 登记 photo_id |
| 查询资料 | GET 照片信息 | 返回描述、标签、资料版本；默认经过 Redis |
| 修改资料 | PATCH 描述与标签 | MySQL 更新版本，同事务登记缓存失效事件 |
| 创建任务 | POST 缩略图或元数据提取任务 | 返回稳定 task_id，支持相同幂等键重试 |
| 查询进度 | GET 任务 | 从 MySQL 返回权威状态与失败原因 |
| 获取结果 | GET 成功任务的结果 | 从已选中的结果文件读取；不暴露未提交 attempt 文件 |
| 批量实验 | CLI 执行指定故障场景 | 输出步骤、注入确认、指标、断言和恢复结果 |

首批图片格式：JPEG、PNG。任务类型：THUMBNAIL、EXTRACT_METADATA。缩略图固定约束尺寸、格式和处理器版本；元数据保存原始字段来源，不推断未知 EXIF 时区。

最终计划不含人脸识别、模型推理、完整相册 UI、账号体系、云对象存储或公开互联网部署。个人测试环境使用固定 owner_scope 和简单 Bearer Token；这不是多租户权限系统。

### 1.3 三种重要失败

- Redis 超时：业务能否控制回源量，而非把故障压力转嫁给 MySQL？
- MySQL COMMIT 结果不确定：同一创建请求重试会不会生成两份任务？
- Kafka 重复消息与 Worker 重启：结果会不会重复发布，任务会不会永久卡住？

所有升级都至少对应一个失败场景和一个可观测断言。

## 2. 固定技术决策

| ID | 决定 | 理由与边界 |
|---|---|---|
| D01 | 单仓库、独立 target 和独立进程 | 部署方便；代理不链接 MySQL、Redis、Kafka 客户端 |
| D02 | 全工程统一 C++20 / Linux x86-64，从空仓库实现 | 不迁移旧模块，不保留 C++17 兼容分支 |
| D03 | ChaosProxy 自行实现 epoll EventLoop | 这是要训练和展示的核心能力 |
| D04 | PhotoTask 使用 Drogon HTTP 与异步数据库/Redis 客户端 | 复用协议、连接管理；自己实现事务、缓存策略和恢复语义 |
| D05 | MySQL/InnoDB 是业务权威状态 | Redis 和 Kafka 都不决定任务是否成功 |
| D06 | Redis Cache-Aside，仅缓存可接受短暂陈旧的照片资料 | 任务状态和幂等结果直接查 MySQL 主库 |
| D07 | 创建任务、幂等记录、Outbox 同一事务 | 避免数据库成功但消息意图丢失 |
| D08 | 先 MySQL 轮询 Worker，后 Kafka 分发 | 两种模式共享执行器、claim 和结果提交协议；Kafka 有完整后续阶段 |
| D09 | HTTP/场景配置用 JSON；任务事件用 Protobuf | 事件契约在 API/Dispatcher/Worker 间共享；不自研通用二进制 codec |
| D10 | Kafka 至少一次投递，业务结果幂等 | 不宣称 Kafka 自动保证 MySQL 和文件系统 exactly-once |
| D11 | 文件按 attempt 独立、不可变存储；MySQL CAS 选择结果 | 旧 Worker 可以留下孤儿文件，但不能成为业务可见结果 |
| D12 | Nginx 只承担入口与实例转发 | 不替代 ChaosProxy，不自动重试业务写请求 |
| D13 | UDS JSON 控制 + 场景执行器 | 不为控制面自建 Web 平台 |
| D14 | 公平调度、定时器精度、故障决策复现优先 | 提升故障工具本身的质量；多 Reactor 为后续性能阶段 |
| D15 | 编译器、vcpkg baseline、镜像摘要全部固定 | 本版已核验依赖解析；编译与服务兼容性另有验收，不凭空填性能数据 |

Drogon 官方提供数据库客户端及 Redis 支持；底层依赖和功能开关需在固定版本上验证。[安装说明](https://github.com/drogonframework/drogon/wiki/ENG-02-Installation)、[DbClient](https://github.com/drogonframework/drogon/wiki/ENG-08-1-Database-DbClient)

### 2.1 原文需要统一的条款

- “中间件不成为 ChaosProxy 依赖”保留；PhotoTask 可以依赖中间件。
- “只支持一个上游”改成每个命名 Proxy 固定一个上游；一个进程可管理多组监听，用于分别注入 Redis、MySQL 和 API 实例链路。
- “ET 必须无条件一直处理到 EAGAIN”改成：可以按预算让出，但必须记录就绪状态并主动续跑。
- “固定 seed 保证真实故障完全复现”改成：受控输入下复现决策；真实网络分块和调度另有边界。
- UDS 控制、配置版本、场景自动化升为业务实验必需能力。
- 本版初始化 A0.1 为未开始；后续只依据新仓库源码与实际验证更新进度。
- 不要求先完成所有可选性能优化才能开始业务；两条线按依赖交汇。

## 3. 部署关系与进程职责

### 3.1 数据路径

用户请求 → Nginx → 指定 API 链路的 ChaosProxy → PhotoTask API。

PhotoTask API → Redis 链路的 ChaosProxy → Redis。

PhotoTask API / Dispatcher / Worker → MySQL 链路的 ChaosProxy → MySQL。

Dispatcher / Kafka Worker → broker 对应的 ChaosProxy 监听 → Kafka broker。

Runner → UDS 控制 ChaosProxy；Runner → 测试 API 发请求；Runner → 独立观察连接查询数据库和指标。

观察连接不承载业务流量，仅用于断言。配置中显式区分 APP_* 与 OBSERVER_* 地址，防止业务悄悄绕过代理。

### 3.2 可执行程序

| 程序 | 职责 | 不承担 |
|---|---|---|
| chaosproxy | 多监听 TCP 转发、策略、定时、UDS 控制 | HTTP/MySQL/Kafka 业务解析 |
| chaosctl | 请求控制面、查询已应用版本 | 管理业务数据库 |
| photo-api | 查询、修改、幂等创建任务、结果读取 | 图像计算、Kafka 消费 |
| photo-admin | 导入 fixture、受控重试、查看隔离事件 | 通用数据库管理工具 |
| photo-dispatcher | Outbox claim、缓存失效、Kafka 发送、恢复调度 | 图像处理 |
| photo-worker | MySQL 轮询或 Kafka 获取任务，执行和提交结果 | HTTP 入口 |
| scenario-runner | 场景步骤、就绪屏障、断言、报告、清理 | 重写代理核心 |

首版同一 Linux 主机，容器使用命名卷共享 input/result；两个 API 实例是同机副本。跨主机部署必须先定义共享存储能力和访问协议，不能假定两台机器的 /data 是同一目录。

### 3.3 命名监听示例

| Proxy 名称 | 监听端口示例 | 固定上游 | 常见故障 |
|---|---:|---|---|
| redis-api | 16379 | redis:6379 | 慢缓存、单向暂停、RST |
| mysql-api | 13306 | mysql:3306 | 写入确认不确定、连接耗尽 |
| api-a | 18081 | photo-api-a:8080 | 单实例慢响应或断线 |
| api-b | 18082 | photo-api-b:8080 | 对照实例 |
| kafka-b1 | 19092 | kafka-b1:9092 | broker 客户端链路故障 |

这是隔离测试网络的示例端口，实施前检查冲突。Kafka 后续每个客户端可见 broker 都要配置代理映射。

## 4. 全新工程的目录、Target 与构建契约

### 4.1 起点与实现方式

本版从空仓库开始，统一 C++20 / Linux x86-64；不读取或迁移旧 ChaosProxy/PhotoBridge 的实现，不设置“旧代码审计、模块复用、兼容旧接口”的前置阶段。

代码由本文规定的接口与数据流重新实现。成熟第三方库仍通过 vcpkg 使用；“从零写项目”不等于手写 HTTP、MySQL、Redis、Kafka 和图片编解码协议。

目录是最终落点，按教学阶段逐步建立。每一组明确本次文件和函数，未到阶段不创建空类。新工程中的双方向转发、任务执行等同构流程各保留一份明确实现。

### 4.2 完整工程目录

工程根目录固定命名 `chaosproxy-phototask/`。下表与后面的 Target 源码列表、类接口目录对应。

```text
chaosproxy-phototask/
├── .gitignore
├── .gitmodules
├── CMakeLists.txt
├── CMakePresets.json
├── README.md
├── apps/
│   ├── CMakeLists.txt
│   ├── chaosctl_main.cpp
│   ├── chaosproxy_main.cpp
│   ├── photo_admin_main.cpp
│   ├── photo_api_main.cpp
│   ├── photo_dispatcher_main.cpp
│   └── photo_worker_main.cpp
├── benchmarks/
│   └── run_proxy.py
├── cmake/
│   └── project_options.cmake
├── configs/
│   ├── photo.json
│   └── proxy.json
├── deploy/
│   ├── compose.yaml
│   ├── images.env
│   ├── kafka.env.example
│   └── nginx.conf
├── docs/
│   ├── BUG_LEDGER.md
│   ├── DECISIONS.md
│   └── DEPENDENCIES.md
├── include/
│   ├── chaosproxy/
│   │   ├── common/
│   │   │   ├── clock.h
│   │   │   ├── completion.h
│   │   │   ├── logging.h
│   │   │   ├── status.h
│   │   │   ├── status_or.h
│   │   │   └── unique_fd.h
│   │   ├── control/
│   │   │   ├── control_client.h
│   │   │   ├── control_protocol.h
│   │   │   └── control_server.h
│   │   └── proxy/
│   │       ├── buffer.h
│   │       ├── config.h
│   │       ├── connection_pair.h
│   │       ├── connection_table.h
│   │       ├── event_loop.h
│   │       ├── proxy_server.h
│   │       ├── socket_ops.h
│   │       ├── timer_queue.h
│   │       ├── toxic_pipeline.h
│   │       ├── toxics.h
│   │       ├── trace_writer.h
│   │       └── types.h
│   └── phototask/
│       ├── application/
│       │   ├── dependency_guard.h
│       │   ├── outbox_dispatcher.h
│       │   ├── photo_service.h
│       │   ├── ports.h
│       │   ├── task_admission.h
│       │   ├── task_executor.h
│       │   └── task_service.h
│       ├── http/
│       │   └── http_handlers.h
│       ├── infra/
│       │   ├── filesystem_store.h
│       │   ├── image_processors.h
│       │   ├── mysql_store.h
│       │   ├── photo_runtime.h
│       │   └── redis_cache.h
│       ├── kafka/
│       │   ├── kafka_consumer.h
│       │   └── kafka_publisher.h
│       └── model/
│           ├── ids.h
│           ├── models.h
│           └── request_fingerprint.h
├── policies/
│   └── latency_200ms.json
├── proto/
│   ├── CMakeLists.txt
│   └── phototask/
│       └── v1/
│           └── task_event.proto
├── runs
├── scenarios/
│   ├── kafka_duplicate.json
│   ├── mysql_commit_unknown.json
│   └── redis_slow.json
├── sql/
│   └── migrations/
│       ├── 001_core.sql
│       ├── 002_cache.sql
│       └── 003_inbox.sql
├── src/
│   ├── CMakeLists.txt
│   ├── common/
│   │   ├── logging.cpp
│   │   └── status.cpp
│   ├── control/
│   │   ├── control_client.cpp
│   │   ├── control_protocol.cpp
│   │   └── control_server.cpp
│   ├── phototask/
│   │   ├── application/
│   │   │   ├── dependency_guard.cpp
│   │   │   ├── outbox_dispatcher.cpp
│   │   │   ├── photo_service.cpp
│   │   │   ├── task_admission.cpp
│   │   │   ├── task_executor.cpp
│   │   │   └── task_service.cpp
│   │   ├── http/
│   │   │   └── http_handlers.cpp
│   │   ├── infra/
│   │   │   ├── filesystem_store.cpp
│   │   │   ├── image_processors.cpp
│   │   │   ├── mysql_store.cpp
│   │   │   ├── photo_runtime.cpp
│   │   │   └── redis_cache.cpp
│   │   ├── kafka/
│   │   │   ├── kafka_consumer.cpp
│   │   │   └── kafka_publisher.cpp
│   │   └── model/
│   │       ├── models.cpp
│   │       └── request_fingerprint.cpp
│   └── proxy/
│       ├── buffer.cpp
│       ├── config.cpp
│       ├── connection_pair.cpp
│       ├── connection_table.cpp
│       ├── event_loop.cpp
│       ├── proxy_server.cpp
│       ├── socket_ops.cpp
│       ├── timer_queue.cpp
│       ├── toxic_pipeline.cpp
│       ├── toxics.cpp
│       └── trace_writer.cpp
├── state.md
├── tests/
│   ├── CMakeLists.txt
│   ├── fixtures/
│   │   └── photos/
│   │       └── manifest.json
│   ├── helpers/
│   │   ├── echo_server.h
│   │   ├── fake_clock.h
│   │   └── fake_file_store.h
│   ├── integration/
│   │   ├── kafka/
│   │   │   └── admission_test.cpp
│   │   ├── phototask/
│   │   │   ├── cache_race_test.cpp
│   │   │   └── create_task_test.cpp
│   │   └── proxy/
│   │       └── half_close_test.cpp
│   └── unit/
│       ├── common/
│       │   └── status_test.cpp
│       ├── phototask/
│       │   └── task_state_test.cpp
│       └── proxy/
│           ├── buffer_test.cpp
│           ├── pipeline_test.cpp
│           └── timer_queue_test.cpp
├── third_party/
│   └── vcpkg
├── tools/
│   └── scenarios/
│       ├── observers.py
│       ├── runner.py
│       └── schema.json
└── vcpkg.json
```

`include/chaosproxy/common` 属于轻量基础库，命名空间 `chaosproxy`；`phototask` 显式使用其中的 Status、StatusOr、UniqueFd，不因此依赖网络代理运行时。其他业务模型和类放 `namespace phototask`。所有 C++ 项目源码都使用 C++20。

源文件列表是最终模板。A0.1 先创建可运行 main，A0.2～A0.6 逐步建立基础库与测试；B、C、D 阶段依次增加其余源文件，不能第一天要求不存在的文件参与编译。

### 4.3 Target 与依赖方向

| Target | 源码职责 | 允许链接 |
|---|---|---|
| cp_common | Status、日志、fd、时间与摘要基础 | spdlog、Threads；不含 SQL/HTTP |
| chaosproxy_core | Socket、模型、EventLoop、队列、Timer、策略、ProxyServer | cp_common、JsonCpp |
| chaosproxy_control | 控制解析、UDS 服务端/客户端 | chaosproxy_core、JsonCpp |
| phototask_events | protoc 生成的任务事件 | protobuf::libprotobuf |
| phototask_core | 领域模型、指纹、用例、任务/Outbox/准入协议 | cp_common、phototask_events、JsonCpp、OpenSSL::Crypto |
| phototask_infra | MySQL/Redis、文件与图片适配、runtime 组装 | phototask_core、Drogon、图像库 |
| phototask_http | HTTP 校验、路由、响应映射 | phototask_infra、Drogon |
| phototask_kafka | Producer/Consumer 与位点处理 | phototask_core、RdKafka::rdkafka |
| 各 app | 参数、依赖组装、启动/退出 | 对应最上层 Target、CLI11 |

领域模型不持有 Drogon Request、MYSQL*、redisContext*、rd_kafka_t*。外部协议类型止于 adapter 边界。`cp_common` 不是任意功能的垃圾桶，不能把缓存、数据库事务或业务 schema 放进去。

### 4.4 已核验的 vcpkg 身份

- 官方仓库：`https://github.com/microsoft/vcpkg.git`
- 发布标签：`2026.07.29`
- **Git commit：`9e593bb18ea69cc5095e012465dcd675a822ed0d`**
- **builtin-baseline：`9e593bb18ea69cc5095e012465dcd675a822ed0d`**
- 该提交的工具发布：`2026-07-27`
- 本轮实际 bootstrap 输出：`2026-07-27-98d7cb0cf1f4686a3e43aa5672b6230c1d56bce8`
- Target/Host triplet：均为 `x64-linux`；默认静态 C++ 库，但不等于生成完全静态 Linux 可执行文件。

Git submodule 固定的是 vcpkg 仓库树；baseline 控制内建版本解析基线；overrides 固定本文列出的关键包精确版本。baseline 是版本下界规则，不能把它单独解释成任何输入下都不会变化的完整 lockfile。

版本证据来自固定提交的 [baseline.json](https://github.com/microsoft/vcpkg/blob/9e593bb18ea69cc5095e012465dcd675a822ed0d/versions/baseline.json)、[ports](https://github.com/microsoft/vcpkg/tree/9e593bb18ea69cc5095e012465dcd675a822ed0d/ports) 与 [tool metadata](https://github.com/microsoft/vcpkg/blob/9e593bb18ea69cc5095e012465dcd675a822ed0d/scripts/vcpkg-tool-metadata.txt)。版本规则参见 [vcpkg 官方说明](https://learn.microsoft.com/en-us/vcpkg/users/versioning)。

### 4.5 第三方库精确版本与用途

| vcpkg port | 精确包版本 | 用途 |
|---|---|---|
| cli11 | `2.6.2` | 命令行解析 |
| spdlog | `1.17.0#1` | 日志 |
| fmt | `12.2.0#1` | spdlog 格式化依赖 |
| jsoncpp | `1.9.6` | 统一 JSON 配置与 HTTP 边界 |
| gtest | `1.17.0#3` | 测试 |
| drogon | `1.9.13#1` | HTTP/异步 MySQL/Redis |
| trantor | `1.5.28` | Drogon 网络依赖 |
| libmariadb | `3.4.8` | MySQL 兼容客户端 |
| hiredis | `1.3.0` | Redis 客户端依赖 |
| protobuf | `6.33.4#2` | 任务事件/生成器 |
| openssl | `3.6.3` | SHA-256 与客户端 TLS |
| libjpeg-turbo | `3.2.0` | JPEG 编解码 |
| libpng | `1.6.58` | PNG 编解码 |
| exiv2 | `0.28.8` | 元数据读取 |
| librdkafka | `2.14.2` | Kafka C 客户端 |
| abseil | `20260107.1#3` | Protobuf 依赖 |
| zlib | `1.3.2#1` | 压缩依赖 |


`#N` 是 vcpkg port-version，即同一上游版本的打包修订，不是上游库补丁版本。不要省略带 # 的部分后声称完全一致。

Protobuf 有两套可见版本编号：本 baseline 的包版本为 `6.33.4#2`，port 拉取上游 `v33.4`。不能因此把 protoc 的版本输出与 vcpkg 包版本判定为不匹配；生成器和 runtime 均来自同一固定 port，并检查生成文件头中的版本约束。

Drogon 启用 `mysql`、`redis` 时自动带入 `orm`，MySQL 客户端由 `libmariadb` 提供，服务端仍是 MySQL。不能把 `libmariadb 3.4.8` 写成 MySQL Server 版本。

### 4.6 完整 vcpkg.json

根目录使用以下 Manifest。`tests`、`photo`、`kafka` 为项目 feature，由 Preset 选择；只构建代理时不会拉取 Drogon/Kafka。

```json
{
  "name": "chaosproxy-phototask",
  "version-string": "0.1.0",
  "builtin-baseline": "9e593bb18ea69cc5095e012465dcd675a822ed0d",
  "dependencies": [
    "cli11",
    "spdlog",
    "jsoncpp"
  ],
  "features": {
    "tests": {
      "description": "GoogleTest tests",
      "dependencies": [
        "gtest"
      ]
    },
    "photo": {
      "description": "PhotoTask HTTP MySQL Redis and processors",
      "dependencies": [
        {
          "name": "drogon",
          "default-features": false,
          "features": [
            "mysql",
            "redis"
          ]
        },
        "protobuf",
        "openssl",
        "libjpeg-turbo",
        "libpng",
        {
          "name": "exiv2",
          "default-features": false,
          "features": [
            "png",
            "xmp"
          ]
        }
      ]
    },
    "kafka": {
      "description": "Kafka transport adapter",
      "dependencies": [
        {
          "name": "librdkafka",
          "default-features": false,
          "features": [
            "ssl",
            "zlib",
            "zstd"
          ]
        }
      ]
    }
  },
  "overrides": [
    {
      "name": "cli11",
      "version": "2.6.2"
    },
    {
      "name": "spdlog",
      "version-semver": "1.17.0",
      "port-version": 1
    },
    {
      "name": "fmt",
      "version": "12.2.0",
      "port-version": 1
    },
    {
      "name": "jsoncpp",
      "version": "1.9.6"
    },
    {
      "name": "gtest",
      "version-semver": "1.17.0",
      "port-version": 3
    },
    {
      "name": "drogon",
      "version-semver": "1.9.13",
      "port-version": 1
    },
    {
      "name": "trantor",
      "version-semver": "1.5.28"
    },
    {
      "name": "libmariadb",
      "version-semver": "3.4.8"
    },
    {
      "name": "hiredis",
      "version": "1.3.0"
    },
    {
      "name": "protobuf",
      "version": "6.33.4",
      "port-version": 2
    },
    {
      "name": "openssl",
      "version": "3.6.3"
    },
    {
      "name": "libjpeg-turbo",
      "version": "3.2.0"
    },
    {
      "name": "libpng",
      "version": "1.6.58"
    },
    {
      "name": "exiv2",
      "version": "0.28.8"
    },
    {
      "name": "librdkafka",
      "version": "2.14.2"
    },
    {
      "name": "abseil",
      "version": "20260107.1",
      "port-version": 3
    },
    {
      "name": "zlib",
      "version": "1.3.2",
      "port-version": 1
    }
  ]
}
```

`photo` 中明确启用 Drogon 的 mysql/redis、Exiv2 的 png/xmp；Kafka 启用 ssl/zlib/zstd。没有加入 gRPC、PostgreSQL、SQLite、YAML 或 BMFF 解码。

### 4.7 初始化与版本检查命令

以下命令针对新目录，按顺序执行。已有用户仓库不要直接删除重建；本文不授权删除旧项目。

```bash
mkdir chaosproxy-phototask
cd chaosproxy-phototask
git init
git submodule add https://github.com/microsoft/vcpkg.git third_party/vcpkg
git -C third_party/vcpkg checkout --detach 9e593bb18ea69cc5095e012465dcd675a822ed0d
./third_party/vcpkg/bootstrap-vcpkg.sh -disableMetrics
git -C third_party/vcpkg rev-parse HEAD
./third_party/vcpkg/vcpkg version
git add .gitmodules third_party/vcpkg
```

应提交 `.gitmodules` 与 submodule gitlink，而非第三方源码和 installed/buildtrees。新机器：`git clone --recurse-submodules <你的仓库地址>`，或已有 checkout 执行 `git submodule update --init --recursive`。

Ubuntu 24.04 x86-64 为默认开发系统；使用 GCC 13 系列、Ninja、CMake ≥3.25、Python 3。系统工具可通过 apt 安装，C++ 依赖使用 Manifest。系统工具补丁版本、glibc、内核与编译器实际输出记录到环境报告；本文不虚构用户机器当前 patch 版本。

```bash
sudo apt update
sudo apt install build-essential gcc-13 g++-13 cmake ninja-build git curl zip unzip tar pkg-config autoconf automake libtool nasm python3 python3-venv
g++-13 --version
cmake --version
ninja --version
```

CMake 工程与依赖内部构建工具分别管理：本轮 vcpkg dry-run 自动下载了它要求的 CMake 4.4.0，这不等于工程的 `cmake_minimum_required` 必须改成 4.4。bootstrap/tool 下载校验由该提交的 metadata 和工具代码控制。

### 4.8 CMakePresets.json

每个 Preset 使用独立 build 与 vcpkg_installed，防止 feature 切换互相污染。默认 jobs=2 是内存保守起点，用户可在本机按可用资源调整。

```json
{
  "version": 6,
  "cmakeMinimumRequired": {
    "major": 3,
    "minor": 25,
    "patch": 0
  },
  "configurePresets": [
    {
      "name": "base",
      "hidden": true,
      "generator": "Ninja",
      "binaryDir": "${sourceDir}/build/${presetName}",
      "cacheVariables": {
        "CMAKE_TOOLCHAIN_FILE": "${sourceDir}/third_party/vcpkg/scripts/buildsystems/vcpkg.cmake",
        "VCPKG_TARGET_TRIPLET": "x64-linux",
        "VCPKG_HOST_TRIPLET": "x64-linux",
        "VCPKG_INSTALLED_DIR": "${sourceDir}/build/${presetName}/vcpkg_installed",
        "CMAKE_EXPORT_COMPILE_COMMANDS": "ON",
        "BUILD_TESTING": "ON",
        "CP_BUILD_PHOTO": "OFF",
        "CP_BUILD_KAFKA": "OFF",
        "CP_SANITIZER": "none",
        "VCPKG_MANIFEST_NO_DEFAULT_FEATURES": "ON",
        "CMAKE_C_COMPILER": "/usr/bin/gcc-13",
        "CMAKE_CXX_COMPILER": "/usr/bin/g++-13"
      }
    },
    {
      "name": "proxy-debug",
      "inherits": "base",
      "cacheVariables": {
        "CMAKE_BUILD_TYPE": "Debug",
        "CP_BUILD_PHOTO": "OFF",
        "CP_BUILD_KAFKA": "OFF",
        "CP_SANITIZER": "none",
        "VCPKG_MANIFEST_FEATURES": "tests"
      }
    },
    {
      "name": "proxy-release",
      "inherits": "base",
      "cacheVariables": {
        "CMAKE_BUILD_TYPE": "Release",
        "CP_BUILD_PHOTO": "OFF",
        "CP_BUILD_KAFKA": "OFF",
        "CP_SANITIZER": "none",
        "VCPKG_MANIFEST_FEATURES": "tests"
      }
    },
    {
      "name": "proxy-asan",
      "inherits": "base",
      "cacheVariables": {
        "CMAKE_BUILD_TYPE": "Debug",
        "CP_BUILD_PHOTO": "OFF",
        "CP_BUILD_KAFKA": "OFF",
        "CP_SANITIZER": "address-undefined",
        "VCPKG_MANIFEST_FEATURES": "tests"
      }
    },
    {
      "name": "photo-debug",
      "inherits": "base",
      "cacheVariables": {
        "CMAKE_BUILD_TYPE": "Debug",
        "CP_BUILD_PHOTO": "ON",
        "CP_BUILD_KAFKA": "OFF",
        "CP_SANITIZER": "none",
        "VCPKG_MANIFEST_FEATURES": "tests;photo"
      }
    },
    {
      "name": "photo-release",
      "inherits": "base",
      "cacheVariables": {
        "CMAKE_BUILD_TYPE": "Release",
        "CP_BUILD_PHOTO": "ON",
        "CP_BUILD_KAFKA": "OFF",
        "CP_SANITIZER": "none",
        "VCPKG_MANIFEST_FEATURES": "tests;photo"
      }
    },
    {
      "name": "kafka-debug",
      "inherits": "base",
      "cacheVariables": {
        "CMAKE_BUILD_TYPE": "Debug",
        "CP_BUILD_PHOTO": "ON",
        "CP_BUILD_KAFKA": "ON",
        "CP_SANITIZER": "none",
        "VCPKG_MANIFEST_FEATURES": "tests;photo;kafka"
      }
    },
    {
      "name": "kafka-release",
      "inherits": "base",
      "cacheVariables": {
        "CMAKE_BUILD_TYPE": "Release",
        "CP_BUILD_PHOTO": "ON",
        "CP_BUILD_KAFKA": "ON",
        "CP_SANITIZER": "none",
        "VCPKG_MANIFEST_FEATURES": "tests;photo;kafka"
      }
    },
    {
      "name": "kafka-tsan",
      "inherits": "base",
      "cacheVariables": {
        "CMAKE_BUILD_TYPE": "Debug",
        "CP_BUILD_PHOTO": "ON",
        "CP_BUILD_KAFKA": "ON",
        "CP_SANITIZER": "thread",
        "VCPKG_MANIFEST_FEATURES": "tests;photo;kafka"
      }
    }
  ],
  "buildPresets": [
    {
      "name": "proxy-debug",
      "configurePreset": "proxy-debug",
      "jobs": 2
    },
    {
      "name": "proxy-release",
      "configurePreset": "proxy-release",
      "jobs": 2
    },
    {
      "name": "proxy-asan",
      "configurePreset": "proxy-asan",
      "jobs": 2
    },
    {
      "name": "photo-debug",
      "configurePreset": "photo-debug",
      "jobs": 2
    },
    {
      "name": "photo-release",
      "configurePreset": "photo-release",
      "jobs": 2
    },
    {
      "name": "kafka-debug",
      "configurePreset": "kafka-debug",
      "jobs": 2
    },
    {
      "name": "kafka-release",
      "configurePreset": "kafka-release",
      "jobs": 2
    },
    {
      "name": "kafka-tsan",
      "configurePreset": "kafka-tsan",
      "jobs": 2
    }
  ],
  "testPresets": [
    {
      "name": "proxy-debug",
      "configurePreset": "proxy-debug",
      "output": {
        "outputOnFailure": true
      },
      "execution": {
        "noTestsAction": "error"
      }
    },
    {
      "name": "proxy-release",
      "configurePreset": "proxy-release",
      "output": {
        "outputOnFailure": true
      },
      "execution": {
        "noTestsAction": "error"
      }
    },
    {
      "name": "proxy-asan",
      "configurePreset": "proxy-asan",
      "output": {
        "outputOnFailure": true
      },
      "execution": {
        "noTestsAction": "error"
      }
    },
    {
      "name": "photo-debug",
      "configurePreset": "photo-debug",
      "output": {
        "outputOnFailure": true
      },
      "execution": {
        "noTestsAction": "error"
      }
    },
    {
      "name": "photo-release",
      "configurePreset": "photo-release",
      "output": {
        "outputOnFailure": true
      },
      "execution": {
        "noTestsAction": "error"
      }
    },
    {
      "name": "kafka-debug",
      "configurePreset": "kafka-debug",
      "output": {
        "outputOnFailure": true
      },
      "execution": {
        "noTestsAction": "error"
      }
    },
    {
      "name": "kafka-release",
      "configurePreset": "kafka-release",
      "output": {
        "outputOnFailure": true
      },
      "execution": {
        "noTestsAction": "error"
      }
    },
    {
      "name": "kafka-tsan",
      "configurePreset": "kafka-tsan",
      "output": {
        "outputOnFailure": true
      },
      "execution": {
        "noTestsAction": "error"
      }
    }
  ]
}
```

默认编译器固定 `/usr/bin/gcc-13` 和 `/usr/bin/g++-13`，需要先安装；若明确切换 Clang，使用新的 preset/build 目录并重新记录依赖 ABI，不能在旧缓存上覆盖编译器。

注意：`CP_BUILD_KAFKA=ON` 必须同时 `CP_BUILD_PHOTO=ON`，并在 configure 前选择 vcpkg 的 photo+kafka feature。仅在配置之后切换 CMake 开关不会替你安装缺失依赖。

### 4.9 CMake 具体模板

下面是达到完整目录后的最终模板；教学时按新增文件同步补齐 target_sources，不生成空源码凑齐列表。

**根 CMakeLists.txt**

```cmake
cmake_minimum_required(VERSION 3.25)
project(chaosproxy_phototask VERSION 0.1.0 LANGUAGES C CXX)
if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
  message(FATAL_ERROR "This blueprint targets Linux")
endif()
set(CMAKE_CXX_EXTENSIONS OFF)
option(CP_BUILD_PHOTO "Build PhotoTask programs" OFF)
option(CP_BUILD_KAFKA "Build Kafka transport" OFF)
option(CP_WARNINGS_AS_ERRORS "Treat project warnings as errors" OFF)
set(CP_SANITIZER "none" CACHE STRING "none/address-undefined/thread")
if(CP_BUILD_KAFKA AND NOT CP_BUILD_PHOTO)
  message(FATAL_ERROR "CP_BUILD_KAFKA requires CP_BUILD_PHOTO")
endif()
include(cmake/project_options.cmake)
include(CTest)
find_package(Threads REQUIRED)
find_package(CLI11 CONFIG REQUIRED)
find_package(spdlog CONFIG REQUIRED)
find_package(jsoncpp CONFIG REQUIRED)
if(CP_BUILD_PHOTO)
  add_subdirectory(proto)
endif()
add_subdirectory(src)
add_subdirectory(apps)
if(BUILD_TESTING)
  add_subdirectory(tests)
endif()
```

**cmake/project_options.cmake**

```cmake
function(cp_project_target target)
  target_compile_features(${target} PUBLIC cxx_std_20)
  target_compile_options(${target} PRIVATE
    -Wall -Wextra -Wpedantic -Wconversion -Wshadow)
  if(CP_WARNINGS_AS_ERRORS)
    target_compile_options(${target} PRIVATE -Werror)
  endif()
  if(CP_SANITIZER STREQUAL "address-undefined")
    target_compile_options(${target} PRIVATE
      -fsanitize=address,undefined -fno-omit-frame-pointer)
    target_link_options(${target} PRIVATE -fsanitize=address,undefined)
  elseif(CP_SANITIZER STREQUAL "thread")
    target_compile_options(${target} PRIVATE
      -fsanitize=thread -fno-omit-frame-pointer)
    target_link_options(${target} PRIVATE -fsanitize=thread)
  elseif(NOT CP_SANITIZER STREQUAL "none")
    message(FATAL_ERROR "Unknown CP_SANITIZER=${CP_SANITIZER}")
  endif()
endfunction()
```

项目自有 target 调用 cp_project_target；第三方与 protoc 生成代码不启用项目专属 Werror。生成代码也不直接计入手写核心代码预算。ASan/UBSan 与 TSan 单独运行，不同时启用。

**src/CMakeLists.txt**

```cmake
add_library(cp_common STATIC
  common/status.cpp
  common/logging.cpp
)
cp_project_target(cp_common)
target_include_directories(cp_common PUBLIC "${PROJECT_SOURCE_DIR}/include")
target_link_libraries(cp_common PUBLIC spdlog::spdlog Threads::Threads)

add_library(chaosproxy_core STATIC
  proxy/socket_ops.cpp
  proxy/config.cpp
  proxy/buffer.cpp
  proxy/timer_queue.cpp
  proxy/connection_table.cpp
  proxy/connection_pair.cpp
  proxy/event_loop.cpp
  proxy/toxic_pipeline.cpp
  proxy/toxics.cpp
  proxy/proxy_server.cpp
  proxy/trace_writer.cpp
)
cp_project_target(chaosproxy_core)
target_include_directories(chaosproxy_core PUBLIC "${PROJECT_SOURCE_DIR}/include")
target_link_libraries(chaosproxy_core PUBLIC cp_common PRIVATE JsonCpp::JsonCpp)

add_library(chaosproxy_control STATIC
  control/control_protocol.cpp
  control/control_server.cpp
  control/control_client.cpp
)
cp_project_target(chaosproxy_control)
target_include_directories(chaosproxy_control PUBLIC "${PROJECT_SOURCE_DIR}/include")
target_link_libraries(chaosproxy_control PUBLIC chaosproxy_core PRIVATE JsonCpp::JsonCpp)

if(CP_BUILD_PHOTO)
  find_package(Drogon CONFIG REQUIRED)
  find_package(OpenSSL REQUIRED)
  find_package(JPEG REQUIRED)
  find_package(PNG REQUIRED)
  find_package(exiv2 CONFIG REQUIRED)
  add_library(phototask_core STATIC
    phototask/model/models.cpp
    phototask/model/request_fingerprint.cpp
    phototask/application/photo_service.cpp
    phototask/application/task_service.cpp
    phototask/application/dependency_guard.cpp
    phototask/application/outbox_dispatcher.cpp
    phototask/application/task_admission.cpp
    phototask/application/task_executor.cpp
  )
  cp_project_target(phototask_core)
  target_include_directories(phototask_core PUBLIC "${PROJECT_SOURCE_DIR}/include")
  target_link_libraries(phototask_core PUBLIC cp_common phototask_events PRIVATE JsonCpp::JsonCpp OpenSSL::Crypto)
  add_library(phototask_infra STATIC
    phototask/infra/mysql_store.cpp
    phototask/infra/redis_cache.cpp
    phototask/infra/filesystem_store.cpp
    phototask/infra/image_processors.cpp
    phototask/infra/photo_runtime.cpp
  )
  cp_project_target(phototask_infra)
  target_include_directories(phototask_infra PUBLIC "${PROJECT_SOURCE_DIR}/include")
  target_link_libraries(phototask_infra PUBLIC phototask_core Drogon::Drogon PRIVATE JPEG::JPEG PNG::PNG Exiv2::exiv2lib)
  add_library(phototask_http STATIC
    phototask/http/http_handlers.cpp
  )
  cp_project_target(phototask_http)
  target_include_directories(phototask_http PUBLIC "${PROJECT_SOURCE_DIR}/include")
  target_link_libraries(phototask_http PUBLIC phototask_infra Drogon::Drogon)
endif()

if(CP_BUILD_KAFKA)
  find_package(RdKafka CONFIG REQUIRED)
  add_library(phototask_kafka STATIC
    phototask/kafka/kafka_publisher.cpp
    phototask/kafka/kafka_consumer.cpp
  )
  cp_project_target(phototask_kafka)
  target_include_directories(phototask_kafka PUBLIC "${PROJECT_SOURCE_DIR}/include")
  target_link_libraries(phototask_kafka PUBLIC phototask_core PRIVATE RdKafka::rdkafka)
endif()
```

**proto/CMakeLists.txt**

```cmake
find_package(Protobuf CONFIG REQUIRED)
set(CP_GENERATED_DIR "${PROJECT_BINARY_DIR}/generated")
file(MAKE_DIRECTORY "${CP_GENERATED_DIR}")
add_library(phototask_events STATIC phototask/v1/task_event.proto)
protobuf_generate(
  TARGET phototask_events
  LANGUAGE cpp
  IMPORT_DIRS "${CMAKE_CURRENT_SOURCE_DIR}"
  PROTOC_OUT_DIR "${CP_GENERATED_DIR}"
)
target_include_directories(phototask_events PUBLIC "${CP_GENERATED_DIR}")
target_link_libraries(phototask_events PUBLIC protobuf::libprotobuf)
target_compile_features(phototask_events PUBLIC cxx_std_20)
```

生成头文件用 `#include "phototask/v1/task_event.pb.h"`。禁止调用系统 PATH 中另一个版本的 protoc；使用所导入包的 `protobuf::protoc`。本版只声明 native x64-linux 构建，交叉编译另开设计记录。

**apps/CMakeLists.txt**

```cmake
add_executable(chaosproxy chaosproxy_main.cpp)
cp_project_target(chaosproxy)
target_link_libraries(chaosproxy PRIVATE chaosproxy_control CLI11::CLI11)
add_executable(chaosctl chaosctl_main.cpp)
cp_project_target(chaosctl)
target_link_libraries(chaosctl PRIVATE chaosproxy_control CLI11::CLI11)
if(CP_BUILD_PHOTO)
  add_executable(photo-api photo_api_main.cpp)
  add_executable(photo-admin photo_admin_main.cpp)
  add_executable(photo-dispatcher photo_dispatcher_main.cpp)
  add_executable(photo-worker photo_worker_main.cpp)
  target_link_libraries(photo-api PRIVATE phototask_http)
  foreach(app IN ITEMS photo-api photo-admin photo-dispatcher photo-worker)
    cp_project_target(${app})
    target_link_libraries(${app} PRIVATE phototask_infra CLI11::CLI11)
  endforeach()
  if(CP_BUILD_KAFKA)
    foreach(app IN ITEMS photo-dispatcher photo-worker)
      target_link_libraries(${app} PRIVATE phototask_kafka)
      target_compile_definitions(${app} PRIVATE CP_HAS_KAFKA=1)
    endforeach()
  endif()
endif()
```

**tests/CMakeLists.txt：首组可编译形状**

```cmake
find_package(GTest CONFIG REQUIRED)
include(GoogleTest)
add_executable(common_tests unit/common/status_test.cpp)
cp_project_target(common_tests)
target_link_libraries(common_tests PRIVATE cp_common GTest::gtest_main)
gtest_discover_tests(common_tests DISCOVERY_MODE PRE_TEST)
# 随后按阶段显式增加 proxy_tests、phototask_tests 和集成测试 target。
# 不使用 GLOB 把不属于当前阶段的实验代码自动加入构建。
```

### 4.10 find_package 与 target 对照

| 库 | 查找语句 | 本项目使用的 imported target |
|---|---|---|
| CLI11 | find_package(CLI11 CONFIG REQUIRED) | CLI11::CLI11 |
| spdlog | find_package(spdlog CONFIG REQUIRED) | spdlog::spdlog |
| JsonCpp | find_package(jsoncpp CONFIG REQUIRED) | JsonCpp::JsonCpp |
| GoogleTest | find_package(GTest CONFIG REQUIRED) | GTest::gtest_main |
| Drogon | find_package(Drogon CONFIG REQUIRED) | Drogon::Drogon |
| Protobuf | find_package(Protobuf CONFIG REQUIRED) | protobuf::libprotobuf / protobuf::protoc |
| OpenSSL | find_package(OpenSSL REQUIRED) | OpenSSL::Crypto |
| libjpeg-turbo | find_package(JPEG REQUIRED) | JPEG::JPEG，使用 libjpeg C API |
| libpng | find_package(PNG REQUIRED) | PNG::PNG |
| Exiv2 | find_package(exiv2 CONFIG REQUIRED) | Exiv2::exiv2lib |
| librdkafka | find_package(RdKafka CONFIG REQUIRED) | RdKafka::rdkafka，C API |

所有名称已对照该 commit 的 usage/portfile 或固定上游导出定义；这不等于已经在用户机器完成全库链接。Drogon/Exiv2 等确切头文件 API 在相应阶段通过真实编译验收。

### 4.11 已执行的验证与后续门槛

本轮实际完成：官方 tag/commit 对应查询；固定版本浅克隆；bootstrap；读取 port feature 和版本；对完整 Manifest 执行 x64-linux dry-run，成功解析依赖图。

原始解析结果：

```text
The following packages will be built and installed:
  * abseil:x64-linux@20260107.1#3
  * brotli:x64-linux@1.2.0
  * c-ares:x64-linux@1.34.8
    cli11:x64-linux@2.6.2
    drogon[core,mysql,orm,redis]:x64-linux@1.9.13#1
    exiv2[core,png,xmp]:x64-linux@0.28.8
  * expat:x64-linux@2.8.2
  * fmt:x64-linux@12.2.0#1
    gtest:x64-linux@1.17.0#3
  * hiredis:x64-linux@1.3.0
  * inih[core,cpp]:x64-linux@62#1
    jsoncpp:x64-linux@1.9.6
  * libiconv:x64-linux@1.19
    libjpeg-turbo:x64-linux@3.2.0
  * libmariadb[core,openssl]:x64-linux@3.4.8
    libpng:x64-linux@1.6.58
    librdkafka[core,ssl,zlib,zstd]:x64-linux@2.14.2
  * libuuid:x64-linux@1.0.3#17
  * lz4:x64-linux@1.10.0
    openssl:x64-linux@3.6.3
    protobuf:x64-linux@6.33.4#2
    spdlog[core,fmt,tz-offset]:x64-linux@1.17.0#1
  * trantor:x64-linux@1.5.28
  * utf8-range:x64-linux@6.33.4
  * vcpkg-cmake:x64-linux@2024-04-23
  * vcpkg-cmake-config:x64-linux@2026-07-21
  * vcpkg-cmake-get-vars:x64-linux@2025-05-29
  * vcpkg-make:x64-linux@2026-07-09
  * vcpkg-tool-meson:x64-linux@1.9.0#10
  * zlib:x64-linux@1.3.2#1
  * zstd:x64-linux@1.5.7
Additional packages (*) will be modified to complete this operation.
```

本轮未执行：全部依赖编译安装、项目完整 configure/link、MySQL/Redis/Kafka 启动或端到端测试。不能把 dry-run 说成“已编译通过”。

新仓库写入 Manifest/Presets 后，先运行：

```bash
./third_party/vcpkg/vcpkg install --dry-run --triplet=x64-linux --host-triplet=x64-linux --x-feature=tests --x-feature=photo --x-feature=kafka
cmake --list-presets
cmake --preset proxy-debug
cmake --build --preset proxy-debug
ctest --preset proxy-debug
```

photo-debug、kafka-debug 随阶段切换。每个里程碑保存 vcpkg list、编译器/CMake/Ninja 版本和镜像摘要。升级 baseline 是独立改动，不与连接状态或事务协议重构混在一起。

### 4.12 服务版本与 vcpkg 的边界

vcpkg 安装的是客户端/编解码库，不能安装出 MySQL、Redis、Nginx、Kafka 服务端集群。服务端通过独立容器镜像运行：

| 服务 | 固定候选 tag | 查询结果 / registry digest |
|---|---|---|
| library/mysql | `8.4.7` | `sha256:0426ec38c7a10aa45ba383887df7878f74ee70e2fd589c7b69207f3577901903` |
| library/redis | `7.4.7` | `sha256:976ab0f0939b78c06d802bcd3bea44c5142ea9a756a57b861e08819817f768c8` |
| library/nginx | `1.28.2` | `sha256:42e026ae5315aa0deec22fb00c364fc5ec8d9af1c4833ad5317e2a433e4de0df` |
| apache/kafka | `4.1.1` | `sha256:0bc1bb2478f45b6cea78864df86acdc11e8df2c5172477819a4d12942cbe5d40` |
| prom/prometheus | `v3.5.0` | `sha256:63805ebb8d2b3920190daf1cb14a60871b16fd38bed42b857a3182bc621f4996` |
| grafana/grafana | `12.1.1` | `sha256:a1701c2180249361737a99a01bc770db39381640e4d631825d38ff4535efa47d` |


表内 digest 已通过 registry 元数据查询，尚未拉取运行。Compose image 使用 tag@digest 并固定 platform: linux/amd64；它锁定的是镜像 manifest/index，不等于服务兼容性已测。客户端库版本与服务端版本不同是正常的，协议兼容性仍在 B0/F1 做运行验证。这些版本是项目固定实验基线，不声称是最新发行版或长期免维护版本。

### 4.13 当前开发虚拟机与低内存约束

当前开发虚拟机基线固定记录为：6 GiB 内存、80 GiB 动态磁盘；虚拟化配置“2 个处理器、每个 4 核”在来宾系统中通常表现为 8 个 vCPU。该配置足以完成 A～E 阶段和单 broker Kafka 的功能验证，但不属于全组件并发压测或三 broker 容错环境。

8 vCPU 不代表可以开启 8 路编译。此机器的约束资源是内存；默认 Preset 的 build jobs 保持 2，并统一设置：

```bash
export VCPKG_MAX_CONCURRENCY=2
export CMAKE_BUILD_PARALLEL_LEVEL=2

cmake --build --preset proxy-debug --parallel 2
```

首次编译 Drogon、Protobuf、OpenSSL、Exiv2 或启用 Sanitizer 时，如果出现 OOM、系统持续交换或交互明显卡顿，立即降为 1，不提高并行度重试：

```bash
export VCPKG_MAX_CONCURRENCY=1
export CMAKE_BUILD_PARALLEL_LEVEL=1

cmake --build --preset photo-debug --parallel 1
```

资源使用规则：

| 场景 | 可同时运行 | 禁止或限制 |
|---|---|---|
| A：代理开发 | 编辑器、编译器、ChaosProxy、Echo、单元测试 | 不启动数据库和 Kafka |
| B/C：业务开发 | 单 API、MySQL、Redis、单 Worker | Worker 处理线程为 1；编译时停止不需要的服务 |
| D/E：双实例实验 | 两个 API、Nginx、MySQL、Redis、单 Worker | 实验结束恢复单 API；监控 profile 默认关闭 |
| F：Kafka 功能验证 | 单 broker、Dispatcher、Consumer/Worker、MySQL；Redis/Nginx 按场景启动 | 不与 vcpkg 全量编译、ASan/TSan、Prometheus/Grafana 同时运行 |
| 三 broker / 全链压测 | 不在当前 6 GiB 虚拟机执行 | 换 12～16 GiB 环境，结果单独标注机器配置 |

本机开发配额初值：MySQL `innodb_buffer_pool_size=256M`、`max_connections=50`；Redis `maxmemory=128mb`；Kafka 单 broker JVM heap 为 512 MiB；平时只启一个 API；Worker 只启一个进程和一个处理线程。它们是低资源开发值，不是性能结论，进入压力测试时必须在报告中记录和重新评估。

建议为虚拟机提供 6～8 GiB swap。swap 只是避免短时编译峰值直接触发 OOM，不能把持续 swap 当作可接受运行状态。每次大规模安装、编译或场景测试前执行：

```bash
nproc
free -h
df -h /
```

安全门槛：`MemAvailable` 低于 1 GiB 时不再启动新服务；根文件系统可用空间低于 15 GiB 时不开始 vcpkg 全量构建；低于 10 GiB 时停止生成新构建目录并先检查 `build/`、vcpkg `buildtrees/`、`downloads/`、容器镜像和 `runs/`，确认内容后再清理。禁止用未经展开确认的递归删除命令。

如果宿主机本身核心数不多，建议把虚拟机 CPU 拓扑调整为 1 个处理器、4 个核心；这对当前并发上限已经足够，也更少占用宿主机调度资源。保留 2×4 也不会破坏项目正确性，但构建并发仍不得超过上述限制。

## 5. ChaosProxy 对象与所有权

### 5.1 固定对象

| 对象 | 拥有的状态 | 生命周期 |
|---|---|---|
| Listener | listen fd、ProxyId、固定上游 | Proxy 生命周期 |
| ConnectionPair | 两个 Endpoint、两个 Direction | 同一 Reactor 创建和销毁 |
| Endpoint | UniqueFd、连接阶段、读写关闭标记、事件兴趣 | ConnectionPair 所有 |
| Direction | ready/delayed 队列、字节偏移、策略运行状态 | ConnectionPair 所有 |
| ConnectionTable | slot → generation → ConnectionPair | Reactor 所有 |
| TimerQueue | deadline、序号、TimerId、连接 token | Reactor 所有 |
| ReadyQueue | 待续跑的连接方向及去重标记 | Reactor 所有 |
| PolicySnapshot | 不可变策略参数与版本 | Chunk/连接持有共享只读引用 |

Endpoint 的传输阶段用 Connecting/Established/Closed；read_eof、write_shutdown 分别记录。不能把“读关闭”和“写关闭”做成互斥枚举，导致无法表达双向关闭组合。

方向恒定为 client_to_upstream、upstream_to_client；配置中的 upstream/downstream 必须在文档中对应，不交替改名。

### 5.2 核心接口示意

接口的唯一声明见第 23 节；这里集中说明协议与错误路径。

调用系统接口和队列操作的失败必须返回 Status 或进入明确关闭路径；这里省略返回值不表示实际 AddFd/ModifyFd 能忽略错误。generation 回绕不能静默复用：采用足够位宽或回绕时退休 slot。

### 5.3 非阻塞 I/O 规则

- socket/accept fd 设置 NONBLOCK、CLOEXEC；UniqueFd 只移动。
- recv > 0 才推进输入偏移；recv == 0 是该方向 EOF。
- EINTR 重试但受公平预算约束；EAGAIN 等待就绪；错误记录 errno。
- send 仅消费实际写出的字节；MSG_NOSIGNAL 防止进程被 SIGPIPE 终止。
- connect EINPROGRESS 订阅建连相关事件；完成后查 SO_ERROR，不能把 EPOLLOUT 视为成功。
- 连接态检查之后处理 ERR/HUP/RDHUP 与可读数据；HUP 不是丢弃缓冲的充分理由。
- 一个连接每轮最多入 ReadyQueue 一次/方向，避免重复 token 放大工作。
- 读取上游名称的 DNS 使用启动解析或专门解析器；禁止 Reactor 内阻塞 getaddrinfo。

### 5.4 半关闭与回收

收到源 EOF → 停止该方向读取 → 等待所有策略暂存与发送队列排空 → shutdown(destination, SHUT_WR)。另一方向继续运行。

显式 abort/reset 可以丢弃队列，但必须记录策略与 discarded_bytes；graceful close 不得偷换成 abort。Half-close 等待有可配置上限，超时则有明确 reason。

Close 幂等顺序：标记关闭 → 失效事件/定时 token → 从就绪队列逻辑移除 → 注销 epoll → 关闭 fd → 释放队列预算 → 在安全回收点销毁对象。不得在正在使用对象的回调栈中释放自身后继续访问成员。

## 6. 缓冲、背压与公平调度

### 6.1 数据所有权

Chunk 采用不可变数据块 + offset/length view。切片共享底层块，消费推进视图；有界队列拥有 view。Toxic 不反复拷贝整块字符串。

预算分两类：

- 逻辑排队字节：每方向待转发有效字节，用于水位。
- 真实内存：底层块 capacity 只计一次，另计队列节点、定时器、日志与控制队列；释放最后引用才归还。

避免一个 1 字节 view 长期钉住 1 MiB 块却只计 1 字节内存。也不能把共享底层块重复计作多份物理内存。

所有 ready、delayed、pipeline 暂存都纳入水位。全局预算耗尽时停止接收/读入并暴露过载指标；恢复时按就绪队列轮转，避免一次唤醒全部连接。

### 6.2 ET + 用户态轮转

每次 Pump 直到以下之一：EAGAIN、EOF/错误、没有业务发送资格、背压上限、字节/操作预算耗尽。

| 停止原因 | 下一次推进来源 |
|---|---|
| recv/send EAGAIN | 相应 epoll 就绪事件 |
| 公平预算耗尽但仍可推进 | 用户态 ReadyQueue 主动续跑 |
| 读高水位 | 下游消费降到低水位后主动读一次或安排 Pump |
| 无 token | 计算最早可发送时间，定时器唤醒 |
| 延迟未到期 | 到期定时器 |
| 暂停策略 | 控制面恢复/策略截止定时器 |

ReadyQueue 非空时 epoll_wait 使用非阻塞轮询并继续调度，不能无限阻塞；每轮也必须处理新 fd 事件、到期定时器和少量控制命令。禁止把暂时不可运行的方向不断塞回就绪队列形成忙等。

只有 connect 未完成，或有“已获准发送的数据且 socket 曾 EAGAIN”时需要写就绪关注。无 token/未到期而 socket 一直可写时，不靠 EPOLLOUT 空转。

Linux 手册讨论了 ET 饥饿与用户态 ready list，可作为行为依据。[epoll(7)](https://man7.org/linux/man-pages/man7/epoll.7.html)

### 6.3 初始可调参数

以下是实验起点，不是性能承诺：块 16 KiB；每方向高/低水位 256/128 KiB；每次 Pump 最多 64 KiB 或 32 次 I/O；每轮定时回调也限额；全局数据块预算 64 MiB；连接数由 fd 限额和队列预算共同限制。

必须验证：一个大流连接与短请求并存时，短请求 p99 和 timer_lateness 不被无限放大。比较吞吐损失与公平收益，不只报告最高吞吐。

### 6.4 过载、资源耗尽与控制保活

accept 同样有每轮预算。EMFILE/ENFILE 时记录原因并暂时停止接收，使用有界退避恢复；reserve fd 可以帮助接收后立即关闭一个连接，但不替代最大连接数与进程 fd 配额。禁止在错误就绪上无限 accept 忙等。

控制队列和日志有独立保留额度；数据缓冲达到上限不能使 restore 命令永远无法执行。记录拒绝连接、队列丢弃和日志丢失计数。SIGTERM 经 signalfd 或安全唤醒交给 owner loop，不在异步信号处理器里销毁 C++ 连接对象。

## 7. TimerQueue 与故障策略

### 7.1 时钟和定时器

Clock 使用 steady_clock；日志另记墙上时间。最小堆 key=(deadline, sequence)；timerfd 设置最近到期时间。Cancel 惰性删除时要有压缩阈值，不能让取消项长期无限累积。

Timer 只保存 ConnectionToken、DirectionId、操作序号，或可验证生命周期的弱引用。触发时重新查表。Clock/FakeClock 与 RandomSource 可注入，禁止测试依赖长 sleep。

### 7.2 策略契约

| 策略 | 固定语义 | 需要澄清的边界 |
|---|---|---|
| latency | 数据段进入策略后的最早可释放时间 | 不承诺接收端测得延迟精确等于配置 |
| jitter | 基础延迟上加有界随机量 | 到期时间按输入顺序单调钳制，避免字节乱序 |
| bandwidth | Token Bucket，按送入下一阶段/发送的明确边界扣 token | 配置声明每连接还是共享；V1 默认每方向每连接 |
| slicer | 限制应用层 emit/send 片段大小，可加间隔 | 不控制内核分段与对端 recv 边界 |
| pause | 暂停释放并保持有界背压 | 不把无限缓存叫黑洞 |
| timeout | 指定 pause 时长后 abort；0 表示持续暂停至恢复 | 与 idle_timeout、connect_timeout 分开命名 |
| limit_data | 按成功 send 字节达到阈值后执行指定 close/reset | send 成功不代表对端应用已消费 |
| close | 排空当前允许保留的数据后 FIN，或显式 abort 模式 | 两种模式不能共用模糊名称 |
| reset | 对指定 socket 做 abortive close，并处理关联方向 | 不能让一次 RST 只关闭 TCP 的一个半方向 |

TCP 代理丢弃已读取的字节属于数据破坏，不是 IP 丢包；真实包丢失/乱序实验由隔离 netns + tc netem 负责。

Pipeline 顺序在配置中固定。首批支持 latency/jitter → bandwidth → slicer → output；limit_data 在实际发送边界计数；pause/timeout/reset 是状态控制。任意策略重排不是首批保证，unsupported 组合在配置校验时拒绝。

Token Bucket 使用单调时间：tokens = min(capacity, tokens + rate × elapsed)。扣除实际获准转发的字节，允许部分 Chunk 推进；若发送失败/短写，按选定的扣费边界归还或只扣实际发送量，不能不同分支混用。capacity 和 rate 校验正值；rate=0 需要暂停时使用显式 pause，不在除法中处理。下一唤醒根据最小可推进字节计算，不强制等整个大 Chunk 的 token，避免 Chunk 大于 bucket 后永久卡住。

### 7.3 异步 continuation

接口的唯一声明见第 23 节；这里集中说明协议与错误路径。

延迟操作拥有 Chunk 与旧 PolicySnapshot；回调不捕获栈上 DirectionContext&。恢复必须从 next_stage 继续，不能重新从第一个 latency 开始。每个 operation 只有一个完成/取消出口，防止重复 Emit。

关闭时同时取消 pipeline 与 TimerQueue；对已失效操作 Resume 返回可观测的 stale 状态而非访问悬空内存。

## 8. 运行时控制与故障复现

### 8.1 控制协议

Linux UDS，换行分隔 JSON，单帧上限 64 KiB，单客户端请求/响应队列有界。使用成熟 JSON 库，允许部分读写。UDS 权限限定实验用户；不用手写 HTTP 控制解析器。

命令：list_proxies、list_connections、apply_policy、pause、resume、reset_connections、restore_baseline、get_metrics、shutdown。所有变更带 request_id 和 expected_policy_version；不匹配返回 CONFLICT。重复 request_id 返回原结果，缓存本身有界且声明保留窗口。

ACK 表示 Reactor 已应用变更，并返回 applied_version、受影响连接数和开始时间；不是仅表示控制进程“收到了字符串”。

### 8.2 热更新边界

- listen/upstream 变化：显式 drain/recreate，不静默改变活跃连接目的地。
- 新策略只作用于更新后接收的新 Chunk；已有 Chunk 携带旧快照完成。
- 新旧队列必须经同一方向有序输出闸门，不能因新配置延迟更小而超越旧数据。
- pause/resume 是独立的方向输出闸门，能冻结已有待发数据；恢复不强制改写旧策略状态。
- 重配需精确实验窗口时：先等 quiescent 屏障，再 apply ACK，再启动测试请求。
- 多 Reactor 阶段：全部目标 owner 应用后才 ACK；失败必须返回各 owner 状态，Runner 不把部分成功当成功。

### 8.3 固定 seed 的真实含义

每个受控逻辑连接、方向和策略派生独立随机流。实验连接 ID 由测试协调映射，不使用 fd 数字作为跨运行身份。

byte_offset 表示方向累计字节；按字节阈值触发的策略不绑定偶然 recv 分块。jitter 若要求跨分块复现，使用固定大小逻辑区间及记录的尾段/释放规则；不能把 recv 次数当稳定序号。

Trace 记录 run_id、配置摘要、策略版本、逻辑连接、方向、触发偏移、随机决策、计划/实际触发时间、关闭原因。默认不记录业务 payload。

三个复现等级：

1. 单元：FakeClock + 受控分块，精确复现决策和输出。
2. 组件：受控 Socket 输入、固定逻辑分块与屏障，验证故障位置。
3. 真实服务：复现相同条件与决策，比较允许误差；不声称 OS 调度或端到端延迟完全一致。

Trace 是有界诊断产物；丢失条目必须标记 incomplete，不能称作完整重放记录。

## 9. PhotoTask 应用层、线程与资源边界

### 9.1 用例而非万能 Service

- ImportPhoto：受控导入与内容登记。
- GetPhoto：缓存查询、回源与响应版本。
- UpdatePhoto：乐观版本更新与缓存失效 Outbox。
- CreateTask：幂等创建与任务事件。
- GetTask / GetTaskResult：权威状态与当前结果。
- DispatchEvents：可靠传递事件。
- AdmitTask：将任务事件接入持久化待执行状态。
- ExecuteTask：claim、计算、结果提交与恢复。

Controller 仅校验 HTTP、构造命令、调用用例、映射结果。SQL 位于具体 repository；框架 Result/HttpRequest 不渗透 domain。

### 9.2 端口与回调约束

接口的唯一声明见第 23 节；这里集中说明协议与错误路径。

类型声明由对应阶段补齐。新工程在 cp_common 中实现唯一一套 Status/StatusOr，CLI 与 HTTP 只负责边界映射，不另造 Result/Outcome。

操作拥有 shared state；所有成功、超时和连接错误竞争一个 once-completion。单个请求状态固定回到所属应用 loop 更新，跨线程通过框架投递。完成 HTTP 响应后不再使用悬空 Controller/栈引用。

**逻辑超时不等于底层操作已取消。** 容量令牌必须保留到客户端库确认完成或连接销毁，迟到结果不得二次响应。无法取消的 SQL 仍可能执行；不能因为 HTTP 已超时便释放所有限制并接受无限新请求。

### 9.3 线程分工

| 执行位置 | 工作 | 禁止 |
|---|---|---|
| ChaosProxy Reactor | Socket、策略、计时 | 图像、SQL、阻塞日志 |
| PhotoTask HTTP loop | 请求编排、异步回调 | future.get、同步 SQL、图像解码 |
| 数据库/Redis 客户端连接 | 非阻塞协议与连接状态 | 多调用者无约束共享一条事务连接 |
| Dispatcher | 小批量 claim 与异步发送 | 持有 DB 行锁等待 Redis/Kafka |
| Kafka poll loop | 消费、投递 DB admission、处理回调 | 图像计算、无限排队 |
| Worker CPU pool | 图像与元数据处理 | 持有 DB 事务执行耗时任务 |

复用 Drogon 的连接管理，不再写一套相同的原生连接池。自己实现的是进入客户端库之前的容量限制、总 deadline、查询合并和降级策略。

### 9.4 预算模板

每实例配置：最大在途 HTTP、最大 Redis 操作数、最大 MySQL 操作数、等待队列容量、singleflight key/等待者数量；Worker 配置：CPU 并发、最大解码像素、解码内存总量；Kafka 配置：客户端 fetch/produce 缓冲和 admission 在途数。

Redis 不可用时，所有 API 实例的 MySQL 回源上限之和必须留在数据库预算内。进程内 singleflight 不保证跨实例合并，不用依赖 Redis 的锁来保护 Redis 故障回源。

Deadline 从请求入口开始，包含排队。可用操作时间为 min(剩余总时间, 依赖上限)。确切超时 API 与网络取消行为必须在固定框架版本上实验；不把配置一个定时器冒充强制中断 SQL。

## 10. HTTP API 与幂等契约

| API | 请求要点 | 响应/一致性 |
|---|---|---|
| GET /v1/photos/{id} | 可选 consistency=strong | 默认缓存快照；strong 查 MySQL 主库 |
| PATCH /v1/photos/{id} | description、tags、expected_version | 200 + 新版本；并发冲突 409 |
| POST /v1/tasks | Idempotency-Key、photo_id、kind、params | 提交确认后 202 + task_id + Location |
| GET /v1/tasks/{id} | owner 范围校验 | MySQL 状态，不读 Redis |
| GET /v1/tasks/{id}/result | 仅成功任务 | 当前已选结果或明确错误 |
| GET /v1/task-requests/{key} | 同 owner 下查询创建请求 | 幂等记录存在则返回原 task_id |
| POST /v1/tasks/{id}/retry | 受控管理调用、预期状态、操作幂等键 | 同任务新 generation，不覆盖历史 attempt |
| GET /livez、/readyz、/metrics | 独立语义 | 存活/服务能力/指标 |

错误体：code、message、request_id、retryable；COMMIT_UNKNOWN 额外说明只能使用同一幂等键查询/重试，不提供“任务肯定失败”的结论。HTTP 示例映射：参数错误 400、未找到 404、冲突 409、容量不足 429、依赖不可用 503、deadline 504。

API 限制 body、标签数、字符串长度和图片任务参数；不接受客户端传入绝对文件路径、SQL 或 arbitrary callback URL。使用参数化 SQL。

任务 params 规范化：补齐默认值、限制数字范围、排序无序集合、保留有序步骤。指纹包含 owner、photo_id、输入内容摘要、任务种类、参数与 processor_version；不包含请求时间、request_id。

指纹由版本化规范 JSON 投影 + SHA-256 生成，Golden fixture 固定语义。不能直接 hash 原始 HTTP JSON；字段顺序或空白不同应视为同一请求。Protobuf 序列化也不直接承担此规范化契约。

同 owner 下：同 key 同指纹返回同 task_id；同 key 不同指纹返回 409。V1 实验不自动清除幂等记录，若未来设置保留期必须明确超过保留期后的重试语义。

## 11. MySQL 数据模型与事务

### 11.1 权威模型

所有业务表使用 InnoDB；时间记录 UTC、微秒精度；跨进程 lease 使用数据库时间。进程本地 deadline 仍用单调时钟，二者不混用。

数据库使用主库读写，明确事务隔离级别为 READ COMMITTED。锁定查询只在短事务内使用。以下是字段蓝图 DDL，需要在 B 阶段补齐外键、CHECK、权限、migration checksum 与回滚/升级策略。

```sql
CREATE TABLE photos (
  photo_id CHAR(36) CHARACTER SET ascii PRIMARY KEY,
  owner_scope VARBINARY(64) NOT NULL,
  input_key VARBINARY(240) NOT NULL,
  content_sha256 BINARY(32) NOT NULL,
  content_bytes BIGINT UNSIGNED NOT NULL,
  media_type VARCHAR(32) NOT NULL,
  description TEXT NOT NULL,
  tags_json JSON NOT NULL,
  metadata_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_at DATETIME(6) NOT NULL,
  updated_at DATETIME(6) NOT NULL,
  UNIQUE KEY uq_input (owner_scope, input_key)
) ENGINE=InnoDB;

CREATE TABLE photo_tasks (
  task_id CHAR(36) CHARACTER SET ascii PRIMARY KEY,
  owner_scope VARBINARY(64) NOT NULL,
  photo_id CHAR(36) CHARACTER SET ascii NOT NULL,
  kind VARCHAR(32) NOT NULL,
  params_json JSON NOT NULL,
  input_sha256 BINARY(32) NOT NULL,
  processor_version VARCHAR(64) NOT NULL,
  state VARCHAR(32) NOT NULL,
  dispatch_mode VARCHAR(16) NOT NULL,
  generation BIGINT UNSIGNED NOT NULL DEFAULT 1,
  owner_epoch BIGINT UNSIGNED NOT NULL DEFAULT 0,
  attempt_count INT UNSIGNED NOT NULL DEFAULT 0,
  lease_owner VARCHAR(96),
  lease_until DATETIME(6),
  next_run_at DATETIME(6),
  result_key VARBINARY(240),
  result_sha256 BINARY(32),
  last_error_code VARCHAR(64),
  created_at DATETIME(6) NOT NULL,
  updated_at DATETIME(6) NOT NULL,
  KEY ix_ready (state, next_run_at, task_id),
  KEY ix_lease (state, lease_until, task_id),
  KEY ix_photo (photo_id, created_at),
  FOREIGN KEY (photo_id) REFERENCES photos(photo_id)
) ENGINE=InnoDB;

CREATE TABLE idempotency_requests (
  owner_scope VARBINARY(64) NOT NULL,
  idempotency_key VARBINARY(96) NOT NULL,
  fingerprint_version INT UNSIGNED NOT NULL,
  request_fingerprint BINARY(32) NOT NULL,
  task_id CHAR(36) CHARACTER SET ascii NOT NULL,
  response_code SMALLINT UNSIGNED NOT NULL,
  response_json JSON NOT NULL,
  created_at DATETIME(6) NOT NULL,
  PRIMARY KEY (owner_scope, idempotency_key),
  FOREIGN KEY (task_id) REFERENCES photo_tasks(task_id)
) ENGINE=InnoDB;

CREATE TABLE outbox_events (
  event_id CHAR(36) CHARACTER SET ascii PRIMARY KEY,
  aggregate_id CHAR(36) CHARACTER SET ascii NOT NULL,
  event_kind VARCHAR(40) NOT NULL,
  aggregate_version BIGINT UNSIGNED NOT NULL,
  destination VARCHAR(24) NOT NULL,
  schema_version INT UNSIGNED NOT NULL,
  payload MEDIUMBLOB NOT NULL,
  state VARCHAR(24) NOT NULL,
  claim_token CHAR(36) CHARACTER SET ascii,
  lease_until DATETIME(6),
  next_attempt_at DATETIME(6) NOT NULL,
  attempt_count INT UNSIGNED NOT NULL DEFAULT 0,
  last_error_code VARCHAR(64),
  created_at DATETIME(6) NOT NULL,
  delivered_at DATETIME(6),
  UNIQUE KEY uq_event (aggregate_id, event_kind, aggregate_version),
  KEY ix_dispatch (state, next_attempt_at, event_id),
  KEY ix_outbox_lease (state, lease_until, event_id)
) ENGINE=InnoDB;

CREATE TABLE task_attempts (
  task_id CHAR(36) CHARACTER SET ascii NOT NULL,
  generation BIGINT UNSIGNED NOT NULL,
  owner_epoch BIGINT UNSIGNED NOT NULL,
  worker_id VARCHAR(96) NOT NULL,
  state VARCHAR(32) NOT NULL,
  output_key VARBINARY(240),
  output_sha256 BINARY(32),
  error_code VARCHAR(64),
  started_at DATETIME(6) NOT NULL,
  finished_at DATETIME(6),
  PRIMARY KEY (task_id, generation, owner_epoch),
  FOREIGN KEY (task_id) REFERENCES photo_tasks(task_id)
) ENGINE=InnoDB;

CREATE TABLE inbox_events (
  consumer_group VARCHAR(96) NOT NULL,
  event_id CHAR(36) CHARACTER SET ascii NOT NULL,
  topic VARCHAR(128) NOT NULL,
  partition_id INT NOT NULL,
  record_offset BIGINT NOT NULL,
  admitted_at DATETIME(6) NOT NULL,
  PRIMARY KEY (consumer_group, event_id),
  UNIQUE KEY uq_record (consumer_group, topic, partition_id, record_offset)
) ENGINE=InnoDB;

CREATE TABLE consumer_errors (
  consumer_group VARCHAR(96) NOT NULL,
  topic VARCHAR(128) NOT NULL,
  partition_id INT NOT NULL,
  record_offset BIGINT NOT NULL,
  payload_sha256 BINARY(32) NOT NULL,
  bounded_payload BLOB,
  reason VARCHAR(128) NOT NULL,
  created_at DATETIME(6) NOT NULL,
  PRIMARY KEY (consumer_group, topic, partition_id, record_offset)
) ENGINE=InnoDB;
```

migration 分配：001_core.sql 创建 photos/photo_tasks/idempotency_requests/outbox_events/task_attempts；002_cache.sql 只做缓存阶段新增的必要索引/约束（没有数据库变更则不制造空迁移）；003_inbox.sql 创建 inbox_events/consumer_errors。migration runner 记录版本、校验和并串行执行；已应用脚本不原地修改。

migration 分配：001_core.sql 创建 photos/photo_tasks/idempotency_requests/outbox_events/task_attempts；002_cache.sql 只做缓存阶段新增的必要索引/约束（没有数据库变更则不制造空迁移）；003_inbox.sql 创建 inbox_events/consumer_errors。migration runner 记录版本、校验和并串行执行；已应用脚本不原地修改。

事件 payload 业务限制 64 KiB，不因为 MEDIUMBLOB 容量大就放整张图片。标签、任务参数和响应都限制大小。结果路径只含内部生成的短 ID；检查 Linux 文件名上限。

MySQL 持久性实验必须记录实际配置：InnoDB 提交日志同步策略、binlog 是否启用及其同步策略、卷类型。主线按每次事务提交要求日志同步配置；如果测试环境放宽持久性，报告明确降级。唯一约束与事务不能单独证明设备掉电后数据一定存在。

### 11.2 创建任务事务

生成 task_id、event_id、规范指纹后：

1. BEGIN，校验 photo 所属、不可变输入摘要、任务参数。
2. INSERT photo_tasks，状态 PENDING_DISPATCH，冻结 dispatch_mode。
3. INSERT idempotency_requests（owner_scope, key 唯一）。
4. INSERT outbox_events：TASK_REQUESTED，version=task generation，destination=LOCAL_TASK 或 KAFKA_TASK。
5. COMMIT，只有收到客户端库的真实事务提交成功通知，才返回 202。

并发重复请求若在第 3 步唯一约束冲突，整个事务回滚，临时 task 不保留；再在新事务/新可用连接中读取既有幂等记录、比较指纹并返回。死锁或锁等待不能一律当重复；明确错误分类并使用相同 key 有界重试。

COMMIT 回调前连接断开：记录 COMMIT_UNKNOWN，丢弃失去同步状态的连接，不自动生成新 key；调用方查主库记录或用原 key 重试。不得把 RAII Transaction 对象销毁等同于服务端确认提交。

MySQL 参数化查询和事务通过 Drogon 适配。阶段 B0 必须用真实数据库验证事务提交/回滚回调、超时和连接失效语义后，才封装 CreateAtomic；不靠框架命名猜测行为。

### 11.3 资料更新事务

UPDATE photos SET description=?, tags_json=?, metadata_version=metadata_version+1 WHERE photo_id=? AND owner_scope=? AND metadata_version=?。

受影响 1 行：同事务写 CACHE_INVALIDATE Outbox，aggregate_version=新版本，提交后返回新版本。0 行要区分不存在、owner 不匹配和版本冲突，不能假装成功。

缓存删除是加速传播，不属于 MySQL 事务。提交后可以 best-effort 快速删除，可靠重试由 Outbox 保证；不得因 Redis 删除失败回滚已经提交的数据库事实。

## 12. Redis 缓存、降级与一致性

### 12.1 Cache-Aside 流程

GetPhoto：总 deadline → Redis（短依赖预算）→ 命中则验证结构与 schema → 返回快照；miss/超时/连接错误 → 检查回源额度 → singleflight → MySQL 主库 → 返回并尝试填充缓存。

缓存 key：phototask:v1:{owner}:photo:{photo_id}。value：photo_id、description、tags、metadata_version、cache_schema_version。使用 TTL，V1 不做长期负缓存。

GetTask 和幂等查询不走 Redis。GET photo?consistency=strong 也不走 Redis，并计入数据库额度。Redis 故障是可降级事件，不作为整个 API 的 readyz 失败条件。

Cache-Aside 是常见读缓存模式；本项目的一致性承诺需由自己的写入和过期规则界定。[Redis 官方说明](https://redis.io/docs/latest/develop/use-cases/cache-aside/)

### 12.2 不夸大缓存一致性

删除缓存失败可可靠重试，但仍可能发生“旧查询读到旧值 → 更新并删除缓存 → 旧查询晚到回填”。因此不能宣称强一致。

本版规定：

- 默认照片资料允许短暂陈旧，返回 metadata_version。
- 同一 key 已有更高版本时，Redis Lua CAS 不允许更低版本覆盖。
- key 不存在时 CAS 无法知道数据库的新版本，仍可能晚到旧填充。
- 填充 TTL 按本次数据库读取开始计算剩余有效期；过期或 deadline 后的结果禁止填充，避免慢查询重置完整 TTL。
- 单机实验考虑命令传输延迟和时钟限制，不宣称任意故障环境下严格秒级陈旧上界。
- 需要写后读自己最新数据时，客户端用 PATCH 返回值或 strong 查询。

Outbox 重放较老 invalidation 删除新缓存只会造成额外 miss，不破坏数据库；可优化但不以优化牺牲恢复正确性。

### 12.3 降级机制

- Redis 操作超时使用客户端真实完成/连接关闭清理，不只退出 HTTP。
- 每个 API 实例独立熔断器：Closed → Open → HalfOpen；阈值、窗口、探测数和恢复条件入配置。
- Open 时绕过 Redis，但 MySQL 回源仍必须准入；满时 503/429，不无限等待。
- singleflight 的 key、leader 和 waiter 数都有限；waiter 有独立 deadline，leader 的后台操作也有总上限。
- Redis 恢复不同时全量预热；先受控 probe 再恢复正常查询。
- 熔断状态不是分布式一致状态，不把两个实例的不同状态当实现错误。

初始实验预算示例：请求总 800 ms，缓存操作上限 30 ms，回源排队上限 50 ms，单次 DB 调用上限 500 ms；具体数值须符合固定客户端库能力并实测。拒绝把毫秒 timer 包住不可取消秒级阻塞调用后声称已实现硬上限。

## 13. Outbox 与发布协议

### 13.1 三种 destination

| destination | 执行动作 | 成功依据 |
|---|---|---|
| CACHE_INVALIDATE | Redis DEL | Redis 确认，重复删除允许 |
| LOCAL_TASK | 把 PENDING_DISPATCH 任务变为 READY | 与本行 Outbox DELIVERED 在同一 MySQL 事务提交 |
| KAFKA_TASK | 发布 Protobuf 任务事件 | Kafka delivery callback 成功后，更新本行 DELIVERED |

初期不运行 Kafka，但事务与 Outbox 已存在。切换模式只影响新任务，旧事件按创建时 destination 完成；不能一键把未处理旧消息标为已投递。

### 13.2 Claim、重试与重复

短事务用 FOR UPDATE SKIP LOCKED 查询有限批可用 Outbox 行 → 写 IN_FLIGHT、claim_token、lease_until、attempt_count → COMMIT。释放行锁后才调用 Redis/Kafka。

成功更新必须匹配 event_id + claim_token + IN_FLIGHT。失败写回 PENDING 与指数退避+jitter；进程崩溃由 lease 过期后重新 claim。总 attempt/时间有上限，超过后进入 NEEDS_REVIEW，暴露积压并提供受控重试；不直接删除可靠事件。

SKIP LOCKED 适合这种队列表分工，不用它读取用户需要一致视图的业务列表。[MySQL 锁定读](https://dev.mysql.com/doc/refman/8.4/en/innodb-locking-reads.html)

Redis 已删除但 DB 标记失败：重新删除。Kafka 已写入但 DB 标记失败：可能重新发布相同 event_id。此窗口是至少一次协议的正常情况，不用一句“幂等生产者”掩盖。

事件创建与业务事实同事务；外部投递不在事务内；已被 claim 的 payload 不修改。retention 不得删除尚未交付或待调查事件。

LOCAL_TASK 完成动作在同一事务校验 Outbox claim_token 和 task generation，再将任务变 READY 与事件标 DELIVERED；过期 claim 不能继续推动状态。故障时若一个事务结果未知，重新读取二者状态，不补写一半。

### 13.3 端口示意

接口的唯一声明见第 23 节；这里集中说明协议与错误路径。

PublishReceipt 表示 broker 确认或本地动作持久化，不能在 produce() 仅入本地队列时返回成功。每个 inflight 消息拥有 payload 和回调状态，发送队列容量耗尽时向 Outbox 施加背压。

## 14. Worker 状态、租约与文件结果

### 14.1 状态机

PENDING_DISPATCH → READY → RUNNING → SUCCEEDED。

RUNNING 可到 RETRY_WAIT、FAILED_FINAL、NEEDS_REVIEW；RETRY_WAIT 在 next_run_at 到期后由 scheduler 变 READY；失联 RUNNING 在 lease 过期后进入可重试路径。每次尝试增加 owner_epoch 与 attempt_count。

手动 retry 只针对允许的终态：保留 task_id，增加 generation，重置本轮状态为 PENDING_DISPATCH，同事务创建新的 TASK_REQUESTED 事件。旧 generation 消息只能标为 stale，不能重启新任务。成功任务不做原地“覆盖重试”，重新处理应新建任务。

### 14.2 Claim 与 Heartbeat

事务中锁定 READY task → owner_epoch++ → 写 worker_id、lease_until、RUNNING → INSERT task_attempt → COMMIT。只有收到提交确认才开始图像操作。

Heartbeat 使用数据库 NOW(6)，条件匹配 task_id、generation、epoch、worker_id、RUNNING 且 lease 尚未过期；影响 0 行则失去所有权。恢复必须新 claim，不允许旧 Worker 给过期 lease 续命。

MySQL 不可用时，不继续作出业务可见的结果提交。Worker 可以停止计算或留下私有中间文件，不能把本地计算完成当 SUCCEEDED。

### 14.3 图像与输入

photo-admin 从白名单源根通过安全相对路径打开文件，限制文件大小、类型，拷贝到服务拥有的 input 目录，边读计算 SHA-256；完成文件同步和目录发布后，登记 photos。

数据库登记失败留下孤儿 input，后台按登记记录和安全保留期清理；不对原 PhotoBridge 源做删除。HTTP API 只接收 photo_id。

Worker 检查输入 hash 与 task 快照一致。先读取尺寸并检查宽高乘法溢出、总像素和解码预算，再解码；压缩文件大小小不代表内存小。图像失败明确分类为无效输入/可重试 I/O/内部错误。

处理器以 TaskProcessor 接口区分 ThumbnailProcessor、MetadataProcessor。共享的 claim、deadline、输出写入和提交由执行器负责，不能各写一套恢复逻辑。

### 14.4 文件发布与 DB 可见性

output_key 使用 result/{task_id}/{generation}/{epoch}/{nonce}.jpg 或 .json。文件属于 attempt，不使用所有 Worker 共享的 final.jpg。

流程：

1. 当前 claim 下在对应目录创建唯一 temp，O_EXCL，不覆盖。
2. 生成并写完，校验输出结构、大小与 digest。
3. 同步文件，rename no-replace 到本 attempt 的不可变 output_key，同步目录；新目录也处理父目录项同步。
4. 开短事务锁 task，检查当前 generation/epoch/worker/状态及未过期 lease。
5. 更新 photo_tasks.result_key/result_sha256、SUCCEEDED，并更新 task_attempts；同事务提交。
6. HTTP 仅依据 MySQL 的 result_key 提供结果。

如果旧 Worker 在失去租约后完成第 3 步，它只能产生一个不被引用的文件。它不能通过第 4 步，所以不会覆盖新 Worker 的业务结果。**这里的 fencing 保护的是数据库可见性，并不声称能阻止一切旧文件写入。**

DB COMMIT 未知时重新查询当前 task 选择的 output_key；不盲目重写成功结果。文件发布成功但 DB 未选择的输出作为孤儿候选，保留调查窗口后按 attempt 记录清理；不得仅按扩展名删除。

SUCCEEDED 但 result 文件消失/损坏：返回 RESULT_INCONSISTENT 并记录审计，不能向用户继续报成功下载；显式修复另走管理路径。

同机持久卷是当前前提。fsync 不能替代跨主机存储保证；进程 kill 实验也不单独证明掉电安全。[fsync(2)](https://man7.org/linux/man-pages/man2/fsync.2.html)

### 14.5 执行器接口

接口的唯一声明见第 23 节；这里集中说明协议与错误路径。

Processor 在 CPU pool 运行，不直接修改 task state；它写入执行器提供的 TempOutput，文件发布与数据库提交由执行器分别完成。一个任务仅一种 processor，若未来需要多个独立处理步骤，应新建子任务模型而非暗中复用同一状态。

## 15. Nginx 双实例与重试归属

部署两个 PhotoTask API 实例，使用同一 MySQL、Redis 和输入/结果卷。Nginx 到每个实例经过不同命名 Proxy，以便只破坏 A 而保留 B。

GET 查询允许在明确连接错误/超时条件下有界切换；POST/PATCH 默认不做网关重试，客户端使用幂等键决定重试。不能仅靠 HTTP 方法推断所有操作语义。

Nginx 的 proxy_read_timeout 是相邻读操作间的超时，不是完整业务请求总 deadline。应用仍要维护自己的总预算。非幂等请求和响应已经发给客户端之后的重试有额外限制。[Nginx proxy 模块](https://nginx.org/en/docs/http/ngx_http_proxy_module.html)

配置模板应显式给出：upstream 两个代理地址、max_fails/fail_timeout、connect/read/send timeout、next_upstream_tries；写请求 location 关闭自动 next_upstream 重试。使用默认被动失败检测，不宣称已实现商业版主动健康检查。

livez 只判断进程主循环；readyz 判断必要能力与容量。Redis 故障只进入 degraded。数据库故障可使实例不接受写请求，但不要配置成不停重启进程；重启不会修好共享 MySQL。

只指定一个业务重试主负责人：测试客户端负责写入不确定后的有界重试；应用仅对确认安全的内部动作重试；网关不再重复写重试。记录 original_request_count 与 backend_attempt_count，检查重试放大。

## 16. Kafka 后续实现蓝图

Kafka 是已确定的后续阶段，不是本文遗漏的“有空再研究”。它替换 LOCAL_TASK 的分发动作，保留 MySQL 的任务权威状态、幂等和 Worker 执行协议。

### 16.1 采用“消息接入”和“图像执行”分离

最终流程：

创建任务事务 → Outbox → Kafka TASK_REQUESTED → Consumer 持久化 admission → MySQL READY → 执行器 claim → 图像处理 → 结果条件提交。

Consumer 把事件接入 inbox，并将匹配的 PENDING_DISPATCH task 更新为 READY；两者同事务。**Kafka offset 在这个持久接入事务确认后提交，而不是等待图片计算结束。** 后续故障由 MySQL 的 READY/lease/retry 协议恢复。

这样明确地把可靠责任从 Kafka 交给 MySQL，不把消息只放进内存 CPU 队列就提交 offset。Kafka 分区也不会被长时间图像计算占用。

MySQL READY 表用于任务状态、租约和恢复；Kafka 用于事件传递与后续独立订阅。当前单服务单机规模下 Kafka 未必提高吞吐，其价值必须通过异步隔离、积压恢复和消费者实验体现，不能包装成无成本的必需组件。

### 16.2 Protobuf 事件

以下为 proto/phototask/v1/task_event.proto 的完整消息形状模板，进入 B 阶段后固定字段编号：

```proto
syntax = "proto3";
package phototask.events.v1;

message TaskRequested {
  string task_id = 1;
  string photo_id = 2;
  uint64 generation = 3;
  string task_kind = 4;
  bytes input_sha256 = 5;
  string processor_version = 6;
}

message CacheInvalidated {
  string photo_id = 1;
  string owner_scope = 2;
  uint64 metadata_version = 3;
}

message EventEnvelope {
  uint32 schema_version = 1;
  string event_id = 2;
  string aggregate_id = 3;
  int64 created_at_unix_ms = 4;
  string trace_id = 5;
  oneof body {
    TaskRequested task_requested = 10;
    CacheInvalidated cache_invalidated = 11;
  }
}
```

Outbox 从 B 阶段就存该序列化事件，LOCAL_TASK Dispatcher 同样读取，未来无需重写业务 payload。HTTP 的 params 留在 MySQL task 快照中，Kafka 事件是任务引用，不携带图片或任意本地绝对路径。

Consumer 不能盲信 event：检查 schema_version、oneof、必需字段、hash 长度、对应 task 的 photo/generation/kind/processor/input 摘要一致。新增未知 optional 字段按协议兼容；未知任务类型或不支持版本进入隔离处理。

删除字段用 reserved，不能复用编号。固定 protoc/runtime 与 Golden 兼容 fixture；不要求跨版本序列化字节完全相同。[Protobuf 语言指南](https://protobuf.dev/programming-guides/proto3/)

### 16.3 Topic 与消费组

| 项目 | 选择 |
|---|---|
| 主 topic | phototask.task-requested.v1 |
| key | task_id，重试事件同一任务落同一 key |
| 核心消费组 | phototask-admission-v1 |
| dead-letter topic | phototask.invalid-events.v1，先 durable 隔离后按需投递 |
| 新订阅者 | 统计/审计用不同 group；不能因此重复执行同一 task |
| 初始分区 | 实验选择少量分区；用明确连接与吞吐数据调整 |

消息顺序不能替代 DB generation 判断。重复和过期事件不使 SUCCEEDED 回退为 READY；task 当前 RUNNING/READY 时重复 admission 是无副作用成功。

### 16.4 Producer

librdkafka：enable.idempotence=true、acks=all；核查与 retries/max.in.flight 的约束；设置有限 message.timeout.ms、queue.buffering.max.kbytes/messages。通过 poll/delivery callback 驱动完成，不能阻塞整个 Dispatcher。

- produce 入队成功不等于 broker 写入成功。
- delivery 成功后才尝试更新 Outbox DELIVERED。
- 本地消息超时仍需允许结果不确定；重发保留原 event_id。
- producer 幂等不能去除进程重启、Outbox 重放造成的所有业务重复。
- 关闭时先停止 claim、drain 回调至截止时间，未确认事件留在 Outbox 恢复。

客户端配置和提交方式以固定版本说明为准。[librdkafka 配置](https://docs.confluent.io/platform/current/clients/librdkafka/html/md_CONFIGURATION.html)

单 broker profile 用于逻辑和重试测试，不能宣称可容忍 broker 故障导致的数据丢失。后续三 broker profile 明确 replication.factor=3、min.insync.replicas=2、acks=all，测试 quorum 不足时写入失败和恢复；不用单节点指标替代多副本实验。

### 16.5 Consumer 与 Offset

设置 enable.auto.commit=false、enable.auto.offset.store=false。每条记录的关键流程：

1. 有界读取并解析消息。
2. BEGIN；用 consumer_group + event_id 去重。
3. 校验 task 快照与 generation；新事件使 PENDING_DISPATCH → READY，重复已接入事件不改运行态。
4. INSERT inbox_events 并 COMMIT。
5. 仅在事务确认后将该分区 offset+1 标为可提交。

首版每分区最多一个 DB admission 在途，允许不同分区并行；只提交连续已持久化前缀，不能跳过一个未完成 offset。Consumer poll 线程持续服务心跳与回调，用 pause/resume 控制没有额度的分区，不能暂停调用 poll。

DB 提交未知：不提交 offset；重新连接后查 inbox/重放。DB 成功、offset 提交失败：重放由 inbox 去重。

Rebalance 时停止对撤销分区新建 admission；确认提交只能由当前 assignment 处理。旧回调可完成幂等 DB admission，但不能以旧 assignment 提交 offset。新 Consumer 重放后查询 inbox 即可恢复。

### 16.6 毒消息与重试职责

- 解析失败、未知 schema、任务指纹不匹配：将 topic/partition/offset、摘要和受限 payload 持久化 consumer_errors 后，才允许该 offset 前进；告警并供人工处理。
- 数据库暂时不可用：不隔离成坏消息，暂停相应分区并退避重试。
- 图片无效：属于任务执行失败，记录 FAILED_FINAL；不反复扔回 Kafka。
- 图片处理临时 I/O 错误：MySQL RETRY_WAIT 负责，不要求重新发布原消息。
- DLQ 发送失败：已有 consumer_errors 保留事实；可由独立 Outbox 再投递，不因此丢记录。

隔离记录不代表业务成功；相关 task 持续未 admission 应由 PENDING_DISPATCH 年龄监控发现。手动修复需验证原 task 与 schema 后创建明确修复动作。

### 16.7 避免 broker 地址绕过代理

broker 返回 advertised.listeners 地址，客户端会按它建立后续连接。只把 bootstrap.servers 指向 ChaosProxy 不够。

同一 Compose 网络的示例：

- broker-b1 的 CLIENT 实际监听 kafka-b1:9092，ChaosProxy kafka-b1 监听 19092 并转发至它。
- CLIENT 公布 chaosproxy:19092；其他 broker 分别公布 19093、19094。
- broker 间使用独立 INTERNAL listener 和真实 broker 地址，不经客户端故障代理。
- KRaft controller listener/端口独立配置。
- Producer/Consumer 与公布地址处于同一可达网络；不把容器 localhost 当其他容器。

检查实际连接目的地和代理字节计数，确认没有绕行。多 broker 只故障一个入口时，客户端可能切换其他 broker，必须在实验定义中说明预期。[Kafka advertised.listeners](https://kafka.apache.org/41/configuration/broker-configs/#advertised.listeners)

## 17. 场景执行器与精确故障窗口

### 17.1 Runner 设计

使用 Python CLI 编排现有 C++ 可执行文件、HTTP 和数据库观察器；这是测试工具，不再为编排写第二个 C++ 网络框架。配置统一 JSON，提供有限步骤，不实现通用脚本语言。

步骤类型：wait_ready、seed_fixture、start_workload、wait_barrier、apply_fault、release_barrier、wait_observation、restore、assert、collect_report。

每步必须有 deadline；等待就绪或条件轮询可使用短有界间隔，不用固定睡 10 秒猜测服务是否启动。恢复 baseline 和停止负载放入 finally；Runner 异常退出后的 watchdog 使用故障 TTL 恢复，避免下一次实验继承旧故障。

场景 ID、run_id、配置摘要、fixture 摘要、二进制版本、镜像版本写入报告。预先声明预期失败：某些请求超时可以是实验成功；业务不变量失守才是断言失败。

示意，不是已经实现的 CLI：

```text
scenario-runner run scenarios/redis_slow.json --report runs/<run-id>
chaosctl apply --proxy redis-api --file policies/latency_200ms.json
chaosctl restore --proxy redis-api
```

### 17.2 控制与业务触发分离

ChaosProxy 不解析 SQL、Redis 命令或 Kafka 请求。因此“第 N 字节断开”不能自动等同于“COMMIT 后断开”或“broker 已写入后断开”。精确测试使用测试客户端/业务 hook 提供屏障，代理只控制传输。

测试 hook 编译选项默认 OFF，仅在隔离实验开放 UDS/独立内部入口。hook 具有总截止时间；生产配置不允许启用。它们控制时序，不伪造数据库提交事实。

至少提供：before_db_commit、after_db_commit_before_http_response、before_result_commit、after_outbox_publish_before_mark、after_inbox_commit_before_offset、after_attempt_output_sync。

### 17.3 MySQL 提交确认丢失实验

1. 使用专用实验实例/连接池容量 1，事务完成 INSERT 后停在 before_db_commit。
2. Runner 等到屏障，暂停该连接 mysql upstream_to_client 输出，等 applied ACK。
3. 通过独立控制入口释放屏障；COMMIT 向 MySQL 正常发出。
4. 独立 observer 主库连接轮询幂等记录，确认提交已经可见。
5. reset 被测连接；应用应得到失败或结果不确定，而非伪造明确未提交。
6. 使用相同 Idempotency-Key 重试；检查 task 数为 1、Outbox 事件数为 1、返回相同 task_id。

如果第 4 步未观察到提交，该运行标记为未命中目标窗口，不用它证明 COMMIT_UNKNOWN 已覆盖。另做 after_db_commit_before_http_response 的 HTTP 响应丢失场景，两者不能混为同一个测试。

### 17.4 故障脚本模板

```json
{
  "schema_version": 1,
  "scenario_id": "redis_slow_cache_fallback",
  "seed": 42,
  "fixture": "photos-small-v1",
  "steps": [
    {"op": "wait_ready", "target": "photo-api", "timeout_ms": 30000},
    {"op": "start_workload", "profile": "hot_photo_reads"},
    {"op": "apply_fault", "proxy": "redis-api", "direction": "upstream_to_client",
     "policy": {"type": "latency", "delay_ms": 200}, "fault_ttl_ms": 20000},
    {"op": "wait_observation", "condition": "redis_breaker_open", "timeout_ms": 10000},
    {"op": "assert", "condition": "db_admission_within_configured_limit"},
    {"op": "restore", "proxy": "redis-api"},
    {"op": "wait_observation", "condition": "cache_recovered", "timeout_ms": 15000},
    {"op": "collect_report"}
  ]
}
```

condition 名称映射固定 Observer 函数，不 eval 任意输入。模板引用的 fixture/profile/condition 要在场景目录注册且可校验，不能把配置通过当测试通过。

## 18. 故障验收矩阵

| 编号 | 场景 | 触发与观察 | 必须成立 |
|---|---|---|---|
| P01 | 无故障双向 Echo | 不同分块、不同大小 | 内容与顺序完全一致 |
| P02 | 半关闭叠加延迟限速 | 源 FIN，队列非空 | 排空后 FIN，反向仍可继续 |
| P03 | 慢消费者 | 停读/低速读 | 队列/内存有界，恢复可继续 |
| P04 | fd 复用与过期 timer | 高频建连关闭 | 旧 token 不命中新连接 |
| P05 | ET 公平预算 | 一个大流 + 多短请求 | 不饿死、不丢续跑；记录 p99 |
| P06 | token 为零 | 低速与空桶 | 无 EPOLLOUT 忙等 |
| P07 | 热更新旧队列 | 新旧 delay 不同 | 同方向无乱序、ACK 版本准确 |
| P08 | reset/close/timeout | tcpdump 与错误日志 | 语义可区分，关闭幂等 |
| B01 | Redis 慢 | 延迟、缓存超时 | 回源数有界，应用按预算结束 |
| B02 | Redis 恢复 | remove fault | HalfOpen 受控，缓存恢复可观察 |
| B03 | 删除缓存失败 | Redis 断开时更新资料 | DB 版本正确，Outbox 留存并补偿 |
| B04 | 旧查询晚回填 | 屏障控制读写顺序 | 不宣称强一致；strong 返回新值，陈旧最终消退 |
| B05 | 同 key 并发创建 | 两个 API 同时请求 | 只一 task/创建事件；相同返回 |
| B06 | 同 key 不同参数 | 参数变化 | 409，无第二任务 |
| B07 | COMMIT 确认丢失 | 第 17.3 节屏障 | 原 key 重试得到原任务 |
| B08 | DB 不可用 | 暂停/RST | 队列有界、明确拒绝、无假成功 |
| W01 | Worker 计算中退出 | kill 专用 Worker | lease 后恢复，历史 attempt 保留 |
| W02 | 旧 Worker 晚完成 | 卡住后过租约再释放 | 旧 epoch 不能选择业务结果 |
| W03 | 输出完成后 DB 失败 | 屏障 + DB fault | 孤儿可定位，任务不假成功 |
| W04 | 成功结果丢失 | 受控删除 fixture 结果 | RESULT_INCONSISTENT，不静默掩盖 |
| N01 | API A 变慢 | Nginx→A 故障 | GET 有界切换；B 正确，记录重试放大 |
| N02 | 写响应丢失 | after_commit hook | 网关不无条件重放写，业务 key 保证唯一 |
| K01 | broker ACK 丢失 | 已确认事件写入后切断响应 | Outbox 可重发，event_id 不变 |
| K02 | Outbox 发布后退出 | delivery 成功、mark 前 kill | 重复被 inbox/任务状态吸收 |
| K03 | DB admission 后退出 | offset 前 kill | 重放不会重复启动已接入任务 |
| K04 | 消费期间 DB 故障 | admission 事务失败 | 不越过未持久化 offset，缓冲有界 |
| K05 | Consumer rebalance | 增减实例 | 旧 assignment 不提交新 offset，任务不丢 |
| K06 | 未知/损坏消息 | fixture 注入 | 隔离持久化后前进，不无限卡分区 |
| K07 | 旧 generation 消息 | 手动 retry 后重放 | 新 generation 不被旧事件覆盖 |
| K08 | broker 路由 | 查连接地址与字节计数 | 公布的客户端地址全部按计划经代理 |

失守的业务不变量优先于性能。每个场景报告是否命中目标窗口；未命中不能计作通过。kill -9 仅在独立 fixture 进程；重启时间、超时和预算误差在报告中记录。

K01 的 broker 写入事实要由独立观察 Consumer/event_id 或可核查 broker 结果确认。若仅观察 produce() 返回入队成功，该测试不成立。

## 19. 可观测性与报告

### 19.1 指标分层

| 层 | 指标 |
|---|---|
| 代理 | active_connections、forwarded/discarded_bytes、queued_bytes、allocated_buffer_bytes、backpressure_duration |
| 调度 | ready_queue_size、event_loop_iteration_time、timer_lateness、budget_yield_count |
| 策略 | fault_applied、fault_active、policy_version、trace_dropped |
| API | request_total、request_duration histogram、inflight、rejected、deadline_exceeded |
| 依赖 | Redis/DB operation_duration、errors、inflight、admission_wait、breaker_state |
| 缓存 | hit/miss、fallback、singleflight_join、invalidate_pending_age |
| 事务 | idempotency_replay/conflict、commit_unknown |
| 任务 | 各状态数量、queue_age、attempt_duration、lease_expired、stale_complete_rejected |
| 消息 | outbox_age、publish_errors、inbox_duplicate、consumer_lag、quarantined |

Prometheus label 只用有限集合，例如 proxy、direction、operation、outcome、task_kind。不能把 task_id、request_id、photo_id、错误消息作为 label，防止高基数。这些 ID 放结构化日志和报告。

代理本体无需链接 Drogon 暴露 metrics；可用 UDS 快照由轻量 exporter 转成 Prometheus 格式。PhotoTask 用框架路由导出。监控采集失败不能阻塞数据路径。

### 19.2 报告字段

run_id、scenario_id、git_commit、build_type、kernel、CPU/内存、依赖镜像摘要、fixture hash、参数、seed、步骤时间、applied ACK、目标窗口证据、断言结果、原始指标文件、清理结果。

分别报告：请求成功率、预期故障率、恢复耗时、任务成功/失败/未知数、重复业务结果数。没有真实结果的数据字段填“未测量”，不填预期值冒充测试。

Prometheus/Grafana 是可选的可视化部署 profile；JSON/CSV 报告与退出码是必需交付。不要求先做仪表盘才算会使用中间件。

## 20. 性能升级与停止条件

### 20.1 必须先保存的基线

直连 vs 无故障代理；小消息 vs 大流；1/100/1000 连接按机器能力；单/双方向；启用单策略与组合策略；业务缓存命中/未命中/依赖故障。

记录吞吐、p50/p95/p99、CPU、RSS、系统调用、队列长度、timer_lateness。负载发生器 CPU 单独观察。说明闭环压测在服务变慢时会自动降低发送速率；固定到达率实验需控制发压端积压并记录失败，避免只看幸存请求延迟。

### 20.2 升级顺序

1. 每轮 I/O 预算 + ReadyQueue；验证公平性和故障精度。
2. Chunk view / scatter-gather 写出；确认确有复制或 syscall 热点再应用。
3. TimerQueue 惰性取消压缩与批处理；时间轮需在大量定时器数据上与堆比较。
4. 多 Reactor：每个 ConnectionPair 固定 owner，不迁移连接；acceptor 交接或 SO_REUSEPORT 二选一。
5. Buffer Pool：确认分配热点后实现，测内存常驻成本和 p99，不只测平均速度。

sendmsg/writev 批量只能合并当前已允许发送的连续数据，不能绕过延迟/限速。splice 可作为无策略路径的独立实验，但与内容追踪/缓冲预算及热切换有成本；不列为必做“零拷贝”。

### 20.3 多 Reactor 决议

默认升级采用每 Reactor 自己 accept 的 SO_REUSEPORT listener，连接终身固定 owner；控制命令由 eventfd+有界队列投递，指标按线程汇总。共享总内存先用保守配额分片，不先引入复杂无锁全局 allocator。

连接 token 包含 owner 身份；timer/close 在 owner 执行。所有 fd 同时可用的线程数量不代表线性吞吐增益。用 1/2/4 Reactor 对比真实 CPU 瓶颈；如果上游或负载端已饱和，不能据此判定 Reactor 优化无效或成功。

性能优化只在具体风险或瓶颈解决后扩展；不要以“再多一个技术点”为理由无限改架构。

## 21. 部署与日常运行契约

### 21.1 Compose profiles

| profile | 服务 |
|---|---|
| proxy | ChaosProxy、Echo、Runner |
| photo-base | proxy + MySQL、Redis、photo-api、Dispatcher、DB 模式 Worker |
| photo-ha | photo-base + 第二 API、Nginx |
| photo-kafka | photo-ha + Kafka、Kafka admission 模式 Worker；新任务 mode=kafka |
| kafka-quorum | 三 broker 与副本配置，用于后续实验 |
| observability | Prometheus、Grafana/exporter |

这些是实施时需要交付的 profile 名称，不表示当前存在 compose.yaml。主机端测试与容器内测试不能混用 hostname；README 给出一种默认拓扑并把另一种列为单独方案。

示意工作流：配置/构建 → 启动基础依赖 → migrate schema → 启动应用 → wait_ready → photo-admin import fixtures → 正常任务成功 → Runner 故障实验 → 查看报告。

readiness 检查比 depends_on 的启动顺序更重要；应用需要处理运行中的依赖断开与重连。凭据使用本地未提交环境文件，实验账户最小范围；不把真实私人照片作为公开 fixture。

### 21.2 资源与配额

日常开发遵守第 4.13 节的 6 GiB 虚拟机限制。分别记录 proxy-only、photo-base 和 Kafka profile 的实测内存/CPU；单 broker Kafka 只做功能与恢复验证，Kafka 三节点在 12～16 GiB 以上的独立环境运行。限制容器内存时也观察客户端缓存和解码峰值，不把容器 OOM 当业务超时处理。

任务与 Outbox 虽存放在数据库，也不能无限接纳：设置每 owner 的待处理任务额度、Outbox 积压告警和结果目录容量水位。容量达到阈值时在创建事务前/事务内执行一致的准入检查并拒绝新任务；并发计数可使用 owner 配额行锁或条件计数更新，不用不加锁的 COUNT 后 INSERT 证明硬上限。成功历史、inbox 与幂等记录的清理单独定义保留窗口，不能为腾空间删除尚需去重/恢复的数据。

### 21.3 关闭顺序

停止新场景/负载 → 控制面恢复故障 → API 停止接收新请求并有界等待 → Dispatcher 停 claim 并处理已发送回调 → Consumer 停 admission 并处理可确认 offset → Worker 结束或放弃 lease → 代理排空/截止关闭 → 收集最终指标。

关闭超时不是丢弃 durable state 的理由；未确认 Outbox、READY 和过期 lease 留给下次恢复。

## 22. 关键不变量总表

1. 除显式数据破坏/abort，代理不改变同方向内容与顺序。
2. EOF 不丢弃仍应转发的延迟、限速和发送队列。
3. fd 数字不是连接身份；关闭和取消幂等。
4. 预算让出后主动续跑；资源不足后等待真正唤醒，不忙等。
5. 所有队列、inflight 和实际数据块都有边界。
6. 控制 ACK 表示目标 Reactor 应用完成；策略更新不使旧 Chunk 失去所有者。
7. 图像计算不进入网络事件循环。
8. 业务成功状态以 MySQL 提交确认或事后权威查询为依据。
9. 同 owner、同幂等键只绑定一个创建任务指纹和 task_id。
10. 创建任务与发布意图同事务；外部发送不持有行锁。
11. 默认照片缓存允许陈旧；强读取、任务状态和幂等查询不依赖缓存。
12. Redis 故障时回源仍有独立额度。
13. Protobuf 解析成功不等于事件通过业务校验。
14. Kafka offset 只越过已持久接入或已持久隔离的连续记录。
15. admission 持久化后由 MySQL 负责执行恢复，不依赖内存工作队列。
16. Kafka 重放不使任务状态回退；旧 generation 不影响新 generation。
17. 旧 Worker 不能通过数据库条件选择业务结果。
18. attempt 文件不可变且互不覆盖；API 只提供 DB 已选结果。
19. 逻辑超时不被误认为底层 I/O 或事务已取消。
20. 请求重试、Outbox 重试、任务重试分别有负责人、次数和期限。
21. 观察链路可绕过代理，但应用链路不可绕过实验配置。
22. 没有命中目标故障窗口的运行不能计入该场景通过数。
23. 进程崩溃恢复、多副本容错、掉电持久性分别出具证据。
24. 文档设计、实现、可编译、测试通过、已测性能五种状态分别记录。

## 23. 类、函数与文件的详细实现契约

本节是接口级设计，不是完整可编译头文件。代码块省略重复 include、构造依赖和私有成员；类型、路径、调用方向和成功语义必须按本节实现。涉及外部库的方法名来自 adapter 自定义接口，不假装是第三方库原生 API。

### 23.1 公共错误、资源与时间

对应文件：`include/chaosproxy/common/{status.h,status_or.h,unique_fd.h,clock.h,completion.h,logging.h}`；非内联实现放 `src/common/`。

```cpp
namespace chaosproxy {
enum class StatusCode {
    kOk, kInvalidArgument, kNotFound, kConflict, kResourceExhausted,
    kUnavailable, kDeadlineExceeded, kIoError, kCommitUnknown,
    kStaleOwner, kInconsistent, kInternal
};
class Status {
public:
    static Status Ok();
    Status(StatusCode code, std::string message);
    bool ok() const noexcept;
    StatusCode code() const noexcept;
    const std::string& message() const noexcept;
};
template<class T> class StatusOr {
public:
    StatusOr(T value);
    StatusOr(Status error);
    bool ok() const noexcept;
    const Status& status() const noexcept;
    T& value() &;
    const T& value() const &;
    T&& value() &&;
};
class UniqueFd {
public:
    UniqueFd() noexcept;
    explicit UniqueFd(int fd) noexcept;
    ~UniqueFd();
    UniqueFd(const UniqueFd&) = delete;
    UniqueFd& operator=(const UniqueFd&) = delete;
    UniqueFd(UniqueFd&&) noexcept;
    UniqueFd& operator=(UniqueFd&&) noexcept;
    int get() const noexcept;
    int release() noexcept;
    void reset(int replacement = -1) noexcept;
    explicit operator bool() const noexcept;
};
using TimePoint = std::chrono::steady_clock::time_point;
using Duration = std::chrono::steady_clock::duration;
class Clock {
public:
    virtual ~Clock() = default;
    virtual TimePoint Now() const noexcept = 0;
};
struct Unit {};
template<class T> using Completion = std::function<void(StatusOr<T>)>;
Status InitLogging(const LoggingOptions&);
Status StatusFromErrno(int error, std::string_view operation);
int ExitCodeForStatus(const Status&) noexcept;
} // namespace chaosproxy
```

StatusOr 不允许 OK + 无 value；错误构造传入 OK 时规范化为 Internal 或显式禁止，Release 也不能留下非法状态。value 访问契约统一记录，不把错误分支的未检查访问当正常控制流。

UniqueFd 析构不抛异常；Linux close 被 EINTR 中断时不盲目重试同一数字，避免命中被复用 fd。reset 不能 double-close。errno 在错误发生后立即保存。

`Completion<T>` 表示恰好一次逻辑完成；回调生命周期和取消由所属操作状态负责。所有 response code 映射只在 CLI/HTTP 边界完成。

### 23.2 代理值类型与 Socket 层

对应文件：`proxy/types.h`、`proxy/socket_ops.h`、`src/proxy/socket_ops.cpp`。

```cpp
namespace chaosproxy {
struct ProxyId { std::uint32_t value; };
struct ConnectionToken { std::uint32_t slot; std::uint32_t generation; };
enum class EndpointSide { kClient, kUpstream };
enum class DirectionId { kClientToUpstream, kUpstreamToClient };
enum class TransportState { kConnecting, kEstablished, kClosed };
enum class IoCode { kProgress, kWouldBlock, kEof, kError };
struct IoResult {
    IoCode code;
    std::size_t bytes = 0;
    int error = 0;
};
enum class ConnectProgress { kConnected, kInProgress };
struct ConnectResult { UniqueFd fd; ConnectProgress progress; };
struct AcceptResult { UniqueFd fd; SocketAddress peer; };

StatusOr<UniqueFd> CreateListener(const SocketAddress&, int backlog);
StatusOr<std::optional<AcceptResult>> TryAccept(int listener_fd);
StatusOr<ConnectResult> StartConnect(const SocketAddress&);
Status FinishConnect(int fd);
IoResult TryRead(int fd, std::span<std::byte> dst);
IoResult TryWrite(int fd, std::span<const std::byte> src);
Status ShutdownWrite(int fd);
Status SetAbortiveClose(int fd);
} // namespace chaosproxy
```

TryAccept 成功但 optional 为空表示 WouldBlock；其他错误保留 errno。TryRead/TryWrite 每次实际 syscall 或 EINTR 重试都受上层 Pump 预算约束，不能隐藏无限循环。空 buffer 不进入 recv==0 的 EOF 判断；非空写返回 0 当作无进展异常处理。

SocketAddress 在启动时解析为 sockaddr_storage + length；连接路径不用字符串反复解析。网络层不包含 ProxyServer 回调或 SQL。

### 23.3 Buffer、Chunk 与预算

对应文件：`proxy/buffer.h/.cpp`。所有权单位是 allocation，逻辑队列单位是 byte view。

```cpp
namespace chaosproxy {
class BufferBudget {
public:
    bool TryReserve(std::size_t capacity);
    void Release(std::size_t capacity) noexcept;
    std::size_t Used() const noexcept;
};
struct BufferBlock; // owns storage and its budget reservation
struct Chunk {
    std::shared_ptr<const BufferBlock> storage;
    std::size_t offset;
    std::size_t length;
    std::uint64_t stream_offset;
    std::uint64_t sequence;
    std::shared_ptr<const PolicySnapshot> policy;
};
class ByteQueue {
public:
    Status Push(Chunk);
    std::span<const std::byte> FrontBytes() const;
    void Consume(std::size_t bytes);
    std::size_t QueuedBytes() const noexcept;
    bool Empty() const noexcept;
    void Clear() noexcept;
};
} // namespace chaosproxy
```

读入流程：先申请可写块和真实容量预算 → recv 写入独占块 → 用 actual bytes 形成不可变 Chunk → 交给 Pipeline。不能先将 mutable 块共享后继续写它。

ByteQueue 的 Consume 只推进 offset/length，不删除尚未发送的尾部。底层最后一个引用释放时才归还 allocation 预算；逻辑字节数用于读高低水位。测试分别断言逻辑计数与真实 allocation 计数。

### 23.4 EventLoop、ConnectionTable、ConnectionPair

对应文件：`proxy/event_loop.*`、`connection_table.*`、`connection_pair.*`。

```cpp
namespace chaosproxy {
struct PumpBudget { std::size_t bytes_left; std::uint32_t calls_left; };
struct EventToken { std::uint64_t packed; };
struct Endpoint {
    UniqueFd fd;
    TransportState state;
    bool read_eof = false;
    bool write_shutdown = false;
    std::uint32_t subscribed_events = 0;
};
struct Direction; // queues, watermarks, token bucket and pending FIN

class ConnectionTable {
public:
    StatusOr<ConnectionToken> Insert(std::unique_ptr<ConnectionPair>);
    ConnectionPair* Find(ConnectionToken) noexcept;
    void Retire(ConnectionToken);
    void ReclaimRetired();
};
class EventLoop {
public:
    Status AddFd(int fd, std::uint32_t events, EventToken);
    Status ModifyFd(int fd, std::uint32_t events, EventToken);
    Status RemoveFd(int fd);
    void EnqueueReady(ConnectionToken, DirectionId, WakeReason);
    Status Post(ControlCommand); // bounded; eventfd wakes the loop
    void Run();
    void RequestStop();
};
class ConnectionPair {
public:
    void OnReadable(EndpointSide);
    void OnWritable(EndpointSide);
    void OnEventError(EndpointSide, int error);
    void Pump(DirectionId, PumpBudget&);
    void ResumeRead(DirectionId);
    void OnSourceEof(DirectionId);
    void TryFinishDirection(DirectionId);
    void RefreshInterests();
    void Close(CloseReason);
};
} // namespace chaosproxy
```

EventLoop 是本版类名；Reactor 是它所属的模型，不另造一个同义 Reactor 类。

`ProxyServer` 组合 EventLoop、ConnectionTable、TimerQueue、BufferBudget、各 Listener；这些对象都由同一 owner loop 操作。回调先通过 ConnectionTable 验证 token，再调用 ConnectionPair；ConnectionPair 不拥有表，不销毁正在执行的自身。

事件 token 必须索引 endpoint side 与连接 generation，不能只放 fd。具体使用 64 位递增 registration_id，在 EventLoop 注册表中映射到 {ConnectionToken, EndpointSide}；不能把两个 32 位值再加 side 硬挤进 64 位。RemoveFd 删除映射，registration_id 回绕时拒绝继续分配。控制/Listener/timerfd 注册采用带类型的 entry。就绪队列去重 key=(connection token,direction)。`ReclaimRetired()` 在当前事件/回调批次安全点执行。

RefreshInterests 从建连、读预算、待发资格、EAGAIN 状态推导订阅，不把 epoll mask 当另一份互相独立的连接状态。源 FIN、对端 ERR、限速唤醒都要重新计算。

### 23.5 Timer 与策略的调用接口

对应文件：`timer_queue.*`、`toxic_pipeline.*`、`toxics.*`。

```cpp
namespace chaosproxy {
struct TimerId { std::uint64_t value; };
class TimerQueue {
public:
    TimerId Schedule(TimePoint deadline, std::function<void()> callback);
    bool Cancel(TimerId);
    std::optional<TimePoint> NextDeadline() const;
    void RunExpired(TimePoint now, std::size_t callback_budget);
};
class TokenBucket {
public:
    void Refill(TimePoint now);
    std::size_t Available(TimePoint now) const;
    void Consume(std::size_t actual_bytes);
    TimePoint NextEligibleTime(std::size_t bytes, TimePoint now) const;
};
struct ContinuationToken {
    ConnectionToken connection;
    DirectionId direction;
    std::uint64_t operation_id;
    std::uint64_t policy_version;
    std::size_t next_stage;
};
class ToxicPipeline {
public:
    Status Accept(Chunk, DirectionContext&);
    Status Resume(ContinuationToken);
    void CancelAll();
};
} // namespace chaosproxy
```

Latency/Jitter：计算 deadline → 保存 Chunk、next_stage、连接 token → 到期 lookup 验证 → Resume。回调不引用栈上 DirectionContext。

Bandwidth：Refill → 取允许发送字节 → send → Consume(actual bytes)；没有可发字节时安排一次唤醒。`Available` 的只读调用以前一次显式 Refill 后的 token 为准，不能暗中采用另一套时钟计算。

Slicer：产出共享块视图，记录逻辑偏移，不做不必要数据复制。LimitData：在实际 send 成功边界增加计数。Pause/Timeout/Reset：调用明确方向闸门或 Close，不侵入每个策略内部实现清理。

### 23.6 ProxyServer 与控制面

对应文件：`proxy/proxy_server.*`、`proxy/config.*`、`control/*.h/.cpp`、`apps/chaosproxy_main.cpp`、`apps/chaosctl_main.cpp`。

```cpp
namespace chaosproxy {
StatusOr<ProxyConfig> LoadProxyConfig(const std::filesystem::path&);
Status ValidateProxyConfig(const ProxyConfig&);
class ProxyServer {
public:
    Status Start(const ProxyConfig&);
    StatusOr<AppliedPolicy> ApplyPolicy(const ApplyPolicyCommand&);
    Status RestoreBaseline(ProxyId);
    ProxyMetrics SnapshotMetrics() const;
    void Run();
    void BeginDrain(TimePoint deadline);
};
class ControlProtocol {
public:
    Status Feed(std::span<const std::byte>);
    StatusOr<std::optional<ControlCommand>> NextCommand();
    std::string EncodeReply(const ControlReply&) const;
};
class ControlServer {
public:
    Status Start(const std::filesystem::path& uds_path);
    void OnReadable(int client_fd);
    void OnWritable(int client_fd);
    void QueueReply(ControlRequestId, ControlReply);
    void StopAccepting();
};
class ControlClient {
public:
    StatusOr<ControlReply> Call(const ControlCommand&, TimePoint deadline);
};
} // namespace chaosproxy
```

ControlServer 的 client fd 同样需要自己的 generation/注册 token，不能照搬“拿裸 fd 找回复”而忽略控制连接重建。ControlClient 是独立 CLI，可用 poll 驱动有限同步调用，不阻塞代理 loop。

main：CLI11 parse → InitLogging → Load/ValidateConfig → 构造 ProxyServer/ControlServer → Start → Run → 有界 drain → ExitCodeForStatus。帮助与版本输出不启动网络。

TraceWriter 的声明放 `proxy/trace_writer.h`，实现放对应 cpp：`bool TryAppend(const TraceEvent&)`、`void FlushUntil(TimePoint)`、`TraceStats Snapshot() const`。网络 loop 只做有界入队，文件写入走独立日志线程；队列满记录 dropped，不等待磁盘。Snapshot 读取线程安全计数，不泄漏对可变容器的引用。

TraceWriter 的声明放 `proxy/trace_writer.h`，实现放对应 cpp：`bool TryAppend(const TraceEvent&)`、`void FlushUntil(TimePoint)`、`TraceStats Snapshot() const`。网络 loop 只做有界入队，文件写入走独立日志线程；队列满记录 dropped，不等待磁盘。Snapshot 读取线程安全计数，不泄漏对可变容器的引用。

### 23.7 PhotoTask 强类型与领域数据

对应文件：`phototask/model/ids.h`、`models.h/.cpp`、`request_fingerprint.h/.cpp`。

```cpp
namespace phototask {
using chaosproxy::Status;
using chaosproxy::StatusOr;
using chaosproxy::Unit;
template<class T> using Completion = chaosproxy::Completion<T>;
using Deadline = chaosproxy::TimePoint;
struct PhotoId { std::string value; };
struct TaskId { std::string value; };
struct EventId { std::string value; };
struct Digest { std::array<std::byte, 32> bytes; };
struct Generation { std::uint64_t value; };
struct OwnerEpoch { std::uint64_t value; };
enum class TaskKind { kThumbnail, kExtractMetadata };
enum class TaskState {
    kPendingDispatch, kReady, kRunning, kRetryWait,
    kSucceeded, kFailedFinal, kNeedsReview
};
enum class DispatchMode { kLocal, kKafka };
struct ThumbnailParams { std::uint32_t max_width; std::uint32_t max_height; };
struct MetadataParams { bool include_xmp; };
using TaskParams = std::variant<ThumbnailParams, MetadataParams>;
struct PhotoView {
    PhotoId id;
    std::string owner_scope;
    std::string description;
    std::vector<std::string> tags;
    std::uint64_t metadata_version;
};
struct TaskSpec {
    TaskId id;
    PhotoId photo_id;
    TaskKind kind;
    TaskParams params;
    Digest input_digest;
    std::string processor_version;
};
struct TaskClaim {
    TaskSpec spec;
    Generation generation;
    OwnerEpoch owner_epoch;
    std::string worker_id;
};
Status ValidateTaskParams(TaskKind, const TaskParams&);
StatusOr<NormalizedTaskRequest> NormalizeTaskRequest(CreateTaskCommand);
Digest FingerprintV1(const NormalizedTaskRequest&);
bool IsValidTaskTransition(TaskState from, TaskState to);
} // namespace phototask
```

ID 从服务端生成，验证长度/字符集后入库；客户端不能指定 epoch。params 的默认值与规范化只定义一次，JSON 与 Protobuf adapter 不各自定义不同默认值。

指纹内包含规范化参数、输入摘要、owner、种类和 processor_version，HTTP 字段顺序不影响它。输入图片本身在导入时已经冻结；修改 description 不暗中改变已创建任务的处理输入。

### 23.8 Repository、Cache 与 FileStore 端口

集中声明文件 `phototask/application/ports.h`，避免一张表一套 DAO/Store/Gateway。

```cpp
namespace phototask {
class PhotoRepository {
public:
    virtual ~PhotoRepository() = default;
    virtual void Get(PhotoId, std::string owner, Deadline,
                     Completion<PhotoView>) = 0;
    virtual void InsertImported(ImportedPhoto, Deadline,
                                Completion<PhotoId>) = 0;
    virtual void UpdateWithInvalidation(UpdatePhotoIntent, Deadline,
                                        Completion<PhotoView>) = 0;
};
class TaskRepository {
public:
    virtual ~TaskRepository() = default;
    virtual void CreateAtomic(CreateTaskIntent, Deadline,
                              Completion<CreateTaskResult>) = 0;
    virtual void Get(TaskId, std::string owner, Deadline,
                     Completion<TaskView>) = 0;
    virtual void RetryAtomic(RetryTaskIntent, Deadline,
                             Completion<TaskView>) = 0;
    virtual void RetryAtomic(RetryTaskIntent, Deadline,
                             Completion<TaskView>) = 0;
    virtual void FindRequest(std::string owner, std::string idempotency_key,
                             Deadline, Completion<IdempotencyRecord>) = 0;
    virtual void ClaimReady(std::string worker_id, Deadline,
                            Completion<std::optional<TaskClaim>>) = 0;
    virtual void Renew(TaskClaim, Deadline, Completion<Unit>) = 0;
    virtual void Complete(TaskClaim, PublishedOutput, Deadline,
                          Completion<Unit>) = 0;
    virtual void ScheduleRetry(TaskClaim, FailureInfo, Deadline,
                               Completion<Unit>) = 0;
    virtual void RequeueExpired(std::size_t limit, Deadline,
                                Completion<std::size_t>) = 0;
};
class OutboxRepository {
public:
    virtual ~OutboxRepository() = default;
    virtual void ClaimBatch(std::size_t limit, Deadline,
                            Completion<std::vector<OutboxClaim>>) = 0;
    virtual void MarkDelivered(EventId, std::string claim_token, Deadline,
                               Completion<Unit>) = 0;
    virtual void Reschedule(OutboxClaim, FailureInfo, Deadline,
                            Completion<Unit>) = 0;
    virtual void DeliverLocalAtomic(OutboxClaim, Deadline,
                                    Completion<Unit>) = 0;
};
class AdmissionRepository {
public:
    virtual ~AdmissionRepository() = default;
    virtual void AdmitAtomic(ReceivedEvent, Deadline,
                             Completion<AdmissionResult>) = 0;
    virtual void QuarantineAtomic(InvalidRecord, Deadline,
                                  Completion<Unit>) = 0;
};
class PhotoCache {
public:
    virtual ~PhotoCache() = default;
    virtual void Get(PhotoId, std::string owner, Deadline,
                     Completion<std::optional<PhotoView>>) = 0;
    virtual void Put(CacheSnapshot, Deadline, Completion<Unit>) = 0;
    virtual void Invalidate(PhotoId, std::string owner, Deadline,
                            Completion<Unit>) = 0;
};
} // namespace phototask
```

方法名后的 Atomic 是事务契约，表示数据库内部一起提交，不包含外部文件/Redis/Kafka。若回调不确定提交结果，返回 CommitUnknown；不可先把 intent 放进内存队列就回成功。

`CreateTaskIntent` 由 TaskService 构造，包含 task_id/event_id、owner/key/指纹、冻结 params、dispatch_mode、事件 bytes。Repository 负责事务与约束，TaskService 负责 HTTP 无关的用例规则。

### 23.9 应用 Service 与依赖保护

对应文件：`application/photo_service.*`、`task_service.*`、`dependency_guard.*`。

```cpp
namespace phototask {
struct RequestContext {
    std::string request_id;
    std::string owner_scope;
    Deadline deadline;
};
class PhotoService {
public:
    void Get(PhotoId, ReadConsistency, RequestContext, Completion<PhotoView>);
    void Update(UpdatePhotoCommand, RequestContext, Completion<PhotoView>);
};
class TaskService {
public:
    void Create(CreateTaskCommand, RequestContext, Completion<CreateTaskResult>);
    void Get(TaskId, RequestContext, Completion<TaskView>);
    void Retry(RetryTaskCommand, RequestContext, Completion<TaskView>);
    void Retry(RetryTaskCommand, RequestContext, Completion<TaskView>);
    void FindCreation(std::string idempotency_key, RequestContext,
                      Completion<IdempotencyRecord>);
};
class AdmissionLimiter {
public:
    std::optional<CapacityPermit> TryAcquire();
    std::size_t Inflight() const noexcept;
};
class CircuitBreaker {
public:
    bool AllowRequest(chaosproxy::TimePoint now);
    void RecordSuccess(chaosproxy::TimePoint now);
    void RecordFailure(chaosproxy::TimePoint now);
};
class SingleFlight {
public:
    void JoinOrStart(PhotoKey, RequestContext,
                     std::function<void(Completion<PhotoView>)> start,
                     Completion<PhotoView> waiter);
};
} // namespace phototask
```

CircuitBreaker 的参数表示当前单调时刻，和请求截止时间分别传递。不同实例分别维护 breaker/limiter，不能声称跨进程硬配额由这个类自动保证。

服务返回响应后，尚未完成的底层操作仍持 CapacityPermit；真实完成/销毁后才释放。每个 waiter 有自己的超时，leader 操作拥有独立且有界的剩余期限。

### 23.10 MySQL 与 Redis 具体类

对应文件：`infra/mysql_store.*`、`redis_cache.*`。

`MysqlStore final` 实现 PhotoRepository、TaskRepository、OutboxRepository、AdmissionRepository；拥有 Drogon DbClientPtr，单个 Transaction 只在一次事务操作状态中持有。各状态机方法不得跨连接提交同一事务。

| 方法 | SQL/调用次序 | 成功后的下游 |
|---|---|---|
| CreateAtomic | BEGIN → photo 校验 → task → 幂等记录 → Outbox → COMMIT | TaskService 回 202 |
| UpdateWithInvalidation | 条件 UPDATE photo → INSERT invalidation → COMMIT | 返回新版本；Dispatcher 删除缓存 |
| ClaimReady | 锁 READY 行 → epoch++ → lease → attempt → COMMIT | TaskExecutor 获得 TaskClaim |
| Complete | 校验 generation/epoch/worker/lease → task result + attempt → COMMIT | API 可见结果 |
| ClaimBatch | SKIP LOCKED → claim_token/lease → COMMIT | Dispatcher 外部动作 |
| AdmitAtomic | 检查/插入 inbox → 校验 task generation → READY → COMMIT | Consumer 更新连续 offset |

`RedisPhotoCache final` 实现 PhotoCache，拥有 RedisClientPtr；key 编码、JsonCpp 解析、TTL 与版本 CAS 集中在这里。Redis 错误转 Status，是否回源由 PhotoService 决定，adapter 不擅自查 MySQL。

Drogon transaction commit callback、SQL timeout、Redis timeout 与 pending request 行为在 B0 做固定版本实验。文档端口不是对原生库方法的替代证明。

### 23.11 OutboxDispatcher 与 KafkaPublisher

对应文件：`application/outbox_dispatcher.*`、`kafka/kafka_publisher.*`。

```cpp
namespace phototask {
class EventPublisher {
public:
    virtual ~EventPublisher() = default;
    virtual void Publish(Event, Deadline, Completion<PublishReceipt>) = 0;
};
class OutboxDispatcher {
public:
    void Start();
    void Tick();
    void Dispatch(OutboxClaim);
    void OnDelivered(OutboxClaim, StatusOr<PublishReceipt>);
    void StopClaiming();
    void Drain(Deadline, Completion<Unit>);
};
class KafkaPublisher final : public EventPublisher {
public:
    void Publish(Event, Deadline, Completion<PublishReceipt>) override;
    void Poll();
    void FlushUntil(Deadline, Completion<Unit>);
};
} // namespace phototask
```

Dispatcher 路由：CACHE_INVALIDATE → PhotoCache::Invalidate；LOCAL_TASK → DeliverLocalAtomic；KAFKA_TASK → KafkaPublisher::Publish。不同外部语义不硬塞成一个“执行成功即全部成功”的接口。

KafkaPublisher 持有 rd_kafka_t RAII handle 与 delivery callback 的 opaque 操作状态；仅 Poll/delivery 后完成消息回调。返回队列满错误时不丢 Outbox claim；由 Dispatcher 退避重新调度。

### 23.12 FileStore、Processor 与 Executor

接口 FileStore、TaskProcessor 以及 InputHandle/TempOutput/PublishedOutput 的库无关类型声明放在 `application/ports.h`；FilesystemStore 的实现放 `infra/filesystem_store.*`，两个 Processor 实现放 `infra/image_processors.*`，执行器放 `application/task_executor.*`。

```cpp
namespace phototask {
class FileStore {
public:
    virtual ~FileStore() = default;
    virtual StatusOr<ImportedPhoto> Import(const ImportCommand&, ProcessingBudget&) = 0;
    virtual StatusOr<InputHandle> OpenInput(const TaskSpec&) = 0;
    virtual StatusOr<TempOutput> CreateAttemptOutput(const TaskClaim&) = 0;
    virtual StatusOr<PublishedOutput> PublishAttempt(TempOutput, const OutputDigest&) = 0;
    virtual StatusOr<InputHandle> OpenSelectedResult(const TaskView&) = 0;
    virtual Status CleanupOrphan(const OrphanRecord&) = 0;
};
// FilesystemStore final : public FileStore implements these methods.
class TaskProcessor {
public:
    virtual ~TaskProcessor() = default;
    virtual Status Execute(const TaskSpec&, const InputHandle&,
                           TempOutput&, ProcessingBudget&) = 0;
};
class ThumbnailProcessor final : public TaskProcessor {
public:
    Status Execute(const TaskSpec&, const InputHandle&,
                   TempOutput&, ProcessingBudget&) override;
};
class MetadataProcessor final : public TaskProcessor {
public:
    Status Execute(const TaskSpec&, const InputHandle&,
                   TempOutput&, ProcessingBudget&) override;
};
class TaskExecutor {
public:
    void Start();
    void PollReady();
    void RunClaim(TaskClaim);
    void Heartbeat(TaskClaim);
    void OnGenerated(TaskClaim, StatusOr<PublishedOutput>);
    void StopClaiming();
    void Drain(Deadline, Completion<Unit>);
};
} // namespace phototask
```

TaskExecutor 仅持有 FileStore/TaskProcessor 端口引用，phototask_core 不 include infra 头文件，不反向调用具体实现的非虚函数，避免静态库循环依赖。FilesystemStore final : public FileStore 在 filesystem_store.h 中逐项 override 上述接口。

TempOutput/InputHandle 使用 move-only RAII；同步调用借用，跨线程投递转移所有权。文件操作替身仅用于必要的 I/O 故障测试，真实实现仍使用 Linux 文件接口。

TempOutput/InputHandle 使用 move-only RAII；同步调用借用，跨线程投递转移所有权。文件操作替身仅用于必要的 I/O 故障测试，真实实现仍使用 Linux 文件接口。

本版确定 Processor 写入执行器提供的 TempOutput；不再同时支持返回任意自建文件路径的第二套 Processor 接口。输入 handle 拥有 fd；Processor 不关闭借用 fd、不自行标任务成功。

TaskExecutor 在 I/O loop claim 与续租，在有界 CPU pool 调 Processor；处理结束后回 I/O loop做 Complete。文件读写/同步在 Worker 工作线程执行，不阻塞数据库/HTTP loop。

PublishAttempt 只完成 attempt 私有文件的持久发布；Complete 才选择用户可见结果。任何新代码都必须保留这两个不同成功含义。

### 23.13 TaskAdmission、KafkaConsumer 与分区状态

对应文件：`application/task_admission.*`、`kafka/kafka_consumer.*`。

```cpp
namespace phototask {
class TaskAdmission {
public:
    void Accept(KafkaRecord, Deadline, Completion<AdmissionResult>);
};
struct PartitionProgress {
    std::int32_t partition;
    std::int64_t next_committable_offset;
    std::uint64_t assignment_epoch;
    bool admission_inflight;
};
class KafkaConsumer {
public:
    void Poll();
    void OnAssigned(const Assignment&);
    void OnRevoked(const Assignment&);
    void OnRecord(KafkaRecord);
    void OnAdmitted(RecordIdentity, std::uint64_t assignment_epoch,
                    StatusOr<AdmissionResult>);
    void CommitContiguous();
    void StopAdmission();
};
} // namespace phototask
```

OnRecord 做大小检查与拥有 payload 的拷贝/移动，交给 TaskAdmission；Accept 校验 envelope 与 task 快照，调用 AdmitAtomic；事务确认后才回 OnAdmitted。后者检查 assignment_epoch，更新该分区连续前缀，再 CommitContiguous。

offset 数值是下一条要读的位置；不能提交当前 record offset 后误以为已跳过它。assignment_epoch 是本地消费者分配代数，不是业务 task owner_epoch，两个身份不可共用。

### 23.14 HTTP 路由与 Runtime 组装

对应文件：`http/http_handlers.*`、`infra/photo_runtime.*`、`apps/photo_*_main.cpp`。

| handler | 提取输入 | 调用 | 回调结果 |
|---|---|---|---|
| HandleGetPhoto | owner、photo_id、consistency、deadline | PhotoService::Get | 200 + metadata_version |
| HandleUpdatePhoto | expected_version、description、tags | PhotoService::Update | 200 或 409 |
| HandleCreateTask | Idempotency-Key + typed params | TaskService::Create | commit 确认后 202 |
| HandleGetTask | owner、task_id | TaskService::Get | 权威状态 |
| HandleFindCreation | owner、key | TaskService::FindCreation | 同 key 原 task |
| HandleRetryTask | 管理 token、task_id、expected_state、操作 key | TaskService::Retry → RetryAtomic | 同 task 新 generation |
| HandleRetryTask | 管理 token、task_id、expected_state、操作 key | TaskService::Retry → RetryAtomic | 同 task 新 generation |
| HandleGetResult | task_id → TaskService::Get | FilesystemStore::OpenSelectedResult | 有界流式文件响应 |

`RegisterPhotoRoutes(drogon::HttpAppFramework&, PhotoServices&)` 在启动时绑定 callback，捕获 runtime 保证存活的服务引用。响应写一次；下载不把整个图片读成一个巨大 std::string。

`PhotoRuntime` 是 composition root：Create(config, role) → 创建当前进程需要的 DbClient/RedisClient/服务 → Start → BeginShutdown → Drain。API 不启动 Worker CPU pool；Worker 不注册对外 HTTP 路由；Dispatcher 不生成图片。

main 只做 CLI/config/logging、Runtime 构造和退出；不包含 SQL、缓存回源或图像循环。

组装边界：PhotoRuntime 只创建 infra/core 组件，不从 infra 反向引用 phototask_http 或 phototask_kafka。photo-api main 调 RegisterPhotoRoutes；dispatcher/worker main 在 CP_HAS_KAFKA 下构造 KafkaPublisher/KafkaConsumer 并注入 EventPublisher/TaskAdmission。对应 app 持有这些对象直至 drain 完成。

## 24. 从空工程开始的教学阶段与编码交付

### 24.1 里程碑与入口

| 里程碑 | 必须能演示的结果 | 组别 |
|---|---|---|
| M0 | 新仓库、固定依赖、公共错误模型、CLI 和最小测试 | A0.1～A0.6 |
| M1 | 正确、有界、公平的双向 TCP 转发 | A1～A4 |
| M2 | 全部首版故障策略、控制 ACK、代理证据 | A5～A8 |
| M3 | 真实照片输入、MySQL 任务和本地执行闭环 | B0～B6 |
| M4 | Redis 缓存、降级和可靠失效 | C1～C4 |
| M5 | 双 API、Nginx、自动故障实验 | D1～D4、E1～E3 |
| M6 | Kafka 发布、持久接入、任务执行和恢复 | F1～F6 |
| M7 | 实测性能、可重复交付和面试证据 | G1～G4 |

本版新代码起点是 A0.1，所有实现与测试初始标为未开始。学习者已掌握的理论可以快讲，但不能将旧项目代码或聊天中的“完成”直接算成本仓库产物。后续换窗口以新仓库 state.md 的真实记录续接，不反复回到 A0。

### 24.2 A0：工程基础拆分

| 组 | 新建/修改文件 | 核心函数或动作 | 交付与验收 |
|---|---|---|---|
| A0.1 | 根 CMakeLists、Presets、vcpkg.json、third_party/vcpkg、docs/DEPENDENCIES.md | 固定 submodule/baseline，选择 proxy-debug | 保存真实 commit、依赖解析；此时尚无业务 target |
| A0.2 | common/status.h、src/common/status.cpp | Status、StatusFromErrno、ExitCodeForStatus | 错误上下文保留，退出码映射唯一 |
| A0.3 | common/status_or.h、completion.h | 移动值、错误值、value 访问契约 | move-only 类型可返回；OK 无值非法 |
| A0.4 | common/logging.h、src/common/logging.cpp | InitLogging | 明确异步日志容量/满队列策略，不阻塞网络 loop |
| A0.5 | tests/CMakeLists、unit/common/status_test.cpp | 首个 common_tests | configure/build/ctest 实际通过 |
| A0.6 | apps/chaosproxy_main.cpp、chaosctl_main.cpp、apps/CMakeLists | CLI11 参数，help/version，统一退出码 | main 只组装；无配置时给清晰错误 |

表中 common 路径在 include/chaosproxy 下，其他路径以第 4 节为准。A0 使用当期已存在的源码列表，不能照抄最终 src/CMakeLists 把未来文件提前加入后制造编译失败；每组结束显式增补当前 target。临时启动骨架在 A1～A7 中替换，不保留第二套演示入口。

### 24.3 A：网络核心逐组落地

| 组 | 文件落点 | 实现顺序 | 必须通过的行为 |
|---|---|---|---|
| A1 | unique_fd.h、proxy/socket_ops.*、types.h | UniqueFd → CreateListener/TryAccept → StartConnect/FinishConnect → TryRead/TryWrite | NONBLOCK/CLOEXEC；EINTR/EAGAIN/EOF 区分；失败无 fd 泄漏 |
| A2 | event_loop.*、connection_table.*、connection_pair.* | EventToken → AddFd → Insert/Find → OnReadable/OnWritable → Pump | 多连接双向 Echo；旧 generation 事件无效 |
| A3 | buffer.*、connection_pair.* | allocation 预算 → ByteQueue → 短写 Consume → 水位 → TryFinishDirection | P01～P04；慢接收方不丢尾部；FIN 不截断延迟数据 |
| A4 | event_loop.*、connection_pair.* | 去重 ReadyQueue → PumpBudget → 主动续跑 → 控制预算 | P05；ET 提前退出仍有进展；大流不饿死小流 |
| A5 | clock.h、timer_queue.* | FakeClock → heap → Cancel → timerfd → RunExpired | 回调有预算；关闭后的定时事件不触碰新连接 |
| A6 | toxic_pipeline.*、toxics.* | continuation → latency/jitter → token bucket → slicer → pause/timeout/close/reset/limit_data | 每项正常/边界用例；顺序、实际发送字节计数正确 |
| A7 | config.*、proxy_server.*、control/*、chaosctl_main.cpp | 多 Listener → JSON → UDS framing → expected_version → 应用 ACK | P06～P08；短读短写；冲突/超大帧；旧 Chunk 快照 |
| A8 | trace_writer.*、benchmarks/run_proxy.py、代理集成测试 | 有界 trace → 耗尽实验 → drain → 基线 | fd/内存/CPU 有界；报告区分丢失 trace 与完整记录 |

每组依次提交“值类型与接口 → 一条正常路径 → 关键失败路径 → 测试证据”。不先写完几千行再统一接线。缺少 A3 不进入延迟队列；缺少 A4 不开始并发限速压测。

### 24.4 B：真实照片任务与 MySQL

| 组 | 文件落点 | 函数与顺序 | 交付与失败回退 |
|---|---|---|---|
| B0 | photo-runtime、mysql_store、临时集成 probe | 固定 Drogon 建连、事务提交回调、超时；Protobuf 生成/解析 | 确认实际 API；不清楚 commit 语义时停止写 CreateAtomic |
| B1 | model/*、001_core.sql、filesystem_store.*、photo_admin_main.cpp | 强类型 → migration → Import → InsertImported | 实际图片可查；坏输入与重复导入规则明确 |
| B2 | ports.h、task_service.*、mysql_store.*、http_handlers.* | Normalize → FingerprintV1 → CreateAtomic → HTTP 202 | 同 key 同指纹并发只创建一次；不同指纹 409 |
| B3 | outbox_dispatcher.*、task_executor.*、mysql_store.* | ClaimBatch → DeliverLocalAtomic → ClaimReady | 不启动 Kafka，任务仍从 PENDING_DISPATCH 进入 RUNNING |
| B4 | image_processors.*、filesystem_store.*、task_executor.* | 缩略图 Execute → PublishAttempt → Complete；再加入元数据处理 | 真图片输出；大小、像素、内存预算；错误不伪装成功 |
| B5 | mysql_store.*、task_executor.*、worker 集成测试 | Renew → ScheduleRetry → RequeueExpired → 新 epoch | W01～W04；旧 owner 无法提交；无无限重试 |
| B6 | task_service.*、场景 hooks、提交不确定测试 | COMMIT 窗口故障 → FindRequest → 原 key 重试 | B05～B08；无法证明提交窗口命中则实验不算通过 |

B0 probe 是短期调查，确定行为后把必要断言并入集成测试，不长期保留一套并行业务 Demo。B4 可以先完成 THUMBNAIL 再加入 EXTRACT_METADATA；它们使用同一个执行协议。

### 24.5 C / D：缓存和入口

| 组 | 文件落点 | 核心函数/数据流 | 出口 |
|---|---|---|---|
| C1 | redis_cache.*、photo_service.*、002_cache.sql | Get → miss → DB → Put；strong 绕缓存 | 命中/陈旧窗口和版本可见 |
| C2 | dependency_guard.*、photo_service.* | deadline → breaker → singleflight → permit → DB | B01/B02；超时后迟到操作仍受容量约束 |
| C3 | mysql_store.*、outbox_dispatcher.* | UpdateWithInvalidation → Invalidate → MarkDelivered | B03；Redis 恢复后最终删除 |
| C4 | 缓存集成测试、scenarios/redis_slow.json | 旧读晚回填、并发两实例 | B04；TTL 不宣称强一致 |
| D1 | deploy/compose.yaml、photo_runtime.* | 两 API 连接同 DB/卷 | 跨实例同幂等键返回同任务 |
| D2 | deploy/nginx.conf、configs/proxy.json | Nginx → api-a/api-b proxy → API | 单链路注入不影响另一链路路由 |
| D3 | nginx.conf、HTTP deadline 处理 | GET 有界重试；写请求关闭自动重试 | N01/N02；不放大写次数 |
| D4 | http_handlers.*、日志/指标 | livez/readyz/metrics | 故障不造成探针重启风暴 |

### 24.6 E：场景代码也有边界

| 组 | 文件落点 | 具体实现 | 出口 |
|---|---|---|---|
| E1 | tools/scenarios/runner.py、scenarios/*.json | parse → await-ready → control → request → assert → finally restore | 不使用任意 shell 字符串作为场景指令；统一超时和退出码 |
| E2 | 测试 hook、observer 配置、mysql_commit_unknown.json | 就绪屏障 → 故障应用 ACK → 窗口触发 → 独立查询 | 命中和未命中分别记录 |
| E3 | tests/integration、runs/、报告模板 | 批量矩阵 → 汇总结果 → 原始日志关联 | 报告有真实 seed/config/build/时间，不编造通过 |

E1 可在 A7 后开展；E2 随 B6 推进。Runner 用 Python 标准库和 CLI 协调即可，不另写 C++ 通用工作流引擎。未完成模块的场景标 skipped 并给原因，不能混入 passed。

### 24.7 F：Kafka 具体接入顺序

| 组 | 文件落点 | 核心调用 | 出口 |
|---|---|---|---|
| F1 | kafka.env.example、compose.yaml、proto、kafka/*.h | broker 地址映射 → client metadata 检查 → 事件解析 | 所有客户端 broker 路径均经代理；不只测 bootstrap |
| F2 | kafka_publisher.*、outbox_dispatcher.* | produce → Poll → delivery → MarkDelivered | K01/K02；发送本地入队不算发布成功 |
| F3 | task_admission.*、mysql_store.*、003_inbox.sql、kafka_consumer.* | OnRecord → AdmitAtomic → OnAdmitted → CommitContiguous | inbox + READY 已提交后才 offset+1 |
| F4 | kafka_consumer.*、隔离表方法 | assignment_epoch → pause → revoke → quarantine | K03～K06；撤销后旧回调不提交；队列有界 |
| F5 | worker/dispatcher 重启测试 | 原 event_id 重投、旧 generation、未知提交 | K07；一个逻辑结果；无静默丢任务 |
| F6 | kafka_duplicate.json、积压脚本、多 broker 配置 | backlog → 恢复；资源允许才做 quorum 实验 | K08；单节点和三节点报告分开 |

Kafka 编码完成前，本地模式已经能处理照片。KafkaConsumer 只负责持久接入，不长期占住分区等待图像计算；TaskExecutor 从 MySQL READY claim 执行。切换 dispatch_mode 不允许双发两份任务事件，存量 Outbox 按自身 destination 处理。

手动 RetryAtomic 在事务内校验管理操作 key、预期终态和 owner，增加 generation、重置执行字段并插入新 Outbox。管理操作 key 的唯一记录必须持久化，可在 001_core.sql 增加 admin_requests(owner_scope,operation_key,task_id,requested_generation,result_generation,response_json)，不能用进程内 map 去重。核心请求幂等表的 task 创建语义不与管理操作混用。

手动 RetryAtomic 在事务内校验管理操作 key、预期终态和 owner，增加 generation、重置执行字段并插入新 Outbox。管理操作 key 的唯一记录必须持久化，可在 001_core.sql 增加 admin_requests(owner_scope,operation_key,task_id,requested_generation,result_generation,response_json)，不能用进程内 map 去重。核心请求幂等表的 task 创建语义不与管理操作混用。

### 24.8 G：性能与最终交付

G1：保存代理无故障基线、延迟误差、公平性、任务吞吐和故障恢复原始数据。G2：只有 profile 证明瓶颈后引入 buffer pool 或多 EventLoop；一次改变一个主要变量。G3：新环境安装、配置、迁移、运行、关停、测试一遍，清理无用层。G4：每条简历描述绑定 commit、测试场景、结果文件和能力边界。

单组失败就退回它的直接前置：字节顺序错退 A3/A6，控制 ACK 错退 A7，幂等重复退 B2，旧 owner 提交成功退 B5，offset 越过未入库消息退 F3/F4。不要通过增加新的中间件绕开未解决的正确性问题。

## 25. 代码收敛与教学规则

### 25.1 从零实现与第三方库的边界

Reactor、Direction 生命周期、Buffer/背压、TimerQueue、Token Bucket、continuation、场景应用协议；业务侧实现 deadline/准入、幂等事务、Outbox claim、任务 lease 与结果条件提交。

采用成熟第三方库：HTTP/MySQL/Redis/Kafka 协议、JSON、Protobuf、图像编解码、日志、测试框架。既不“所有东西都自己写”，也不把决定正确性的协议全部交给未经理解的框架。

### 25.2 抽象准入

每个新抽象必须回答：隔离哪种变化？由哪些调用者使用？哪条必要测试需要替换它？若三者都没有具体答案，先用普通函数/具体类。

- 不按 SQL 表机械创建 DAO + Repository + Service + Manager 四层。
- 不把每个状态转换封成一个虚函数对象。
- CPU pool 与 I/O loop 各自有明确用途；不为展示线程池再包一层网络任务池。
- 一个中间件一个薄适配边界，不为每个命令写全套通用框架。
- 共享 helpers 只在存在真实重复时提取；错误上下文不能被过度公共化吞掉。
- tests 验证行为、不变量和故障窗口，不镜像 private 实现。

### 25.3 每组授课固定格式

1. 当前进度与本组唯一问题（最多两句）。
2. 当前相关目录结构、本组文件完整路径。
3. 数据/控制路径；只解释新增机制。
4. 核心函数签名与不变量，先于完整实现。
5. 新增代码或 diff；区分概念/项目/伪代码。
6. 错误路径与一个关联场景。
7. 实际构建/验证结果与下一步。

强相关知识可合并；已掌握的短写、RAII、回调语义只引用，不从定义重讲。用户提交代码时集中指出语法、逻辑、边界、生命周期和复杂度问题，不每次只给一个小错误。

不需要每个小组最后都强制进行长问答；在完整协议或里程碑处集中验收。没提供真实命令结果时写“待用户运行”，不自称通过。

## 26. 长期文件职责与换窗口协议

### 26.1 文件职责

| 文件 | 保存什么 | 不保存什么 |
|---|---|---|
| 本文 | 已确认架构、契约、实施顺序 | 每次聊天流水账、完整代码 |
| state.md | 真实进度、commit、通过/失败测试、下一组文件 | 全量教学内容 |
| docs/DECISIONS.md | 取舍、理由、替代方案与重评条件 | 只有结论没有动机 |
| docs/BUG_LEDGER.md | 症状、复现、根因、修复、回归测试 | 虚构“曾经遇到” |
| 仓库源文件 | 真实实现 | 与文档并行维护的第二套假代码 |
| runs/ | 原始数据、环境和配置 | 编造的优化百分比 |

如果聊天无法直接读取仓库，按模块导出源码，顶层构建文件和目录索引常驻；只带下一组的直接依赖。已有代码汇总过长时拆模块，不每次上传七千行全量文本。

### 26.2 state.md 模板

```text
项目：ChaosProxy + PhotoTask
蓝图版本：V2.1
真实仓库/分支：待填写
当前 commit：尚未创建新仓库
已实现能力：无；蓝图完成不等于代码完成
当前里程碑/组：M0 / A0.1 未开始
当前唯一问题：创建空工程并写入固定构建依赖
本组已改文件：
已通过测试与命令：
失败/未执行测试：
已测性能与原始数据路径：
当前已确认 Decision：
已知 Bug/风险：
下一组目标：
下次必须提供文件：
下次可省略的已验证模块：
```

换窗口前：更新 state → 检查代码与状态一致 → 指定下一组读取文件 → 给一段可复制交接指令。本文仅在架构或契约变化时更新，不每次修改整个蓝图。

## 27. 简历与面试可讲内容

### 27.1 三条证据链

- 网络：慢消费者/半关闭异常 → 缓冲与生命周期根因 → 修复 + 回归 → 代理行为正确。
- 业务：Redis 慢导致回源堆积 → deadline/准入/合并 → 对比资源和失败率 → 依赖故障不扩散成无限积压。
- 可靠性：写入确认丢失/消息重复/旧 Worker 晚到 → 幂等、Outbox、inbox、epoch 条件提交 → 只产生一个业务可见结果。

每条都要指出：哪段自己实现、哪段第三方库提供、测试观察到了什么、承诺截止到哪里。

### 27.2 重点追问

1. 为什么用户态 TCP 代理不能模拟真实 TCP 丢包？
2. ET 提前让出以后，谁保证继续处理？
3. token 不足却 Socket 可写，怎么避免忙等？
4. FIN 到达但延迟队列非空，什么时候 shutdown？
5. 同一个 seed 为什么可能不能复现同一真实故障位置？
6. 热更新时旧 Chunk 怎么避免被新 Chunk 超越？
7. Redis 失败后为何不能无条件查 MySQL？
8. TTL 和可靠删除为什么仍不等于强一致？
9. COMMIT_UNKNOWN 为什么不能直接换 key 再创建？
10. Outbox 已发布但未标记，谁处理重复？
11. Kafka offset 为什么在 admission 后提交，而非图像完成后？
12. 消费分区撤销时旧回调还能提交位点吗？
13. 旧 Worker 为什么不能覆盖新结果？数据库 epoch 能阻止文件写入吗？
14. Nginx 和应用各重试三次会发生什么？
15. 单 broker acks=all 能证明多副本容错吗？
16. 使用 Drogon 后，哪些工作仍能体现你的 C++ 能力？
17. 业务任务表和 Kafka 各自承担什么职责，是否存在重复复杂度？
18. 如果没有 Kafka，现有业务是否仍可运行？为什么还选择接入它？

可以说“比较过方案，并选择……”。未实现过的旧方案不能描述成实际优化历史；性能数字必须来自保存的实验。项目价值不依赖虚构生产规模。

## 28. 官方资料与能力边界

这些资料用于核查系统调用/客户端能力；架构选择与实验参数属于本文设计判断，不代表官方给本项目性能背书。

- [Toxiproxy](https://github.com/Shopify/toxiproxy)：成熟故障注入工具，作为职责和功能参照，不把同类功能说成独创。
- [Linux epoll](https://man7.org/linux/man-pages/man7/epoll.7.html)：ET、就绪处理和饥饿问题。
- [Linux fsync](https://man7.org/linux/man-pages/man2/fsync.2.html)：文件与目录持久化边界。
- [Drogon 安装与依赖](https://github.com/drogonframework/drogon/wiki/ENG-02-Installation)：数据库/Redis 可选功能。
- [Drogon DbClient](https://github.com/drogonframework/drogon/wiki/ENG-08-1-Database-DbClient)：异步客户端适配入口。
- [MySQL 锁定读](https://dev.mysql.com/doc/refman/8.4/en/innodb-locking-reads.html)：FOR UPDATE、SKIP LOCKED 的使用范围。
- [Redis Cache-Aside](https://redis.io/docs/latest/develop/use-cases/cache-aside/)：读缓存模式。
- [Nginx proxy 模块](https://nginx.org/en/docs/http/ngx_http_proxy_module.html)：转发、超时和重试边界。
- [Protobuf proto3](https://protobuf.dev/programming-guides/proto3/)：消息与字段演进。
- [Protobuf 非规范化序列化](https://protobuf.dev/programming-guides/serialization-not-canonical/)：稳定摘要不能仅依赖重序列化。
- [librdkafka 配置](https://docs.confluent.io/platform/current/clients/librdkafka/html/md_CONFIGURATION.html)：投递、队列、位点与消费者参数。
- [Kafka broker 配置](https://kafka.apache.org/41/configuration/broker-configs/)：监听、公布地址与副本相关配置。

- [固定 vcpkg baseline](https://github.com/microsoft/vcpkg/blob/9e593bb18ea69cc5095e012465dcd675a822ed0d/versions/baseline.json)：版本表与 overrides 的查询依据。
- [固定 ports](https://github.com/microsoft/vcpkg/tree/9e593bb18ea69cc5095e012465dcd675a822ed0d/ports)：Drogon features、依赖与 CMake 导出依据。
- [固定 vcpkg tool metadata](https://github.com/microsoft/vcpkg/blob/9e593bb18ea69cc5095e012465dcd675a822ed0d/scripts/vcpkg-tool-metadata.txt)：bootstrap 工具版本。
- [vcpkg 版本管理](https://learn.microsoft.com/en-us/vcpkg/users/versioning)：baseline、version>= 与 overrides 的不同含义。
- [Docker Hub MySQL tag 元数据](https://hub.docker.com/v2/repositories/library/mysql/tags/8.4.7)、[Redis](https://hub.docker.com/v2/repositories/library/redis/tags/7.4.7)、[Nginx](https://hub.docker.com/v2/repositories/library/nginx/tags/1.28.2)、[Kafka](https://hub.docker.com/v2/repositories/apache/kafka/tags/4.1.1)：本轮镜像 tag 与摘要查询。
- [Prometheus 镜像元数据](https://hub.docker.com/v2/repositories/prom/prometheus/tags/v3.5.0)、[Grafana](https://hub.docker.com/v2/repositories/grafana/grafana/tags/12.1.1)：可选监控镜像查询。

## 29. 新窗口启动提示词

> 按本蓝图 V2.1 从空工程实现 ChaosProxy + PhotoTask。不要迁移旧 ChaosProxy/PhotoBridge 代码，不设置旧代码审计或兼容接口阶段；采用本蓝图已固定的第三方库，并遵守第 4.13 节的低内存开发限制。
> 首次从 A0.1 开始；已有本版 state.md 时读取它和本组源码，再继续真实进度。源码和实际测试决定完成事实。
> 每组先列具体工程路径、类函数、调用链、所有权与错误出口，再给这一组可编译的实现；不要提前生成未来阶段的空框架。
> 使用第 4 节的 vcpkg commit、Manifest、Presets 和 target 边界。阶段性 CMake 只列当前已有源码。
> 本次未实际执行的构建和测试明确标未执行；每组结束更新 state.md、Decision/Bug 变更与下一组文件。
> 面对目录、签名与数据流冲突，先形成一个一致设计再编码；不保留两套同义接口，不反向连接 core 与 infra。
> 依次解释正常路径、超时/断线/崩溃路径和恢复依据。已掌握的理论快讲，不能跳过项目特定的不变量。


## 30. 函数级数据流、线程交接与完成条件

下表是实现时的接线图；第 23 节规定接口，第 11～16 节规定 SQL 与外部协议。每一步都需要能通过日志 request_id/task_id/connection token 追踪，但不打印图片、SQL 密码或完整消息正文。

### 30.1 代理启动与连接建立

| 步骤 | 调用与数据 | 所在线程/所有权 | 完成条件 |
|---|---|---|---|
| 1 | main → LoadProxyConfig → ValidateProxyConfig | 启动线程，值对象 | 端口、预算、方向、策略合法 |
| 2 | ProxyServer::Start → CreateListener → EventLoop::AddFd | owner loop | 每个 Listener 有 ProxyId 与固定上游 |
| 3 | listener readable → TryAccept → StartConnect | owner loop | 两端 UniqueFd 已接管，connect 可处于 EINPROGRESS |
| 4 | ConnectionTable::Insert → 注册两个 EventToken | 表拥有 ConnectionPair | 注册失败走统一 Close/回收 |
| 5 | upstream writable → FinishConnect(SO_ERROR) | owner loop | 确认 Established 后才正常转发 |

还在 Connecting 时读取客户端数据只能占有有界预连接队列；不得将“fd 可写”直接视为 connect 成功。建连超时也验证连接 token。

### 30.2 从接收字节到对端短写完成

| 步骤 | 调用与数据变化 | 状态/所有权 |
|---|---|---|
| 1 | epoll event → Find(token) → OnReadable → EnqueueReady | event 不持裸 ConnectionPair 指针跨批次 |
| 2 | Pump → BufferBudget::TryReserve → TryRead | recv 之前已有容量；读端仍受水位约束 |
| 3 | actual bytes → Chunk(storage,offset,length,sequence,policy) | 块从独占可写变成共享只读 |
| 4 | ToxicPipeline::Accept → 延迟/切片/输出闸门 | 旧 Chunk 的策略与字节顺序不变 |
| 5 | ByteQueue::FrontBytes → token/预算裁剪 → TryWrite | send 受限速、暂停、可写状态共同约束 |
| 6 | Consume(actual bytes) → bucket Consume → 更新计数 | 未发送尾部留在队列，不按 requested bytes 扣账 |
| 7 | 低水位 → ResumeRead → EnqueueReady；预算耗尽也续跑 | 主动续跑修补 ET 无新边沿问题 |

send 遇 EAGAIN 才等待后续可写事件；token 不足等待 timer；单纯预算耗尽进入就绪轮转。三者不能都简化成“打开 EPOLLOUT”。

### 30.3 延迟释放、半关闭和回收

Latency 保存 Chunk 与 ContinuationToken → TimerQueue::Schedule → timerfd 就绪 → RunExpired → Find(connection) → 验证 operation_id → ToxicPipeline::Resume(next_stage) → 有序输出。过期身份直接丢弃恢复请求并计数，不重新触发延迟。

源 recv==0 → OnSourceEof → 禁止继续读 → 排空 ready/delayed/continuation → TryFinishDirection → ShutdownWrite(目标)。反向读写仍继续；两方向结束或硬错误时 Close → RemoveFd/CancelAll/Retire → 批次结束 ReclaimRetired。BufferBudget 的生命周期必须晚于最后一个 BufferBlock 释放。

### 30.4 控制命令与策略更新

chaosctl CLI → ControlClient::Call → UDS 部分写 → ControlProtocol::Feed/NextCommand → 验证 request_id/expected_version → ProxyServer::ApplyPolicy → 安装新 PolicySnapshot → QueueReply(applied_version) → UDS 部分写 → CLI 成功退出。

单 loop 实现可以直接应用；若控制处理在另一线程，必须 Post 到 owner loop 并等待 owner 应用完成再回复。连接关闭后的回复不能投递给复用同一 fd 的新控制客户端。超时后客户端应查当前版本，不自动假定变更未发生。

### 30.5 资料查询与 Redis 降级

| 步骤 | 类函数 | 结果与下一步 |
|---|---|---|
| 1 | HandleGetPhoto → PhotoService::Get | owner/photo_id/consistency/deadline 成为 RequestContext |
| 2 | strong 则跳缓存；否则 breaker → PhotoCache::Get | 命中校验版本格式后返回；miss/error 进入有界回源 |
| 3 | SingleFlight::JoinOrStart → AdmissionLimiter::TryAcquire | 合并相同 owner/photo 查询；无容量快速失败 |
| 4 | PhotoRepository::Get → MysqlStore → DbClient | DB 是权威值，失败不构造假照片 |
| 5 | RedisPhotoCache::Put(snapshot) | 独立短预算、过期时间受原读取时刻约束；失败不否定成功 DB 读取 |
| 6 | once completion → HTTP callback | 每个 waiter 按自身 deadline 完成；迟到回调只清理资源 |

SQL 库调用与回调走框架 I/O loop；单请求状态在指定应用 loop 修改。回源 permit 直到真实 DB 操作完成才释放。缓存不参与任务状态与幂等结果判断。

### 30.6 幂等创建任务与提交不确定

HandleCreateTask → TaskService::Create → NormalizeTaskRequest → FingerprintV1 → 构造 CreateTaskIntent → MysqlStore::CreateAtomic。

数据库事务顺序：校验冻结输入 → 插入 task → 插入唯一 owner/key 幂等记录 → 插入 Outbox(event_id,destination,payload) → 提交确认。唯一键冲突时回滚本次新记录，读取原 key：指纹相同返回原 task，不同返回 Conflict。并发死锁/锁超时在总预算内按整笔事务重试，不能只补某条 INSERT。

提交确认成功才返回 202。提交结果未知返回 COMMIT_UNKNOWN 与请求查询方式；客户端沿用同一 key。FindCreation 只查 MySQL 主库，发现原记录则返回原 task；短暂未查到也不允许擅自换 key 制造新逻辑请求。

### 30.7 无 Kafka 的执行链

OutboxDispatcher::Tick → ClaimBatch 短事务 → Dispatch(LOCAL_TASK) → DeliverLocalAtomic：校验 claim_token 与 task generation、设置 READY、标 DELIVERED 同事务提交。

TaskExecutor::PollReady → ClaimReady → commit 确认 → RunClaim。LOCAL_TASK 不先标消息完成再调用内存队列；API/Dispatcher 重启后依然能由数据库恢复任务意图。

### 30.8 有 Kafka 的发布与持久接入

| 步骤 | 调用 | 持久性/线程边界 |
|---|---|---|
| 1 | Dispatcher ClaimBatch → KafkaPublisher::Publish | Outbox claim 已提交；payload 有独立所有权 |
| 2 | rd_kafka_producev → Poll → delivery callback | 本地排队≠broker 确认；回调交回 Dispatcher 所属 loop |
| 3 | OnDelivered → MarkDelivered | 原 claim_token 条件更新；崩溃可重复 event_id |
| 4 | KafkaConsumer::Poll → OnRecord → TaskAdmission::Accept | 消费线程持续 Poll；每分区至多一个 admission inflight |
| 5 | 解 Protobuf → AdmitAtomic(inbox + READY) | SQL 提交确认才完成接入；大图计算尚未开始 |
| 6 | OnAdmitted 检查 assignment_epoch → CommitContiguous | 仅推进已持久接入的连续位点，提交 offset+1 |
| 7 | TaskExecutor::PollReady → ClaimReady | 后续与本地模式完全一致的执行协议 |

应用回调不能从 DB loop 直接并发修改消费者分区表：投递到 consumer 所属执行线程。rebalance 后迟到的 DB 成功可以保留，但旧 assignment_epoch 不再提交 offset；新 owner 重读后由 inbox 去重。

### 30.9 图像计算、租约与结果选择

| 步骤 | 调用与数据 | 所在线程 |
|---|---|---|
| 1 | ClaimReady → TaskClaim(task,generation,owner_epoch) | DB/app loop |
| 2 | RunClaim → 有界工作池 → FileStore::OpenInput | 工作线程；校验输入摘要与处理预算 |
| 3 | CreateAttemptOutput → TaskProcessor::Execute | 工作线程；输出只写当前 TempOutput |
| 4 | 计算 digest → FileStore::PublishAttempt | 工作线程；fsync + no-replace rename + 目录同步 |
| 5 | 投递 OnGenerated → TaskRepository::Complete | app loop → DB；检查 owner/epoch/lease 后选择 output |
| 6 | commit 确认 → 标记执行完成/释放执行容量 | 失败文件保留为孤儿候选；未知提交先查 DB |

Heartbeat 独立由 app loop 定时驱动，不等待图像函数返回。失去所有权后结果不能提交；第三方编解码不保证任意时刻可中断，合作式 deadline 不是硬 CPU 隔离。单机受控 fixture 场景用预算限制；需要强制杀死卡住的解码时另加子进程执行设计，不假装线程取消已经解决。

### 30.10 文件结果读取与清理

HandleGetResult → TaskService::Get(owner,task) → 确认 SUCCEEDED + result_key → FileStore::OpenSelectedResult → 安全根内打开/校验 → 框架有界文件响应。URL 不能直接映射整个 result 目录，否则未选择的旧 attempt 文件也会被访问。

清理任务先查询 DB 引用与 attempt 状态，满足保留期才 CleanupOrphan；当前活跃 lease、未知提交待核对、已被 task 引用的输出一律不删。DB 不可用时不做推断式清理。清理与发布竞争通过 attempt 身份和保留窗口规避。

### 30.11 关闭链

先停业务入口与新 claim → consumer StopAdmission、dispatcher StopClaiming、executor StopClaiming → 有界等待已有操作、维持必要 heartbeat/poll → Kafka flush 与 offset 完成回调 → 清理客户端 → 最后结束各 loop 与日志。

超过 drain deadline 时退出并留下可恢复的 lease/Outbox；不能把未投递事件强标 DELIVERED，不能把未选中输出标 SUCCEEDED。ChaosProxy 独立 drain：停止 accept/新控制变更、继续现有双向转发，到期统一关闭并回收 token。

### 30.12 基础配置字段到类成员的映射

`configs/proxy.json` 最小启动模板：

```json
{
  "schema_version": 1,
  "control_socket": "/run/chaosproxy/control.sock",
  "buffer_budget_bytes": 268435456,
  "max_connections": 1024,
  "pump_bytes": 65536,
  "pump_syscalls": 32,
  "proxies": [
    {"id": 1, "name": "redis-api", "listen": "0.0.0.0:16379", "upstream": "redis:6379"},
    {"id": 2, "name": "mysql-api", "listen": "0.0.0.0:13306", "upstream": "mysql:3306"},
    {"id": 3, "name": "api-a", "listen": "0.0.0.0:18081", "upstream": "photo-api-a:8080"},
    {"id": 4, "name": "api-b", "listen": "0.0.0.0:18082", "upstream": "photo-api-b:8080"}
  ]
}
```

这是容器实验网络中的地址，宿主机直接运行时换成该机器能解析的名称/IP。未列出字段采用第 6.3 节默认值；无策略时直接转发。未知 schema_version、重复 ID/name、非法数值、全局预算小于最小保留量均拒绝启动。broker 监听在 F1 按第 16.7 节额外加入。

| 配置项 | 消费者 | 校验/含义 |
|---|---|---|
| proxy buffer_budget_bytes | ProxyServer → BufferBudget | 实际分配字节总上限；不是单方向水位 |
| proxy pump_* | EventLoop → ConnectionPair::Pump | 每轮预算，耗尽必须主动续跑 |
| control_socket | ControlServer/ControlClient | 同一 UDS 路径、权限和独立容量 |
| photo db/redis 地址 | PhotoRuntime → Drogon client | 业务地址只能指向对应 ChaosProxy |
| photo role/dispatch_mode | main → PhotoRuntime/TaskService | role 决定启动组件；模式写进新 Outbox |
| photo input_root/result_root | FilesystemStore | 绝对私有路径，同机共享卷；temp 与目标同文件系统 |
| photo processing_limits | TaskExecutor → ProcessingBudget | 压缩大小、像素、内存、输出大小和 deadline |
| photo worker_id/lease 参数 | TaskExecutor/MysqlStore | 进程身份唯一；租约由 DB 时钟判断 |
| Kafka bootstrap/group/topics | KafkaPublisher/KafkaConsumer | 只在 kafka profile 创建；不能默认绕过代理 |
| observer 地址 | scenario-runner | 单独观测，不注入业务进程配置 |

密码、Bearer Token 通过环境变量名引用，配置与日志不保存明文。运行日志、trace、报告输出到 `runs/<run_id>/`；仓库不提交每次实验的大文件。
