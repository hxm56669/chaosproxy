# M7 面试证据索引

本索引只引用仓库内可重跑的命令和测试；没有 broker、容器或压测数据时，不把静态配置描述为运行结果。

| 能力 | 代码入口 | 可重跑证据 | 当前边界 |
|---|---|---|---|
| 非阻塞双向代理 | `src/proxy/` | `ctest --preset proxy-debug` | 仅记录当前环境实测，不声称生产吞吐 |
| 照片任务本地闭环 | `src/phototask/task_pipeline.cpp` | B2～B6 tests in `ctest --preset photo-debug` | TaskRepository 当前为内存端口 |
| Kafka 持久接入边界 | `src/phototask/kafka_*.cpp`, `src/phototask/task_admission.cpp` | F1～F6 tests in `ctest --preset kafka-debug` | 无 broker，K01～K08 未执行 |
| Redis 降级与失效 | `src/phototask/redis_cache.cpp`, `photo_service.cpp` | C1～C4 tests in `ctest --preset photo-debug` | 已用本机 Redis 实测 |
| 可观测性 | `src/phototask/runtime_status.cpp` | `photo_runtime --health` 和 `/livez` `/readyz` `/metrics` | 指标仍是最小 runtime 集 |
| 可重复交付 | `benchmarks/run_m7.py`, `tools/replay_environment.sh` | M7 runner 生成带 seed/git/UTC 的 JSON | 不从命令时间推导吞吐 |

当前阶段提交由 `git rev-parse HEAD` 读取；报告中的 `git_commit` 是运行时真实值。
