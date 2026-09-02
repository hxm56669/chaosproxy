# Plan

| 里程碑 | 目标 | 状态 |
| --- | --- | --- |
| M0 | 可配置、可编译、可运行、可测试的工程基线 | 已实现 |
| M1 | 最小透明代理数据路径 | 核心路径已被 M2/M3 覆盖，CLI 待接入 |
| M2 | 非阻塞多连接、Buffer 与背压 | 已实现 |
| M3 | 正确双向流、FIN/RST 与幂等回收 | 已实现 |
| Stage7 | TimerQueue、timerfd 与 generation-safe Timer | 已实现 |
| Stage8 | ToxicPipeline、Chunk、Snapshot 与安全 continuation | 下一阶段 |
| Stage9 | Latency 与 Jitter | 待开始 |
| Stage10 | Bandwidth 与 Slicer | 待开始 |
| Stage11 | Timeout、LimitData、Close 与 Reset | 待开始 |

下一步进入 Stage8，只建立可插拔 ToxicPipeline 与 PassThrough 闭环，不提前实现真实 Latency/Bandwidth。命令行监听/上游装配仍需单独补齐并验收。
