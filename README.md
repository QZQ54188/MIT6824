# MIT 6.824：分布式系统课程实验（2024 春季版）

这是我在学习 [MIT 6.824 Distributed Systems](https://pdos.csail.mit.edu/6.824/)（2024 春季）课程期间完成的实验与笔记整理。该课程由麻省理工学院开设，聚焦分布式系统的核心设计原理与实现方法。

## 📘 课程信息

- 📅 学期：2024 年春季
- 🏫 学校：麻省理工学院（MIT）
- 👨‍🏫 授课教师：Robert Morris、Frans Kaashoek 等
- 🌐 官方网站：[https://pdos.csail.mit.edu/6.824/](https://pdos.csail.mit.edu/6.824/)
- 📺 视频公开课：[YouTube 播放列表](https://www.youtube.com/playlist?list=PLkcQbKbegkMqiWfKzF9plFs4EFJ3_XvGA)

## 🧪 实验列表与完成情况

| 实验 | 主题 | 状态 | 说明 |
|------|------|------|------|
| Lab 1 | MapReduce | ✅ 已完成 | 实现一个基本的 MapReduce 框架 |
| Lab 2A | Raft：Leader 选举 | ✅ 已完成 | 实现 Raft 算法中的 Leader 选举部分 |
| Lab 2B | Raft：日志复制 | ✅ 已完成 | 实现 Raft 的日志复制逻辑 |
| Lab 2C | Raft：持久化 & 快照 | ✅ 已完成 | 实现崩溃恢复和快照优化 |
| Lab 3A | Sharded KV（不带迁移） | ✅ 已完成 | 多组 Raft 实现的分布式 KV 存储系统 |
| Lab 3B | Sharded KV（带迁移） | ✅ 已完成 | 实现动态分片迁移与配置更替 |

## 🛠️ 开发与测试环境

- 编程语言：Go 语言（Golang）
- 推荐版本：Go 1.18 及以上
- 系统环境：macOS / Linux / WSL2
- 构建与测试命令示例：
  ```bash
  go build
  go test -run 2A
  go test -run 3B -race

