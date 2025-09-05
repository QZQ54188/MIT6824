# MIT 6.5840 Distributed Systems (Spring 2024)

这是我在学习 **MIT 6.5840 Distributed Systems（2024 春季）** 课程期间整理的实验与笔记。课程由 MIT 提供，重点探讨分布式系统的核心设计与实现方法。


## 📘 课程信息

- 📅 学期：2024 年春季
- 🏫 学校：麻省理工学院（MIT）
- 👨‍🏫 授课教师：Robert Morris、Frans Kaashoek 等
- 🌐 官方网站：[https://pdos.csail.mit.edu/6.824/](https://pdos.csail.mit.edu/6.824/)
- 📺 视频公开课：[YouTube 播放列表](https://www.youtube.com/playlist?list=PLkcQbKbegkMqiWfKzF9plFs4EFJ3_XvGA)

## 实验列表与完成情况

| 实验 | 主题 | 状态 | 说明 |
|------|------|------|------|
| Lab 1 | MapReduce | ✅ 已完成 | 实现基本 MapReduce 框架 |
| Lab 2 | KV Server / Raft | ✅ 已完成 | 实现简单 KV 服务 + Raft 协议 |
| Lab 3 | Raft Consensus | ✅ 已完成 | 完整实现 Raft 共识（leader election, log replication 等） |
| Lab 4 | Fault-tolerant KV Service | ✅ 已完成 | 基于 Raft 构建 KV 存储系统，支持容错 |
| Lab 5 | Sharded KV Service |  未完成 | 分片 KV 服务（数据迁移与配置更替） |

## 🛠️ 开发与测试环境

- 编程语言：Go 语言（Golang）
- 推荐版本：Go 1.18 及以上
- 系统环境：macOS / Linux / WSL2
- 构建与测试命令示例：
  ```bash
  go build
  go test -run 2A
  go test -run 3B -race

