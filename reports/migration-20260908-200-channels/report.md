# 200 频道迁移审计（2026-09-08）

批次：`legacy-results-canary-1788838667002-6035a5ec`。统计时间：04:19–04:24 UTC。仅核查持久化数据和执行证据，未重新请求 YouTube；字段完整不等于逐条人工核实上游真实性。

- 03:37:49 开始，03:53:15 完成，约 15 分 26 秒（含 handle 修复、部署与补跑等待）。
- 198 个通过准入：123 个 ready_auto；75 个 ready_partial，均为近 90 天无符合条件的已发布内容而进入休眠。
- 2 个拒绝：Pontos Principais（账号终止）、Pedro Neto（版权终止）。当前失败、待执行与未完成恢复均为 0。
- 历史失败事件 4 条：handle 必填错误 1 条，路由预算耗尽 3 条（涉及 2 个频道）；均恢复。
- Renata Schneider - Topic 已完成；handle 为空，11500 订阅、4404000 总播放、9 个视频；进入休眠，不是迁移失败。

## 数据核查

198 个频道的名称、频道 ID、URL、订阅数、总播放量、总视频数已核查；123 个正常频道的头像及核心数值状态均完整。订阅数为页面显示量级的 estimated。

5747 个视频候选全部完成处理，最终来源均为 youtubei.js@17.2.0 / youtubejs_get_info；本批次共享视频 API-batch 请求记录为 0。

- 保留 2100 条；排除 3647 条：3614 条超出 90 天、31 条待直播、2 条正在直播；延后处理 0。
- 2100 条核心字段（标题、URL、封面、发布时间、时长、播放、点赞、评论数、类型、访问状态、发布哈希）缺失 0，重复 0。
- 类型：1175 Shorts、733 普通视频、192 直播回放。
- 点赞 2030 条 exact；70 条 zero_from_empty（按空值策略记 0，不能视为上游明确返回了 0）。
- 评论总数：1447 exact、589 zero_from_surface、64 disabled。评论首屏共 10970 行，结构计数不一致 0。
- 业务库和当前搜索索引均为 123 频道 / 2100 视频，与采集保留数量逐频道一致。123 条投影全部 delivered。

## 需要留意

1. Dra Priscilla Vicente - sem filtro（UC2ZYROC5sk5QsvUyubIiRHw）：外链状态 unresolved。原始目标是 `PRISCILLAVICENTE.DRA` 和 `21.984417357`，分别标记 INSTAGRAM / CONTATO；不是完整链接，未擅自拼接 URL 或邮箱。
2. Orff, Narrando Doramas（UCQr2Nif6hbKIVzEZsnCEvTw）有 3 条视频保留了评论总数但首屏行数为 0。持久化诊断记录显示 TOP_COMMENTS 为空后尝试 NEWEST_FIRST，仍为空，且没有 youtubejs_comments_error。仅凭当前证据不能断言是上游无可见评论或解析漏采；本次未做实时复抓。

| 视频 ID | 评论总数 | 已采首屏行数 |
|---|---:|---:|
| PNx19-7hlLA | 5 | 0 |
| teEzwbftywA | 7 | 0 |
| pIH3rBuARus | 44 | 0 |

频道描述为空、国家未公开和 handle 缺失属于可选字段，不按核心缺失统计。
