# 增量事件契约修复离线验收

日期：2026-09-23

此为初始离线记录；后续已完成生产恢复与部署，最终条件及验收见 [磁盘恢复记录](disk-recovery-20260923.md)。

本次修复统一了生产者和 Feature Engine 对 Discovery 完成条件的判断：对于不带完整处置记录的旧基线，存在 `detail_failure_count > 0` 时，即使扫描终止原因为 `list_end`、`age_boundary_crossed` 或其他正常终止条件，也只能产生 `partial`，不能产生 `complete`。

离线使用 2026-09-22 保存的 334 个历史缺失前序事件进行回归：

```text
{"events": 334, "valid": 334, "errors": {}, "examples": []}
```

本次工作区验证：

- `services/qybullmq`: `node --test test/baselineBundle.test.js` 通过。
- `services/feature-engine`: `49` 个 `test_v16_domain_events` 测试通过。
- Feature Engine 全量：`215 passed, 17 skipped`。
- QYBullMQ 全量：`404 passed, 7 failed`；失败集中在构建、数据库隔离和网关/ytdlp 集成环境，增量相关测试通过。

上述验证时尚未部署。后续已经通过容器内连接完成只读核查、修复部署和分批幂等重放。最终版本另外保留了增量完整处置记录对终止排除项的完成判断，避免把已经解决的失败详情误判为 partial。
