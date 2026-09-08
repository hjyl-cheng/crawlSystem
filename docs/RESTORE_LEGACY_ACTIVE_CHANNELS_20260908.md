# 恢复老库正常频道到新系统待迁移列表

用户授权范围：老库 `crawler.channels.status='active'` 的 23,507 个频道，包括 23,433 个 agent done 及 74 个 failed/pending/skipped。休眠和已移除频道不在本次范围。

目标是新库 `crawler.migration_channel_inventory`，供 `/migration-channels` 页面和现有迁移入口使用；不导入旧视频，不自动启动迁移。老库继续只读。

## 入口兼容

原入口只接受 legacy_results_db 来源的 pending 候选。新库增加 `crawler.restored_migration_sources`，保存经授权的频道入口快照及原候选来源、状态和恢复批次；运行开关为 `MIGRATION_RESTORED_SOURCES_ENABLED=true`。

单频道读取验证来源数据库身份及快照哈希；批量读取沿用候选 ID / 频道 ID 排除集合，优先返回恢复列表，再填充原有候选。库存同步保留恢复入口，避免后续同步将其删除。其余原始迁移意图、候选准入、执行栅栏和采集逻辑保持现有流程。

旧搜索发现来源也可使用这个显式恢复入口，无需修改老库状态或伪造旧结果库来源。新表属于迁移库存附属表；迁移 schema 脚本包含建表语句。

## 写入保护与预演

`scripts/restoreLegacyActiveMigrationSources.mjs` 默认事务回滚，`--apply` 才提交。

- 验证只读源库与目标写库身份。
- 精确断言范围为 23,507，done 为 23,433。
- 检查目标 Channel / Candidate / Migration Intent 无重叠。
- 与库存同步共用事务锁；记录已有库存的旧值并对去重后数量断言。
- 不允许重复执行已存在的恢复批次。

生产回滚预演通过：原库存 377,824，其中已有 6 个目标频道；新增 23,501，最终库存 401,325，目标集合 23,507 个均保留。所有预演事务均回滚；正式提交结果需以操作日志及后续数据库验证为准。

## 验证

正式 Node 20 镜像下执行来源读取、恢复入口、库存同步和 schema 单元测试。另以生产回滚事务验证真实 SQL 约束与去重行为。数据恢复后须只读验证页面筛选、单频道快照和批量快照；不提交测试采集任务。
