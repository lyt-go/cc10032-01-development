"""partition_scale 领域服务：分区扩缩与治理。

业务逻辑：
1. 分区策略管理：目标表分区粒度、预创建数量、保留天数
2. 分区治理链路：partition_scale → compaction（分区文件过多触发合并）
3. 失败场景：预创建分区冲突、清理策略误删、扩缩规则失效
4. 与上下游流水线：为 compaction 提供分区文件数量统计

跨模块数据流：
  partition_scale → compaction（分区策略决定压缩任务触发条件）
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class PartitionScaleService(BaseModuleService):
    MODULE = "partition_scale"
    ENTITY = "分区策略"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["策略编号", "目标表", "分区字段", "分区粒度", "预创建数量", "保留天数", "自动清理", "扩缩规则"]
    REQUIRED_FIELDS = ["策略编号", "目标表", "分区字段"]
    STATUS_ORDER = ["草稿", "待验证", "已生效", "已停用"]
    ACTION_RULES = {"提交验证": "待验证", "标记生效": "已生效", "停用策略": "已停用"}
    NEGATIVE_ACTIONS = ["停用策略"]

    PIPELINE = "partition_governance"
    DOWNSTREAM_MODULES = ["compaction"]
    PRODUCES_ARTIFACT = "分区统计记录"

    def scale_partitions(self, entry_id: int) -> dict[str, Any]:
        """执行分区扩缩：预创建分区或清理过期分区。

        失败场景：
        - 预创建分区冲突：分区已存在
        - 清理策略误删：保留天数小于已有数据
        - 粒度不合法：分区粒度不匹配
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "reason": f"策略 {entry_id} 不存在"}
        if entry.get("status") != "已生效":
            return {"ok": False, "reason": f"策略未生效，当前状态: {entry.get('status')}"}

        pre_create = int(str(entry.get("预创建数量", "10")))
        retention_days = int(str(entry.get("保留天数", "30")))
        target_table = str(entry.get("目标表", ""))
        granularity = str(entry.get("分区粒度", "day"))

        valid_granularities = ("hour", "day", "month")
        if granularity not in valid_granularities:
            return {"ok": False, "reason": f"分区粒度 {granularity} 不在允许范围: {valid_granularities}"}

        if pre_create > 100:
            return {"ok": False, "reason": f"预创建数量 {pre_create} 过大"}

        if retention_days < 1:
            return {"ok": False, "reason": f"保留天数 {retention_days} 不可少于1"}

        # 模拟分区创建和清理
        created = []
        cleaned = 0
        total_files = 0
        for i in range(pre_create):
            part_key = f"{target_table}_{granularity}_{i:04d}"
            created.append(part_key)
            total_files += 3  # 每个分区约3个小文件
        if retention_days < 7:
            cleaned = 2  # 模拟清理过期分区

        # 更新统计
        batch_id = f"PART_{entry_id}_{int(time.time())}"
        stats = [{"partition": p, "files": 3, "table": target_table} for p in created]
        self.produce_artifact(stats, batch_id)

        return {
            "ok": True,
            "entry_id": entry_id,
            "table": target_table,
            "partitions_created": len(created),
            "partitions_cleaned": cleaned,
            "total_files": total_files,
            "requires_compaction": total_files > 50,
        }

    def emit_partition_stats_for_compaction(self, table_name: str) -> dict[str, Any]:
        """向 compaction 暴露分区文件统计"""
        rows = store.rows(self.MODULE)
        relevant = [r for r in rows if r.get("目标表") == table_name and r.get("status") == "已生效"]
        if relevant:
            pre_create = int(str(relevant[0].get("预创建数量", 10)))
            return {
                "ok": True,
                "table": table_name,
                "estimated_files": pre_create * 3,
                "needs_compaction": pre_create * 3 > 50,
                "downstream_consumer": "compaction",
            }
        return {"ok": False, "reason": f"表 {table_name} 无分区策略"}

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        base["active_policies"] = sum(1 for r in store.rows(self.MODULE) if r.get("status") == "已生效")
        return base