"""compaction 领域服务：小文件压缩合并。

业务逻辑：
1. 压缩任务：对目标表目标分区的多个小文件合并为大文件
2. 压缩链路：partition_scale → compaction（分区策略输出文件统计触发压缩）
3. 失败场景：源文件正在写入无法合并、合并后校验失败、压缩算法不支持
4. 与上下游流水线：消费 partition_scale 分区统计；输出合并结果

跨模块数据流：
  partition_scale → compaction（分区文件过多时触发压缩合并）
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class CompactionService(BaseModuleService):
    MODULE = "compaction"
    ENTITY = "压缩任务"
    OPERATION_TERM = "存储"
    LIST_FIELDS = ["任务编号", "目标表", "目标分区", "文件数量", "合并前大小", "合并后大小", "压缩算法", "执行时间"]
    REQUIRED_FIELDS = ["任务编号", "目标表", "目标分区"]
    STATUS_ORDER = ["待执行", "执行中", "已完成", "已取消"]
    ACTION_RULES = {"发起压缩": "执行中", "标记完成": "已完成", "取消任务": "已取消"}
    NEGATIVE_ACTIONS = []

    PIPELINE = "compaction_pipeline"
    UPSTREAM_MODULES = ["partition_scale"]
    PRODUCES_ARTIFACT = "压缩合并任务记录"

    def execute_compaction(self, entry_id: int) -> dict[str, Any]:
        """执行压缩合并：将目标表分区的多个小文件合并。

        失败场景：
        - 源文件正在写入：文件被锁无法合并
        - 压缩算法不支持
        - 合并后文件超出预期大小
        - 合并过程中写入中断
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "reason": f"任务 {entry_id} 不存在"}
        if entry.get("status") != "待执行":
            return {"ok": False, "reason": f"任务状态 {entry.get('status')} 非待执行"}

        target_table = str(entry.get("目标表", ""))
        target_partition = str(entry.get("目标分区", ""))
        file_count = int(str(entry.get("文件数量", "0")))
        compression = str(entry.get("压缩算法", "snappy"))

        if file_count == 0:
            return {"ok": False, "reason": "文件数量为0，无数据可合并"}

        supported = ("snappy", "gzip", "lz4", "zstd")
        if compression not in supported:
            return {"ok": False, "reason": f"压缩算法 {compression} 不在支持列表: {supported}"}

        # 消费上游 partition_scale 的分区统计
        part_stats = store.pipeline("partition_governance")
        needs_compaction = True
        if part_stats and part_stats._artifacts:
            table_files = sum(
                a.get("files", 0) for a in part_stats._artifacts
                if a.get("table") == target_table
            )
            needs_compaction = table_files > 10

        if not needs_compaction and file_count < 10:
            return {"ok": False, "reason": f"表 {target_table} 文件数 {file_count} 无需合并（阈值10）"}

        # 模拟合并
        before_size = file_count * 10 * 1024 * 1024  # 10MB/文件
        after_size = before_size * 0.7
        merged_files = max(1, file_count // 3)  # 按3:1比例合并

        entry["文件数量"] = file_count
        entry["合并前大小"] = f"{before_size // 1024 // 1024}MB"
        entry["合并后大小"] = f"{int(after_size) // 1024 // 1024}MB"
        entry["执行时间"] = time.strftime("%Y-%m-%d %H:%M:%S")
        entry["status"] = "执行中"
        entry["pending"] = True

        batch_id = f"COMPACT_{entry_id}_{int(time.time())}"
        result = [{
            "table": target_table,
            "partition": target_partition,
            "files_before": file_count,
            "files_after": merged_files,
            "size_before": before_size,
            "size_after": int(after_size),
            "compression": compression,
        }]
        self.produce_artifact(result, batch_id)

        return {
            "ok": True,
            "entry_id": entry_id,
            "table": target_table,
            "partition": target_partition,
            "files_before": file_count,
            "files_after": merged_files,
            "compression": compression,
            "space_saved": f"{int(before_size - after_size) // 1024 // 1024}MB",
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        base["pending_tasks"] = sum(1 for r in store.rows(self.MODULE) if r.get("status") == "待执行")
        return base