"""snapshot_mgr 领域服务：数据快照版本管理。

业务逻辑：
1. 快照创建：对源表生成版本化快照（存储路径+压缩格式+校验摘要）
2. 快照链路：snapshot_mgr → backfill_job（快照用于历史回填基准）
3. 失败场景：校验摘要不匹配、存储路径不可达、压缩格式错误
4. 与上下游流水线：为 backfill_job 提供回填基准快照

跨模块数据流：
  snapshot_mgr → backfill_job（快照作为回填数据基准）
"""
from __future__ import annotations

import hashlib
import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class SnapshotMgrService(BaseModuleService):
    MODULE = "snapshot_mgr"
    ENTITY = "数据快照"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["快照编号", "源表名称", "快照时间", "快照版本", "行数统计", "存储路径", "压缩格式", "校验摘要"]
    REQUIRED_FIELDS = ["快照编号", "源表名称", "快照时间"]
    STATUS_ORDER = ["创建中", "已完成", "已恢复", "已过期"]
    ACTION_RULES = {"创建快照": "已完成", "标记完成": "已恢复", "恢复快照": "已过期"}
    NEGATIVE_ACTIONS = []

    PIPELINE = "snapshot_management"
    DOWNSTREAM_MODULES = ["backfill_job"]
    PRODUCES_ARTIFACT = "数据版本快照"

    def create_snapshot(self, entry_id: int, source_rows: list[dict[str, Any]]) -> dict[str, Any]:
        """创建数据快照：对源表数据做校验摘要和压缩。

        失败场景：
        - 存储路径不可达
        - 压缩格式不支持
        - 校验摘要生成失败
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "reason": f"快照 {entry_id} 不存在"}

        table_name = str(entry.get("源表名称", ""))
        snap_version = str(entry.get("快照版本", f"v{int(time.time())}"))
        compress_fmt = str(entry.get("压缩格式", "parquet"))

        if not table_name:
            return {"ok": False, "reason": "源表名称为空，无法创建快照"}

        if compress_fmt not in ("parquet", "orc", "avro"):
            return {"ok": False, "reason": f"不支持的压缩格式: {compress_fmt}"}

        # 计算校验摘要
        raw = "".join(str(r) for r in source_rows[:100])
        checksum = hashlib.md5(raw.encode()).hexdigest()[:16]

        storage_path = f"s3://data-lake/snapshots/{table_name}/{snap_version}/"
        row_count = len(source_rows)

        # 更新条目
        entry["快照版本"] = snap_version
        entry["行数统计"] = str(row_count)
        entry["存储路径"] = storage_path
        entry["校验摘要"] = checksum
        entry["快照时间"] = time.strftime("%Y-%m-%d %H:%M:%S")
        entry["status"] = "已完成"
        entry["pending"] = False

        # 保存到快照存储
        store.save_snapshot(entry["快照编号"], {
            "table": table_name,
            "version": snap_version,
            "path": storage_path,
            "checksum": checksum,
            "row_count": row_count,
            "data_sample": source_rows[:3],
        })

        batch_id = f"SNAP_{entry_id}_{int(time.time())}"
        self.produce_artifact(source_rows[:50], batch_id)

        return {
            "ok": True,
            "snapshot_id": entry["快照编号"],
            "table": table_name,
            "version": snap_version,
            "rows": row_count,
            "checksum": checksum,
            "path": storage_path,
        }

    def emit_snapshot_for_backfill(self, table_name: str) -> dict[str, Any]:
        """向 backfill_job 暴露快照基准数据"""
        for snap_id, snap_data in store._snapshot_store.items():
            if snap_data.get("table") == table_name:
                return {"ok": True, "snapshot": snap_data, "downstream_consumer": "backfill_job"}
        return {"ok": False, "reason": f"表 {table_name} 无可用快照"}

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        base["stored_snapshots"] = len(store._snapshot_store)
        return base