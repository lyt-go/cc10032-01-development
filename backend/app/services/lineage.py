"""lineage 领域服务：数据血缘关系追踪。

业务逻辑：
1. 血缘图构建：上游表→下游表→转换逻辑→字段映射
2. 血缘链路：lineage → table_format（血缘信息决定表格式写入策略）
3. 失败场景：环形依赖检测、断裂血缘修复、冲突映射覆盖
4. 与上下游流水线：为 table_format 提供字段映射关系

跨模块数据流：
  lineage → table_format（血缘追踪结果指导表格式治理策略）
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class LineageService(BaseModuleService):
    MODULE = "lineage"
    ENTITY = "血缘关系"
    OPERATION_TERM = "转换"
    LIST_FIELDS = ["关系编号", "上游表", "下游表", "转换逻辑", "字段映射", "依赖类型", "创建时间", "最后更新"]
    REQUIRED_FIELDS = ["关系编号", "上游表", "下游表"]
    STATUS_ORDER = ["草稿", "已确认", "已变更", "已废弃"]
    ACTION_RULES = {"确认关系": "已确认", "标记变更": "已变更", "标记废弃": "已废弃"}
    NEGATIVE_ACTIONS = []

    PIPELINE = "lineage_tracking"
    DOWNSTREAM_MODULES = ["table_format"]
    PRODUCES_ARTIFACT = "血缘图边关系"

    def build_lineage(self, entry_id: int) -> dict[str, Any]:
        """构建血缘关系：记录上游→下游转换逻辑。

        失败场景：
        - 环形依赖检测
        - 上游表不存在
        - 转换逻辑格式错误
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "reason": f"关系 {entry_id} 不存在"}

        upstream = str(entry.get("上游表", ""))
        downstream = str(entry.get("下游表", ""))
        transform_logic = str(entry.get("转换逻辑", "direct"))

        if not upstream or not downstream:
            return {"ok": False, "reason": "上游表或下游表为空"}

        # 环形依赖检测
        downstream_edges = store.lineage_downstream(downstream)
        for edge in downstream_edges:
            if edge.get("downstream") == upstream:
                return {"ok": False, "reason": f"检测到环形依赖: {upstream} → {downstream} → {upstream}"}

        # 记录血缘边
        store.add_lineage_edge(upstream, downstream, {
            "transform": transform_logic,
            "fields": entry.get("字段映射", ""),
            "type": entry.get("依赖类型", "etl"),
        })

        entry["创建时间"] = time.strftime("%Y-%m-%d %H:%M:%S")
        entry["最后更新"] = time.strftime("%Y-%m-%d %H:%M:%S")
        entry["status"] = "已确认"
        entry["pending"] = False

        batch_id = f"LINEAGE_{entry_id}_{int(time.time())}"
        self.produce_artifact([{
            "upstream": upstream,
            "downstream": downstream,
            "transform": transform_logic,
            "ts": time.time(),
        }], batch_id)

        return {
            "ok": True,
            "entry_id": entry_id,
            "upstream": upstream,
            "downstream": downstream,
            "transform": transform_logic,
        }

    def emit_lineage_for_table_format(self, table_name: str) -> dict[str, Any]:
        """向 table_format 暴露表的血缘上下游关系"""
        downstream = store.lineage_downstream(table_name)
        # 寻找上游
        upstream_tables = []
        for up, edges in store._lineage_graph.items():
            for e in edges:
                if e.get("downstream") == table_name:
                    upstream_tables.append(up)

        return {
            "ok": True,
            "table": table_name,
            "upstream": upstream_tables,
            "downstream": [d.get("downstream") for d in downstream],
            "edge_count": len(downstream) + len(upstream_tables),
            "downstream_consumer": "table_format",
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        base["lineage_edges_total"] = sum(len(v) for v in store._lineage_graph.values())
        return base