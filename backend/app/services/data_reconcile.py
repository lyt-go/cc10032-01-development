"""data_reconcile 领域服务：数据对账与差异检测。

业务逻辑：
1. 数据对账：左源表与右源表按对账字段逐行对比
2. 对账链路：data_reconcile → changelog（差异结果写入变更日志追溯）
3. 失败场景：差异阈值超限、字段类型不匹配、对账超时
4. 与上下游流水线：为 changelog 提供对账差异记录

跨模块数据流：
  data_reconcile → changelog（对账差异写入变更日志，用于变更追溯）
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class DataReconcileService(BaseModuleService):
    MODULE = "data_reconcile"
    ENTITY = "对账任务"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["任务编号", "左源表", "右源表", "对账字段", "差异阈值", "执行时间", "差异行数", "对账结论"]
    REQUIRED_FIELDS = ["任务编号", "左源表", "右源表"]
    STATUS_ORDER = ["待执行", "执行中", "一致", "存在差异"]
    ACTION_RULES = {"发起对账": "执行中", "标记一致": "一致", "标记差异": "存在差异"}
    NEGATIVE_ACTIONS = []

    PIPELINE = "reconcile_pipeline"
    DOWNSTREAM_MODULES = ["changelog"]
    PRODUCES_ARTIFACT = "数据对账结果记录"

    def execute_reconcile(self, entry_id: int) -> dict[str, Any]:
        """执行对账：比较左右源表数据差异。

        失败场景：
        - 差异阈值超限
        - 对账字段类型不匹配
        - 源表数据为空
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "reason": f"任务 {entry_id} 不存在"}
        if entry.get("status") != "待执行":
            return {"ok": False, "reason": f"任务状态 {entry.get('status')} 非待执行"}

        left_table = str(entry.get("左源表", ""))
        right_table = str(entry.get("右源表", ""))
        reconcile_field = str(entry.get("对账字段", "id"))
        threshold = float(str(entry.get("差异阈值", "5")))

        # 从相关模块获取数据
        left_rows = store.rows(left_table) if left_table in store._tables else []
        right_rows = store.rows(right_table) if right_table in store._tables else []

        if not left_rows:
            return {"ok": False, "reason": f"左源表 {left_table} 无数据"}

        # 执行对账
        left_map = {r.get(reconcile_field): r for r in left_rows if r.get(reconcile_field) is not None}
        right_map = {r.get(reconcile_field): r for r in right_rows if r.get(reconcile_field) is not None}

        only_left = [k for k in left_map if k not in right_map]
        only_right = [k for k in right_map if k not in left_map]
        mismatches = 0
        for k in left_map:
            if k in right_map:
                # 简单对比
                left_val = left_map[k]
                right_val = right_map[k]
                common_fields = set(left_val.keys()) & set(right_val.keys()) - {"id", "pending", "abnormal", "status"}
                for f in common_fields:
                    if str(left_val.get(f)) != str(right_val.get(f)):
                        mismatches += 1

        diff_count = len(only_left) + len(only_right) + mismatches
        passed = diff_count <= threshold

        entry["差异行数"] = str(diff_count)
        entry["对账结论"] = "一致" if passed else "存在差异"
        entry["执行时间"] = time.strftime("%Y-%m-%d %H:%M:%S")
        entry["status"] = "一致" if passed else "存在差异"
        entry["pending"] = not passed
        entry["abnormal"] = not passed

        # 写入差异到 changelog
        if not passed:
            store.append_changelog({
                "目标表": left_table,
                "变更类型": "对账差异",
                "变更内容": f"only_left: {len(only_left)}, only_right: {len(only_right)}, mismatches: {mismatches}",
                "操作时间": time.time(),
                "操作来源": "reconcile",
                "影响行数": diff_count,
                "回滚版本": "",
            })

        batch_id = f"RECONCILE_{entry_id}_{int(time.time())}"
        result = [{
            "task": entry.get("任务编号"),
            "diff_count": diff_count,
            "passed": passed,
            "left_table": left_table,
            "right_table": right_table,
        }]
        self.produce_artifact(result, batch_id)

        return {
            "ok": True,
            "entry_id": entry_id,
            "left_table": left_table,
            "right_table": right_table,
            "diff_count": diff_count,
            "passed": passed,
            "only_left": len(only_left),
            "only_right": len(only_right),
            "mismatches": mismatches,
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        base["pending_tasks"] = sum(1 for r in store.rows(self.MODULE) if r.get("status") == "待执行")
        return base