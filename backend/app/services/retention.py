"""retention 领域服务：数据保留生命周期管理。

业务逻辑：
1. 保留策略：目标表保留天数、最小快照数、自动清理
2. 保留链路：cold_archive → retention（归档后的数据按保留策略管理生命周期）
3. 失败场景：例外规则冲突、清理策略误删、最小快照数不满足
4. 与上下游流水线：消费 cold_archive 归档清单；输出保留结果

跨模块数据流：
  cold_archive → retention（归档清单决定保留策略的清理范围）
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class RetentionService(BaseModuleService):
    MODULE = "retention"
    ENTITY = "保留策略"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["策略编号", "目标表", "保留天数", "最小快照数", "自动清理", "例外规则", "清理方式", "生效时间"]
    REQUIRED_FIELDS = ["策略编号", "目标表", "保留天数"]
    STATUS_ORDER = ["草稿", "待验证", "已生效", "已停用"]
    ACTION_RULES = {"提交验证": "待验证", "标记生效": "已生效", "停用策略": "已停用"}
    NEGATIVE_ACTIONS = ["停用策略"]

    PIPELINE = "retention_pipeline"
    UPSTREAM_MODULES = ["cold_archive"]
    PRODUCES_ARTIFACT = "数据保留清理记录"

    def execute_retention(self, entry_id: int) -> dict[str, Any]:
        """执行数据保留清理。

        失败场景：
        - 例外规则冲突：在例外规则内的数据被误清理
        - 最小快照数不满足：清理后快照数低于最小要求
        - 自动清理误触发：清理了仍在生命周期内的数据
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "reason": f"策略 {entry_id} 不存在"}
        if entry.get("status") != "已生效":
            return {"ok": False, "reason": f"策略未生效，当前状态: {entry.get('status')}"}

        table_name = str(entry.get("目标表", ""))
        retention_days = int(str(entry.get("保留天数", "30")))
        min_snapshots = int(str(entry.get("最小快照数", "3")))
        auto_clean = str(entry.get("自动清理", "是"))
        exceptions = str(entry.get("例外规则", ""))
        clean_method = str(entry.get("清理方式", "软删除"))

        if retention_days < 1:
            return {"ok": False, "reason": f"保留天数 {retention_days} 不合法"}

        # 消费上游 cold_archive 清单
        arch_records = store.archive_manifest_for_table(table_name)
        total_archived = sum(r.get("archived_rows", 0) for r in arch_records)

        # 模拟清理逻辑
        cleaned = 0
        retained = 0
        excluded = 0
        issues = []

        # 模拟存储的数据行
        store_rows = list(store.rows(table_name))
        cutoff_ts = time.time() - retention_days * 86400

        for row in store_rows:
            row_age = time.time() - float(str(row.get("_ts", time.time())))
            if row_age > retention_days * 86400:
                if exceptions and any(exc in str(row) for exc in exceptions.split(",")):
                    excluded += 1
                    continue
                cleaned += 1
            else:
                retained += 1

        # 最小快照检查
        if len(store_rows) - cleaned < min_snapshots:
            issues.append(f"清理后将低于最小快照数 {min_snapshots}（当前 {len(store_rows)}，清理 {cleaned}）")
            cleaned = max(0, len(store_rows) - min_snapshots)

        entry["生效时间"] = time.strftime("%Y-%m-%d %H:%M:%S")

        batch_id = f"RETENTION_{entry_id}_{int(time.time())}"
        result = [{
            "table": table_name,
            "retention_days": retention_days,
            "cleaned": cleaned,
            "retained": retained,
            "excluded": excluded,
            "total_archived": total_archived,
            "issues": issues,
        }]
        self.produce_artifact(result, batch_id)

        if total_archived > 0:
            # 有归档数据存在，说明 cold_archive → retention 链路贯通
            return {
                "ok": True,
                "entry_id": entry_id,
                "table": table_name,
                "cleaned": cleaned,
                "retained": retained,
                "excluded": excluded,
                "total_archived": total_archived,
                "cross_module_link": "cold_archive→retention 贯通",
                "issues": issues,
            }

        return {
            "ok": True,
            "entry_id": entry_id,
            "table": table_name,
            "cleaned": cleaned,
            "retained": retained,
            "excluded": excluded,
            "total_archived": total_archived,
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        base["active_policies"] = sum(1 for r in store.rows(self.MODULE) if r.get("status") == "已生效")
        base["archive_manifest_total"] = len(store._archive_manifest)
        return base