"""schema_govern 领域服务：Schema治理与版本演进管理。

业务逻辑：
1. Schema注册、版本号递增、兼容性校验
2. Schema演进链路：接收 change_capture 变更 → 校验兼容性 → 写入Schema注册表
3. 失败场景：不兼容的字段删除、类型变更冲突、重复版本号
4. 与上下游流水线：消费 change_capture 变更；为 table_format 提供Schema元信息

跨模块数据流：
  change_capture → schema_govern（CDC变更驱动Schema演进）
  schema_govern → table_format（Schema版本决定表格式写入策略）
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class SchemaGovernService(BaseModuleService):
    MODULE = "schema_govern"
    ENTITY = "Schema版本"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["主题名称", "Schema定义", "版本号", "兼容策略", "注册时间", "序列化格式", "字段列表", "演进历史"]
    REQUIRED_FIELDS = ["主题名称", "Schema定义", "版本号"]
    STATUS_ORDER = ["草稿", "待校验", "已注册", "已废弃"]
    ACTION_RULES = {"提交校验": "待校验", "确认注册": "已注册", "标记废弃": "已废弃"}
    NEGATIVE_ACTIONS = []

    PIPELINE = "schema_evolution"
    UPSTREAM_MODULES = ["change_capture"]
    DOWNSTREAM_MODULES = ["table_format"]
    PRODUCES_ARTIFACT = "Schema演进版本链"

    def validate_compatibility(self, entry_id: int, new_field_spec: dict[str, Any]) -> dict[str, Any]:
        """校验新Schema版本与已注册版本的兼容性。

        失败场景：
        - 字段类型变更 → 不向后兼容
        - 删除已有字段 → 破坏性变更
        - 重复版本号 → 拒绝注册
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "reason": f"Schema {entry_id} 不存在"}

        topic = str(entry.get("主题名称", ""))
        versions = store.schema_versions(topic)
        version_number = new_field_spec.get("version", "v1")

        # 检查重复版本号
        for v in versions:
            if v.get("version") == version_number:
                return {"ok": False, "reason": f"版本号 {version_number} 已存在，请使用新版本号"}

        fields = new_field_spec.get("fields", [])
        result = store.check_schema_compatibility(topic, fields)

        if not result["compatible"]:
            entry["abnormal"] = True
            entry["status"] = "待校验"
            return {"ok": False, "reason": f"兼容性校验失败: {result['reason']}", "old_fields": result}

        return {"ok": True, "topic": topic, "version": version_number, "fields": fields, "compatibility": "BACKWARD"}

    def evolve_schema(self, entry_id: int, change_events: list[dict[str, Any]]) -> dict[str, Any]:
        """基于CDC变更事件驱动Schema演进。

        消费 change_capture 产生的变更事件，分析字段变更然后更新Schema注册表。
        这是 change_capture → schema_govern 贯通数据流的核心。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "reason": f"Schema {entry_id} 不存在"}
        if entry.get("status") != "已注册":
            return {"ok": False, "reason": f"Schema未注册，当前状态: {entry.get('status')}"}

        topic = str(entry.get("主题名称", ""))
        current_version = str(entry.get("版本号", "v1"))
        next_version = f"v{int(current_version.replace('v', '') or 1) + 1}"

        # 分析变更事件中的字段变化
        new_fields = {"id", "name", "ts", "status"}
        for event in change_events:
            if isinstance(event, dict) and event.get("type") == "INSERT":
                # 模拟从CDC事件中提取新字段
                new_fields.add("new_field_from_cdc")

        schema_def = {"fields": sorted(new_fields), "version": next_version}
        compat = self.validate_compatibility(entry_id, schema_def)
        if not compat["ok"]:
            return compat

        store.register_schema(topic, schema_def, next_version)

        # 创建新版本
        new_version = dict(entry)
        new_version["id"] = max((int(r.get("id", 0)) for r in store.rows(self.MODULE)), default=0) + 1
        new_version["版本号"] = next_version
        new_version["Schema定义"] = str(schema_def)
        new_version["字段列表"] = str(sorted(new_fields))
        new_version["演进历史"] = f"{current_version}→{next_version}: 新增字段"
        new_version["status"] = "草稿"
        new_version["pending"] = True
        new_version["abnormal"] = False
        store.rows(self.MODULE).append(new_version)

        batch_id = f"SCHEMA_EVOLVE_{entry_id}_{int(time.time())}"
        self.produce_artifact([schema_def], batch_id)

        return {
            "ok": True,
            "topic": topic,
            "old_version": current_version,
            "new_version": next_version,
            "new_fields": list(new_fields),
            "source": "change_capture_events",
        }

    def emit_schema_for_table_format(self, topic: str) -> dict[str, Any]:
        """向 table_format 暴露当前最新Schema版本，用于决定写入策略"""
        versions = store.schema_versions(topic)
        if not versions:
            return {"ok": False, "reason": f"主题 {topic} 无已注册Schema"}
        latest = versions[-1]
        return {
            "ok": True,
            "topic": topic,
            "latest_version": latest.get("version"),
            "definition": latest.get("definition"),
            "fields": latest.get("definition", {}).get("fields", []),
            "downstream_consumer": "table_format",
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        base["registry_topics"] = len(store._schema_registry)
        return base