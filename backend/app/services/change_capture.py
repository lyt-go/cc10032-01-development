"""change_capture 领域服务：变更数据捕获（CDC）管理。

业务逻辑：
1. CDC配置：源数据库Binlog位点→过滤规则→目标Topic输出
2. 变更捕获链路：change_capture → schema_govern（变更Schema演进）
3. 失败场景：Binlog位点丢失、过滤规则误配、写入目标Topic失败、延迟追赶
4. 与上下游流水线：为 schema_govern 提供变更元信息

跨模块数据流：
  change_capture → schema_govern（捕获到的变更驱动Schema演进）
  change_capture → changelog（变更记录写入全局变更日志）
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class ChangeCaptureService(BaseModuleService):
    MODULE = "change_capture"
    ENTITY = "变更捕获配置"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["配置编号", "源数据库", "目标表", "捕获模式", "Binlog位点", "过滤规则", "变更类型", "目标Topic"]
    REQUIRED_FIELDS = ["配置编号", "源数据库", "目标表"]
    STATUS_ORDER = ["草稿", "待验证", "运行中", "已暂停"]
    ACTION_RULES = {"提交验证": "待验证", "启动捕获": "运行中", "暂停捕获": "已暂停"}
    NEGATIVE_ACTIONS = []

    # 跨模块流水线
    PIPELINE = "cdc_capture_pipeline"
    DOWNSTREAM_MODULES = ["schema_govern", "changelog"]
    PRODUCES_ARTIFACT = "CDC变更事件流"

    # 已捕获位置跟踪
    _capture_progress: dict[int, dict[str, Any]] = {}

    def capture_changes(self, entry_id: int) -> dict[str, Any]:
        """执行CDC变更捕获：模拟从Binlog读取变更事件。

        失败场景：
        - 源连接断开 → Binlog位点丢失
        - 过滤规则错误 → 捕获到不应捕获的表
        - 写入目标Topic失败 → 中断并重试
        - 延迟追赶 → 位点落后
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "reason": f"配置 {entry_id} 不存在"}
        if entry.get("status") != "运行中":
            return {"ok": False, "reason": f"捕获未运行，当前状态: {entry.get('status')}"}

        # 获取上次捕获位点
        progress = self._capture_progress.get(entry_id, {"binlog_pos": 0, "captured_count": 0})
        current_pos = progress.get("binlog_pos", 0)
        target_table = str(entry.get("目标表", ""))

        # 模拟捕获10个变更事件
        changes = []
        dirty_events = []
        new_pos = current_pos + 1000

        for i in range(10):
            change_type = ["INSERT", "UPDATE", "DELETE"][i % 3]
            event = {
                "binlog_pos": new_pos + i * 100,
                "table": target_table,
                "type": change_type,
                "ts": time.time() - i * 2,
                "source_db": entry.get("源数据库"),
            }
            # 模拟模拟过滤规则误配
            if i == 5 and "exclude" in str(entry.get("过滤规则", "")).lower():
                dirty_events.append({"event": event, "issue": "过滤规则误配: 排除了应捕获的变更"})
                continue
            changes.append(event)

        # 模拟写入目标Topic失败
        topic = str(entry.get("目标Topic", ""))
        if not topic:
            return {"ok": False, "reason": "目标Topic为空，写入失败"}

        # 更新进度
        self._capture_progress[entry_id] = {"binlog_pos": new_pos + 1000, "captured_count": progress["captured_count"] + len(changes), "last_ts": time.time()}

        # 写入全局变更日志
        for ch in changes:
            store.append_changelog({
                "目标表": target_table,
                "变更类型": ch["type"],
                "变更内容": str(ch),
                "操作时间": ch["ts"],
                "操作来源": "CDC_capture",
                "影响行数": 1,
                "回滚版本": "",
            })

        # 注册Schema变更（如果捕获到新字段）
        store.register_schema(target_table, {"fields": ["id", "name", "ts", "status"], "version": "v1"}, f"v1_{new_pos}")

        batch_id = f"CDC_{entry_id}_{int(time.time())}"
        self.produce_artifact(changes, batch_id)

        return {
            "ok": True,
            "entry_id": entry_id,
            "captured": len(changes),
            "dirty": len(dirty_events),
            "new_binlog_pos": new_pos + 1000,
            "target_table": target_table,
            "schema_updated": True,
        }

    def get_capture_lag(self, entry_id: int) -> dict[str, Any]:
        """获取捕获延迟信息"""
        progress = self._capture_progress.get(entry_id)
        if progress is None:
            return {"ok": False, "reason": "无捕获记录"}
        lag = time.time() - progress.get("last_ts", 0)
        return {
            "ok": True,
            "entry_id": entry_id,
            "lag_seconds": round(lag, 1),
            "captured_count": progress.get("captured_count", 0),
            "last_binlog_pos": progress.get("binlog_pos", 0),
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        base["capture_progress_tracking"] = len(self._capture_progress)
        base["global_changelog_entries"] = len(store._global_changelog)
        return base