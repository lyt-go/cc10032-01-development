"""late_data 领域服务：迟到数据处理策略。

业务逻辑：
1. 迟到数据判定：基于watermark的当前水位和允许延迟阈值
2. 迟到处理链路：watermark → late_data → 侧输出/重定向主题
3. 失败场景：丢弃阈值超限、重定向主题不存在、侧输出路径不可写
4. 与上下游流水线：消费 watermark 水位；输出被判定为迟到的数据行

跨模块数据流：
  watermark → late_data（水位决定允许延迟窗口）
  late_data → 侧输出/重定向（迟到数据路由到专门主题）
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class LateDataService(BaseModuleService):
    MODULE = "late_data"
    ENTITY = "迟到数据策略"
    OPERATION_TERM = "关联"
    LIST_FIELDS = ["策略编号", "关联源", "允许延迟秒数", "处理方式", "侧输出路径", "重定向主题", "丢弃阈值", "最后更新"]
    REQUIRED_FIELDS = ["策略编号", "关联源", "允许延迟秒数"]
    STATUS_ORDER = ["草稿", "待验证", "已生效", "已停用"]
    ACTION_RULES = {"提交验证": "待验证", "标记生效": "已生效", "停用策略": "已停用"}
    NEGATIVE_ACTIONS = ["停用策略"]

    PIPELINE = "late_data_handler"
    UPSTREAM_MODULES = ["watermark"]
    DOWNSTREAM_MODULES = []
    PRODUCES_ARTIFACT = "迟到数据记录"

    def classify_late_events(self, entry_id: int, events: list[dict[str, Any]]) -> dict[str, Any]:
        """判定事件是否迟到，基于水位和允许延迟窗口。

        失败场景：
        - 超出丢弃阈值 → 直接移除
        - 侧输出路径不存在 → 写入失败
        - 重定向主题未配置 → 无法路由
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "reason": f"策略 {entry_id} 不存在"}
        if entry.get("status") != "已生效":
            return {"ok": False, "reason": f"策略未生效，当前状态: {entry.get('status')}"}

        allowed_lag = float(str(entry.get("允许延迟秒数", "30")))
        discard_threshold = float(str(entry.get("丢弃阈值", "3600")))
        source_id = str(entry.get("关联源", ""))

        # 消费上游watermark
        wm_info = store.get_watermark(source_id) or {}
        current_offset = wm_info.get("offset", time.time())

        on_time = []
        late = []
        discarded = []

        for event in events:
            event_ts = event.get("ts", time.time())
            lag = abs(current_offset - event_ts) if isinstance(current_offset, (int, float)) else abs(time.time() - event_ts)
            if lag <= allowed_lag:
                on_time.append(event)
            elif lag <= discard_threshold:
                late.append({"event": event, "lag": round(lag, 1), "redirect": entry.get("重定向主题", "")})
            else:
                discarded.append({"event": event, "lag": round(lag, 1), "reason": f"超出丢弃阈值 {discard_threshold}s"})

        # 写入到流水线
        batch_id = f"LATE_{entry_id}_{int(time.time())}"
        if late:
            self.produce_artifact(late, batch_id)

        entry["最后更新"] = time.strftime("%Y-%m-%d %H:%M:%S")

        return {
            "ok": True,
            "entry_id": entry_id,
            "source": source_id,
            "allowed_lag": allowed_lag,
            "discard_threshold": discard_threshold,
            "on_time": len(on_time),
            "late": len(late),
            "discarded": len(discarded),
            "late_samples": late[:3],
            "discarded_samples": discarded[:3],
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        base["active_policies"] = sum(1 for r in store.rows(self.MODULE) if r.get("status") == "已生效")
        return base