<template>
  <section class="page">
    <header class="page-head">
      <div>
        <h2>运营概览</h2>
        <p class="page-desc">汇总各业务模块的关键指标，先看总量再看异常。</p>
      </div>
    </header>
    <div class="stat-row">
      <article v-for="card in cards" :key="card.label" class="stat-card">
        <span class="stat-label">{{ card.label }}</span>
        <strong class="stat-value">{{ card.value }}</strong>
      </article>
    </div>
    <table class="data-table">
      <thead>
        <tr><th>业务模块</th><th>今日新增</th><th>待处理</th><th>异常量</th></tr>
      </thead>
      <tbody>
        <tr v-for="row in moduleRows" :key="row.name">
          <td>{{ row.name }}</td>
          <td>{{ row.created }}</td>
          <td>{{ row.pending }}</td>
          <td>{{ row.abnormal }}</td>
        </tr>
      </tbody>
    </table>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { fetchJson } from '@/api/client'

type Overview = {
  cards: { label: string; value: number }[]
  modules: { name: string; created: number; pending: number; abnormal: number }[]
}

const cards = ref<Overview['cards']>([])
const moduleRows = ref<Overview['modules']>([])

onMounted(async () => {
  try {
    const payload = await fetchJson<Overview>('/api/overview')
    cards.value = payload.cards
    moduleRows.value = payload.modules
  } catch {
    cards.value = [{"label": "业务模块", "value": 0}, {"label": "今日新增", "value": 0}]
    moduleRows.value = [{"name": "流式数据源", "created": 0, "pending": 0, "abnormal": 0}, {"name": "批次分片", "created": 0, "pending": 0, "abnormal": 0}, {"name": "变更捕获", "created": 0, "pending": 0, "abnormal": 0}, {"name": "Schema治理", "created": 0, "pending": 0, "abnormal": 0}, {"name": "水位管理", "created": 0, "pending": 0, "abnormal": 0}, {"name": "迟到数据", "created": 0, "pending": 0, "abnormal": 0}, {"name": "流式关联", "created": 0, "pending": 0, "abnormal": 0}, {"name": "合并策略", "created": 0, "pending": 0, "abnormal": 0}, {"name": "分区扩缩", "created": 0, "pending": 0, "abnormal": 0}, {"name": "快照管理", "created": 0, "pending": 0, "abnormal": 0}, {"name": "数据对账", "created": 0, "pending": 0, "abnormal": 0}, {"name": "回填作业", "created": 0, "pending": 0, "abnormal": 0}, {"name": "冷热分层", "created": 0, "pending": 0, "abnormal": 0}, {"name": "压缩合并", "created": 0, "pending": 0, "abnormal": 0}, {"name": "血缘图", "created": 0, "pending": 0, "abnormal": 0}, {"name": "变更日志", "created": 0, "pending": 0, "abnormal": 0}, {"name": "保留管理", "created": 0, "pending": 0, "abnormal": 0}, {"name": "表格式", "created": 0, "pending": 0, "abnormal": 0}]
  }
})
</script>
