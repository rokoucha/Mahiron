import { useEffect } from 'react'
import { api, type Job, type JobResultItem } from '../api'
import type { DashboardState } from '../dashboard'
import { useAsync } from '../hooks'
import { jobStatusLabel } from '../domain/job'
import { formatDate } from '../format/date'
import { PageFrame } from '../ui/layout'
import { ErrorList } from '../ui/logs'

export default function Jobs({ dashboard }: { dashboard: DashboardState }) {
  const { jobs } = dashboard
  const schedules = useAsync(api.schedules)

  useEffect(() => {
    if (dashboard.lastEvent?.resource !== 'job_schedule') return
    api
      .schedules()
      .then(schedules.setData)
      .catch(() => undefined)
  }, [
    dashboard.lastEvent?.resource,
    dashboard.lastEvent?.time,
    schedules.setData,
  ])

  async function runAction(label: string, action: () => Promise<void>) {
    if (!window.confirm(`${label}?`)) return
    await action()
    await jobs.reload()
    schedules.setData(await api.schedules())
  }

  return (
    <PageFrame
      title="ジョブ"
      subtitle="現在のジョブ、スケジュール、手動実行を確認できます。"
    >
      <ErrorList errors={[jobs.error, schedules.error]} />
      <section className="table-section">
        <h2>ジョブ</h2>
        <table>
          <thead>
            <tr>
              <th>名前</th>
              <th>状態</th>
              <th>結果</th>
              <th>更新日時</th>
              <th>時間</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>
            {(jobs.data ?? []).map((job) => (
              <tr key={job.id}>
                <td>
                  <strong>{job.name}</strong>
                  <span>{job.key}</span>
                  {job.error && <em>{job.error}</em>}
                </td>
                <td>
                  <span
                    className={`badge ${job.hasFailed ? 'bad' : job.status}`}
                  >
                    {job.hasFailed ? '失敗' : jobStatusLabel(job.status)}
                  </span>
                </td>
                <td>
                  <JobResultView job={job} />
                </td>
                <td>{formatDate(job.updatedAt)}</td>
                <td>
                  {job.duration ? `${Math.round(job.duration / 1000)}秒` : '-'}
                </td>
                <td className="actions">
                  <button
                    onClick={() =>
                      runAction(`${job.name} を再実行`, () =>
                        api.rerunJob(job.id),
                      )
                    }
                    type="button"
                  >
                    再実行
                  </button>
                  {job.status !== 'finished' && (
                    <button
                      onClick={() =>
                        runAction(`${job.name} を中止`, () =>
                          api.abortJob(job.id),
                        )
                      }
                      type="button"
                    >
                      中止
                    </button>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>
      <section className="table-section">
        <h2>スケジュール</h2>
        <table>
          <thead>
            <tr>
              <th>キー</th>
              <th>スケジュール</th>
              <th>ジョブ</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>
            {(schedules.data ?? []).map((schedule) => (
              <tr key={schedule.key}>
                <td>{schedule.key}</td>
                <td>{schedule.schedule}</td>
                <td>{schedule.job.name}</td>
                <td className="actions">
                  <button
                    onClick={() =>
                      runAction(`${schedule.key} を実行`, () =>
                        api.runSchedule(schedule.key),
                      )
                    }
                    type="button"
                  >
                    実行
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>
    </PageFrame>
  )
}

function JobResultView({ job }: { job: Job }) {
  const result = job.result
  if (!result) return <span>-</span>
  return (
    <div className="job-result">
      <strong>{result.summary || result.kind}</strong>
      {result.counts && <span>{formatCounts(result.counts)}</span>}
      {result.warnings?.map((warning, index) => (
        <em key={index}>{warning}</em>
      ))}
      {(result.items?.length ?? 0) > 0 && (
        <details>
          <summary>詳細 {result.items?.length}件</summary>
          <div className="job-result-items">
            {result.items?.map((item, index) => (
              <JobResultItemView item={item} key={index} />
            ))}
          </div>
        </details>
      )}
    </div>
  )
}

function JobResultItemView({ item }: { item: JobResultItem }) {
  if (item.kind === 'service') {
    const data = item.data ?? {}
    const name = stringValue(data.name) || item.summary || 'サービス'
    return (
      <div className="job-result-item">
        <strong>{name}</strong>
        <span>
          {changeLabel(stringValue(data.change))}
          {numberValue(data.networkId) !== null &&
            ` / NID ${numberValue(data.networkId)}`}
          {numberValue(data.serviceId) !== null &&
            ` / SID ${numberValue(data.serviceId)}`}
          {numberValue(data.transportStreamId) !== null &&
            ` / TSID ${numberValue(data.transportStreamId)}`}
          {numberValue(data.remoteControlKeyId)
            ? ` / リモコン ${numberValue(data.remoteControlKeyId)}`
            : ''}
        </span>
      </div>
    )
  }
  return (
    <div className="job-result-item">
      <strong>{item.summary || item.kind}</strong>
      {item.data && <code>{JSON.stringify(item.data)}</code>}
    </div>
  )
}

function formatCounts(counts: Record<string, number>) {
  return orderedCountEntries(counts)
    .map(([key, value]) => `${countLabel(key)} ${value}`)
    .join(' / ')
}

const countOrder = [
  'services',
  'addedServices',
  'existingServices',
  'removedServices',
  'newNetworks',
  'networks',
  'groups',
  'queued',
  'candidates',
  'observedServices',
  'remainingServices',
  'programs',
  'targets',
  'logos',
  'remaining',
  'timedOut',
]

function orderedCountEntries(counts: Record<string, number>) {
  const order = new Map(countOrder.map((key, index) => [key, index]))
  return Object.entries(counts).sort(([left], [right]) => {
    const leftOrder = order.get(left)
    const rightOrder = order.get(right)
    if (leftOrder !== undefined && rightOrder !== undefined)
      return leftOrder - rightOrder
    if (leftOrder !== undefined) return -1
    if (rightOrder !== undefined) return 1
    return left.localeCompare(right)
  })
}

function countLabel(key: string) {
  const labels: Record<string, string> = {
    services: 'サービス',
    addedServices: '追加',
    existingServices: '既存',
    removedServices: '削除',
    newNetworks: '新規NID',
    networks: 'ネットワーク',
    groups: '取得グループ',
    queued: 'キュー',
    candidates: '候補',
    observedServices: '取得済み',
    remainingServices: '未取得',
    programs: '番組',
    targets: '対象',
    logos: 'ロゴ',
    remaining: '残り',
    timedOut: 'タイムアウト',
  }
  return labels[key] ?? key
}

function changeLabel(change: string | null) {
  const labels: Record<string, string> = {
    added: '追加',
    unchanged: '既存',
    removed: '削除',
  }
  return change ? (labels[change] ?? change) : '項目'
}

function stringValue(value: unknown) {
  return typeof value === 'string' ? value : null
}

function numberValue(value: unknown) {
  return typeof value === 'number' ? value : null
}
