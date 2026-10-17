import type { Job } from '../api'

export function jobStatusLabel(status: Job['status']) {
  const labels: Record<Job['status'], string> = {
    queued: '待機中',
    standby: '準備中',
    running: '実行中',
    finished: '完了',
  }
  return labels[status] ?? status
}

export function currentGatheringChannels(jobs: Job[]) {
  return jobs
    .filter(
      (job) =>
        job.status === 'running' && job.key.startsWith('epg-gather:nid:'),
    )
    .map((job) => job.name.replace(/^EPG Gather /, ''))
    .filter(Boolean)
}

export function epgJobScope(key: string) {
  const match = /^epg-gather:nid:(\d+)(?::stream:(\d+))?(?::channel:.+)?$/.exec(
    key,
  )
  if (!match) return null
  return {
    onid: match[1],
    stream:
      match[2] === undefined
        ? null
        : key.includes(':channel:')
          ? '未取得'
          : match[2],
  }
}
