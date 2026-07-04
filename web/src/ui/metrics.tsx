import type { Tuner } from '../api'

export function Metric({
  label,
  value,
  tone = 'neutral',
}: {
  label: string
  value: string
  tone?: 'neutral' | 'ok' | 'warn' | 'bad'
}) {
  return (
    <section className={`metric ${tone}`}>
      <span>{label}</span>
      <strong>{value}</strong>
    </section>
  )
}

export function Definition({ label, value }: { label: string; value: string }) {
  return (
    <div className="definition">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  )
}

export function StatusPill({ tuner }: { tuner: Tuner }) {
  const tone = tuner.isFault
    ? 'bad'
    : tuner.isUsing
      ? 'warn'
      : tuner.isFree
        ? 'ok'
        : 'neutral'
  const label = tuner.isFault
    ? '故障'
    : tuner.isUsing
      ? '使用中'
      : tuner.isFree
        ? '空き'
        : tuner.isAvailable
          ? 'ビジー'
          : '利用不可'
  return <span className={`badge ${tone}`}>{label}</span>
}

export function StreamState({ connected }: { connected: boolean }) {
  return (
    <span className={`stream-state ${connected ? 'connected' : ''}`}>
      {connected ? '接続中' : '接続待ち'}
    </span>
  )
}
