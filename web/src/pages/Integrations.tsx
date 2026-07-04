import { useMemo } from 'react'
import type { DashboardState } from '../dashboard'
import { isVisibleService, sortServicesForDisplay } from '../domain/service'
import { CopyRow } from '../ui/actions'
import { PageFrame } from '../ui/layout'

export default function Integrations({
  dashboard,
}: {
  dashboard: DashboardState
}) {
  const { services, channels } = dashboard
  const visibleServices = useMemo(
    () =>
      sortServicesForDisplay(
        (services.data ?? []).filter(isVisibleService),
        channels.data ?? [],
      ),
    [channels.data, services.data],
  )
  const origin = window.location.origin
  const links = [
    ['M3Uプレイリスト', `${origin}/api/iptv/playlist`],
    ['XMLTV', `${origin}/api/iptv/xmltv`],
    ['検出情報', `${origin}/api/iptv/discover.json`],
    ['ラインアップ', `${origin}/api/iptv/lineup.json`],
    ['ラインアップ状態', `${origin}/api/iptv/lineup_status.json`],
  ]
  return (
    <PageFrame
      title="連携"
      subtitle="IPTVクライアントや外部プレイヤー向けのURLです。"
    >
      <section className="table-section">
        <h2>IPTV</h2>
        <div className="copy-list">
          {links.map(([label, value]) => (
            <CopyRow key={label} label={label} value={value} />
          ))}
        </div>
      </section>
      <section className="table-section">
        <h2>サービス別ストリーム</h2>
        <div className="copy-list">
          {visibleServices.map((service) => (
            <CopyRow
              key={service.id}
              label={service.name}
              value={`${origin}/api/services/${service.id}/stream?decode=1`}
            />
          ))}
        </div>
      </section>
    </PageFrame>
  )
}
