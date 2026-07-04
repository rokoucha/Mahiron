import { useMemo } from 'react'
import type { Tuner } from '../api'
import type { DashboardState } from '../dashboard'
import { currentGatheringNetworks } from '../domain/job'
import { channelLabel, isVisibleService } from '../domain/service'
import { openServiceMap, openServiceUsers } from '../domain/tuner'
import { formatNumber } from '../format/common'
import { formatDate } from '../format/date'
import { Empty, Logo, PageFrame, Panel } from '../ui/layout'
import { ErrorList } from '../ui/logs'
import { Definition, Metric, StatusPill } from '../ui/metrics'

function sortedTunerTypes(tuner: Tuner) {
  return [...tuner.types].sort((a, b) => a.localeCompare(b))
}

function userName(user: Tuner['users'][number]) {
  return user.agent || user.id
}

function userRoute(user: Tuner['users'][number]) {
  const setting = user.streamSetting
  if (!setting) return null
  if (setting.channel) {
    return channelLabel(setting.channel.type, setting.channel.channel)
  }
  if (setting.networkId !== undefined && setting.serviceId !== undefined) {
    return `${setting.networkId}/${setting.serviceId}`
  }
  return null
}

function userStreamInfo(user: Tuner['users'][number]) {
  if (!user.streamInfo) return []
  return Object.entries(user.streamInfo)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([key, info]) => `${key}: ${info.packet}pkt / ${info.drop}drop`)
}

function userMeta(user: Tuner['users'][number]) {
  const values = [`priority ${user.priority}`]
  if (user.disableDecoder) values.push('decoder off')
  const route = userRoute(user)
  if (route) values.push(route)
  if (user.streamSetting?.serviceId !== undefined) {
    values.push(`service ${user.streamSetting.serviceId}`)
  }
  if (user.streamSetting?.eventId !== undefined) {
    values.push(`event ${user.streamSetting.eventId}`)
  }
  return values
}

export default function Overview({ dashboard }: { dashboard: DashboardState }) {
  const { status, tuners, services, jobs } = dashboard
  const activeTuners = tuners.data?.filter((tuner) => tuner.isUsing).length ?? 0
  const faultTuners = tuners.data?.filter((tuner) => tuner.isFault).length ?? 0
  const activeJobs =
    jobs.data?.filter((job) => job.status !== 'finished').length ?? 0
  const gatheringNetworks = useMemo(
    () => currentGatheringNetworks(jobs.data ?? []),
    [jobs.data],
  )
  const epgGathering = gatheringNetworks.length > 0
  const visibleServices = useMemo(
    () => (services.data ?? []).filter(isVisibleService),
    [services.data],
  )
  const openServices = useMemo(
    () => openServiceMap(tuners.data ?? []),
    [tuners.data],
  )
  const openServiceCount = visibleServices.filter(
    (service) => openServiceUsers(openServices, service).length > 0,
  ).length

  return (
    <PageFrame
      title="概要"
      subtitle="サーバー状態、稼働中の処理、サービス状態を確認できます。"
    >
      <ErrorList
        errors={[
          dashboard.streamError,
          status.error,
          tuners.error,
          services.error,
          jobs.error,
        ]}
      />
      <section className="metric-grid">
        <Metric
          label="保存イベント数"
          value={formatNumber(status.data?.epg?.storedEvents)}
        />
        <Metric
          label="番組表未更新"
          value={formatNumber(status.data?.epg?.staleServices)}
          tone={(status.data?.epg?.staleServices ?? 0) > 0 ? 'warn' : 'ok'}
        />
        <Metric
          label="番組表失敗"
          value={formatNumber(status.data?.epg?.failedServices)}
          tone={(status.data?.epg?.failedServices ?? 0) > 0 ? 'bad' : 'ok'}
        />
        <Metric
          label="使用中チューナー"
          value={`${activeTuners}/${tuners.data?.length ?? '-'}`}
          tone={faultTuners > 0 ? 'bad' : 'ok'}
        />
        <Metric
          label="視聴中サービス"
          value={`${openServiceCount}/${services.loading ? '-' : visibleServices.length}`}
          tone={openServiceCount > 0 ? 'warn' : 'ok'}
        />
        <Metric
          label="実行中ジョブ"
          value={String(activeJobs)}
          tone={activeJobs > 0 ? 'warn' : 'ok'}
        />
        <Metric
          label="TSフィルター"
          value={formatNumber(status.data?.streamCount?.tsFilter)}
        />
        <Metric
          label="デコーダー"
          value={formatNumber(status.data?.streamCount?.decoder)}
        />
        <Metric
          label="チューナーデバイス"
          value={formatNumber(status.data?.streamCount?.tunerDevice)}
        />
      </section>
      <div className="overview-summary">
        <Panel title="番組表">
          <Definition
            label="取得状態"
            value={epgGathering ? '取得中' : '待機中'}
          />
          <Definition
            label="取得中ネットワーク"
            value={gatheringNetworks.join(', ') || 'なし'}
          />
          <Definition
            label="最終更新"
            value={
              status.data?.epg?.lastUpdatedAt
                ? formatDate(status.data.epg.lastUpdatedAt)
                : epgGathering
                  ? '取得中'
                  : '未完了'
            }
          />
          <Definition
            label="保存イベント数"
            value={formatNumber(status.data?.epg?.storedEvents)}
          />
        </Panel>
      </div>
      <Panel
        title={`サービス (${visibleServices.length})`}
        action={<span className="section-note">{openServiceCount} 使用中</span>}
      >
        <div className="overview-service-list">
          {visibleServices.map((service) => {
            const users = openServiceUsers(openServices, service)
            return (
              <div
                className={`overview-service ${users.length > 0 ? 'open' : ''}`}
                key={service.id}
                title={`${service.name} ${users.length > 0 ? `${users.map((user) => user.agent || user.id).join(', ')} が使用中` : '未使用'}`}
              >
                <Logo service={service} />
                <div className="overview-service-main">
                  <strong>{service.name}</strong>
                  <span>
                    {service.channel
                      ? `${service.channel.type} ${service.channel.channel}`
                      : `${service.networkId}/${service.serviceId}`}
                  </span>
                </div>
                <span
                  className={`service-epg ${service.epgReady ? 'ready' : ''}`}
                  aria-label={service.epgReady ? '番組表あり' : '番組表なし'}
                />
              </div>
            )
          })}
          {!services.loading && visibleServices.length === 0 && (
            <Empty message="表示対象のテレビサービスがありません。" />
          )}
        </div>
      </Panel>
      <div className="overview-tuners">
        <Panel title={`チューナー (${tuners.data?.length ?? 0})`}>
          <div className="overview-tuner-list">
            {(tuners.data ?? []).map((tuner) => (
              <div className="overview-tuner" key={tuner.index}>
                <div className="overview-tuner-main">
                  <div className="overview-tuner-header">
                    <div>
                      <strong>
                        #{tuner.index} {tuner.name}
                      </strong>
                      <span>
                        {sortedTunerTypes(tuner).join(', ') || '-'} ·{' '}
                        {channelLabel(
                          tuner.currentChannelType,
                          tuner.currentChannel,
                        )}{' '}
                        · {tuner.users.length}利用
                      </span>
                    </div>
                    <StatusPill tuner={tuner} />
                  </div>
                  {tuner.isUsing && tuner.command && (
                    <code>{tuner.command}</code>
                  )}
                  {tuner.users.length > 0 && (
                    <div className="overview-tuner-users">
                      {tuner.users.map((user) => (
                        <div className="overview-tuner-user" key={user.id}>
                          <strong>{userName(user)}</strong>
                          <span>{userMeta(user).join(' · ')}</span>
                          {user.url && <code>{user.url}</code>}
                          {userStreamInfo(user).map((info) => (
                            <span key={info}>{info}</span>
                          ))}
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              </div>
            ))}
            {!tuners.loading && tuners.data?.length === 0 && (
              <Empty message="チューナーが設定されていません。" />
            )}
          </div>
        </Panel>
      </div>
    </PageFrame>
  )
}
