import { renderToStaticMarkup } from 'react-dom/server'
import { describe, expect, it, vi } from 'vitest'
import type { Channel, Job, Program, Service, Status, Tuner } from '../api'
import type { DashboardResourceState, DashboardState } from '../dashboard'
import Jobs from './Jobs'

describe('Jobs', () => {
  it('renders service names and keeps EPG identifiers in target details', () => {
    const dashboard = {
      data: {
        status: null,
        tuners: null,
        services: null,
        channels: null,
        jobs: null,
        programs: null,
      },
      status: resource<Status>(null),
      tuners: resource<Tuner[]>(null),
      services: resource<Service[]>(null),
      channels: resource<Channel[]>(null),
      jobs: resource<Job[]>([
        {
          id: 'epg-1',
          key: 'epg-gather:nid:32737:stream:32737',
          name: 'EPG Gather GR/26',
          status: 'running',
          retryCount: 0,
          isAborting: false,
          createdAt: 1,
          updatedAt: 2,
        },
        {
          id: 'job-1',
          key: 'service-scan:GR:27',
          name: 'Service Scan GR/27',
          status: 'finished',
          retryCount: 0,
          isAborting: false,
          createdAt: 1,
          updatedAt: 2,
          result: {
            kind: 'service_scan',
            summary: 'GR/27: 2 services (1 added, 0 removed)',
            counts: {
              newNetworks: 1,
              existingServices: 0,
              services: 2,
              addedServices: 1,
              removedServices: 0,
            },
            items: [
              {
                kind: 'service',
                summary: 'NHK',
                data: {
                  name: 'NHK',
                  networkId: 32736,
                  serviceId: 101,
                  transportStreamId: 1,
                  remoteControlKeyId: 1,
                  change: 'added',
                },
              },
            ],
          },
        },
      ]),
      programs: resource<Program[]>(null),
      streamState: 'connected',
      streamError: null,
      lastEvent: null,
      refresh: vi.fn(),
    } satisfies DashboardState

    const html = renderToStaticMarkup(<Jobs dashboard={dashboard} />)

    expect(html).toContain('GR/27: 2 services')
    expect(html).toContain('サービス 2 / 追加 1 / 既存 0 / 削除 0 / 新規NID 1')
    expect(html).toContain('NHK')
    expect(html).toContain('追加')
    expect(html).toContain('SID 101')
    expect(html).toContain('<strong>EPG Gather GR/26</strong>')
    expect(html).toContain(
      '<details><summary>取得対象</summary><span>ONID 32737</span><span>ストリームID 32737</span><code>epg-gather:nid:32737:stream:32737</code></details>',
    )
  })
})

function resource<T>(data: T | null): DashboardResourceState<T> {
  return {
    data,
    error: null,
    loading: false,
    reload: vi.fn(async () => undefined),
  }
}
