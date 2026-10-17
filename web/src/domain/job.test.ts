import { describe, expect, it } from 'vitest'
import type { Job } from '../api'
import { currentGatheringChannels, epgJobScope, jobStatusLabel } from './job'

const job = (overrides: Partial<Job>): Job => ({
  id: '1',
  key: 'job',
  name: 'job',
  status: 'queued',
  retryCount: 0,
  isAborting: false,
  createdAt: 0,
  updatedAt: 0,
  ...overrides,
})

describe('jobStatusLabel', () => {
  it('maps known statuses to Japanese labels', () => {
    expect(jobStatusLabel('queued')).toBe('待機中')
    expect(jobStatusLabel('running')).toBe('実行中')
  })
})

describe('currentGatheringChannels', () => {
  it('shows channel labels instead of internal stream keys for running jobs', () => {
    const jobs = [
      job({
        status: 'running',
        key: 'epg-gather:nid:65534:stream:65120',
        name: 'EPG Gather CATV/C17',
      }),
      job({
        status: 'running',
        key: 'epg-gather:nid:65534:stream:65188',
        name: 'EPG Gather CATV/C18',
      }),
      job({ status: 'finished', key: 'epg-gather:nid:2' }),
      job({ status: 'running', key: 'other:job' }),
    ]
    expect(currentGatheringChannels(jobs)).toEqual(['CATV/C17', 'CATV/C18'])
  })
})

describe('epgJobScope', () => {
  it('distinguishes unknown streams, a known zero stream, and stored network EPG', () => {
    expect(
      epgJobScope('epg-gather:nid:11:stream:0:channel:BS4K:45328'),
    ).toEqual({ onid: '11', stream: '未取得' })
    expect(epgJobScope('epg-gather:nid:4:stream:0')).toEqual({
      onid: '4',
      stream: '0',
    })
    expect(epgJobScope('epg-gather:nid:7')).toEqual({ onid: '7', stream: null })
    expect(epgJobScope('service-scan:GR:26')).toBeNull()
  })
})
