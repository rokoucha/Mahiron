import { describe, expect, it } from 'vitest'
import type { Channel, Service } from '../api'
import {
  channelKey,
  channelLabel,
  epgServiceGroupKey,
  epgServiceUnitKey,
  isStableEpgService,
  isTerrestrialService,
  isVisibleService,
  serviceKey,
  sortServicesForDisplay,
} from './service'

const service = (overrides: Partial<Service>): Service => ({
  id: 1,
  serviceId: 10,
  networkId: 1,
  name: 'svc',
  type: 1,
  ...overrides,
})

describe('isVisibleService', () => {
  it('is true for digital TV (0x01) and IPTV (0xad) service types', () => {
    expect(isVisibleService(service({ type: 0x01 }))).toBe(true)
    expect(isVisibleService(service({ type: 0xad }))).toBe(true)
  })

  it('is false for other service types', () => {
    expect(isVisibleService(service({ type: 0x02 }))).toBe(false)
  })
})

describe('sortServicesForDisplay', () => {
  const channels: Channel[] = [
    { type: 'GR', channel: '27' },
    { type: 'BS', channel: '101' },
    { type: 'BS', channel: '102' },
    { type: 'CS', channel: '001' },
  ]
  const sortableService = (name: string, overrides: Partial<Service>) =>
    service({
      id: name.charCodeAt(0),
      name,
      networkId: 1,
      serviceId: 0,
      channel: { type: 'GR', channel: '27' },
      ...overrides,
    })

  it('groups services by the channel type order from configured channels', () => {
    const services = [
      sortableService('a', {
        channel: { type: 'CS', channel: '001' },
      }),
      sortableService('b', {
        channel: { type: 'BS', channel: '101' },
      }),
      sortableService('c', {
        channel: { type: 'GR', channel: '27' },
      }),
    ]

    expect(
      sortServicesForDisplay(services, channels).map((item) => item.name),
    ).toEqual(['c', 'b', 'a'])
  })

  it('groups scattered configured channel types and sorts services within each type', () => {
    const scatteredChannels = [
      { type: 'GR', channel: '27' },
      { type: 'BS', channel: '101' },
      { type: 'GR', channel: '26' },
      { type: 'CS', channel: '001' },
      { type: 'BS', channel: '102' },
    ]
    const services = [
      sortableService('a', { channel: { type: 'BS', channel: '102' } }),
      sortableService('b', { channel: { type: 'CS', channel: '001' } }),
      sortableService('c', { channel: { type: 'GR', channel: '26' } }),
      sortableService('d', { channel: { type: 'BS', channel: '101' } }),
      sortableService('e', { channel: { type: 'GR', channel: '27' } }),
    ]

    expect(
      sortServicesForDisplay(services, scatteredChannels).map(
        (item) => item.name,
      ),
    ).toEqual(['c', 'e', 'a', 'd', 'b'])
  })

  it('keeps newly added services from changing existing channel type order', () => {
    const services = [
      sortableService('a', { channel: { type: 'CS', channel: '001' } }),
      sortableService('b', { channel: { type: 'BS', channel: '101' } }),
      sortableService('c', { channel: { type: 'GR', channel: '27' } }),
      sortableService('d', { channel: { type: 'BS', channel: '102' } }),
    ]

    expect(
      sortServicesForDisplay(services, channels).map((item) => item.name),
    ).toEqual(['c', 'b', 'd', 'a'])
  })

  it('places unconfigured channel types and channels after configured services', () => {
    const services = [
      sortableService('a', { channel: { type: 'SKY', channel: 'SKY001' } }),
      sortableService('b', { channel: { type: 'BS', channel: '999' } }),
      sortableService('c', { channel: { type: 'GR', channel: '27' } }),
      sortableService('d', { channel: { type: 'BS', channel: '101' } }),
      sortableService('e', { channel: { type: 'CS', channel: '001' } }),
    ]

    expect(
      sortServicesForDisplay(services, channels).map((item) => item.name),
    ).toEqual(['c', 'd', 'e', 'b', 'a'])
  })

  it('sorts by remote control key inside a channel.type group', () => {
    const services = [
      sortableService('a', { remoteControlKeyId: 5 }),
      sortableService('b', { remoteControlKeyId: 1 }),
      sortableService('c', { remoteControlKeyId: 3 }),
    ]

    expect(
      sortServicesForDisplay(services, channels).map((item) => item.name),
    ).toEqual(['b', 'c', 'a'])
  })

  it('sorts services with the same remote key by the ARIB three-digit channel components', () => {
    const services = [
      sortableService('a', { remoteControlKeyId: 2, serviceId: 0x0080 }),
      sortableService('b', { remoteControlKeyId: 2, serviceId: 0x0001 }),
      sortableService('c', { remoteControlKeyId: 2, serviceId: 0x0000 }),
    ]

    expect(
      sortServicesForDisplay(services, channels).map((item) => item.name),
    ).toEqual(['c', 'b', 'a'])
  })

  it('keeps services from the same terrestrial network together when remote keys conflict', () => {
    const services = [
      sortableService('a', {
        networkId: 32738,
        remoteControlKeyId: 3,
        serviceId: 0x0808,
      }),
      sortableService('b', {
        networkId: 32737,
        remoteControlKeyId: 3,
        serviceId: 0x0409,
      }),
      sortableService('c', {
        networkId: 32738,
        remoteControlKeyId: 3,
        serviceId: 0x0809,
      }),
      sortableService('d', {
        networkId: 32737,
        remoteControlKeyId: 3,
        serviceId: 0x0408,
      }),
    ]

    expect(
      sortServicesForDisplay(services, channels).map((item) => item.name),
    ).toEqual(['d', 'b', 'a', 'c'])
  })

  it('sorts BS services by logical channel number without relying on channel.type', () => {
    const userChannels = [{ type: 'USER_DEFINED', channel: 'SAT' }]
    const services = [
      sortableService('a', {
        networkId: 0x0004,
        serviceId: 151,
        channel: { type: 'USER_DEFINED', channel: 'SAT' },
      }),
      sortableService('b', {
        networkId: 0x0004,
        serviceId: 102,
        channel: { type: 'USER_DEFINED', channel: 'SAT' },
      }),
      sortableService('c', {
        networkId: 0x0004,
        serviceId: 143,
        channel: { type: 'USER_DEFINED', channel: 'SAT' },
      }),
      sortableService('d', {
        networkId: 0x0004,
        serviceId: 101,
        channel: { type: 'USER_DEFINED', channel: 'SAT' },
      }),
      sortableService('e', {
        networkId: 0x0004,
        serviceId: 141,
        channel: { type: 'USER_DEFINED', channel: 'SAT' },
      }),
      sortableService('f', {
        networkId: 0x0004,
        serviceId: 103,
        channel: { type: 'USER_DEFINED', channel: 'SAT' },
      }),
      sortableService('g', {
        networkId: 0x0004,
        serviceId: 142,
        channel: { type: 'USER_DEFINED', channel: 'SAT' },
      }),
    ]

    expect(
      sortServicesForDisplay(services, userChannels).map(
        (item) => item.serviceId,
      ),
    ).toEqual([101, 102, 103, 141, 142, 143, 151])
  })

  it('sorts BS services by broadcast logical channel before configured channel order', () => {
    const bsChannels = [
      { type: 'USER_DEFINED', channel: 'BS151_0' },
      { type: 'USER_DEFINED', channel: 'BS141_0' },
      { type: 'USER_DEFINED', channel: 'BS101_0' },
    ]
    const services = [
      sortableService('a', {
        networkId: 0x0004,
        serviceId: 151,
        channel: { type: 'USER_DEFINED', channel: 'BS151_0' },
      }),
      sortableService('b', {
        networkId: 0x0004,
        serviceId: 141,
        channel: { type: 'USER_DEFINED', channel: 'BS141_0' },
      }),
      sortableService('c', {
        networkId: 0x0004,
        serviceId: 101,
        channel: { type: 'USER_DEFINED', channel: 'BS101_0' },
      }),
    ]

    expect(
      sortServicesForDisplay(services, bsChannels).map(
        (item) => item.serviceId,
      ),
    ).toEqual([101, 141, 151])
  })

  it('sorts CS satellite services by logical channel number', () => {
    const userChannels = [{ type: 'USER_DEFINED', channel: 'CS' }]
    const services = [
      sortableService('a', {
        networkId: 0x0007,
        serviceId: 294,
        channel: { type: 'USER_DEFINED', channel: 'CS' },
      }),
      sortableService('b', {
        networkId: 0x0006,
        serviceId: 296,
        channel: { type: 'USER_DEFINED', channel: 'CS' },
      }),
      sortableService('c', {
        networkId: 0x0007,
        serviceId: 250,
        channel: { type: 'USER_DEFINED', channel: 'CS' },
      }),
    ]

    expect(
      sortServicesForDisplay(services, userChannels).map(
        (item) => item.serviceId,
      ),
    ).toEqual([250, 294, 296])
  })

  it('does not let satellite remote control keys override logical channel order', () => {
    const userChannels = [{ type: 'USER_DEFINED', channel: 'SAT' }]
    const services = [
      sortableService('a', {
        networkId: 0x0004,
        serviceId: 141,
        remoteControlKeyId: 0,
        channel: { type: 'USER_DEFINED', channel: 'SAT' },
      }),
      sortableService('b', {
        networkId: 0x0004,
        serviceId: 101,
        channel: { type: 'USER_DEFINED', channel: 'SAT' },
      }),
    ]

    expect(
      sortServicesForDisplay(services, userChannels).map(
        (item) => item.serviceId,
      ),
    ).toEqual([101, 141])
  })

  it('uses stable fallbacks when channel or remote key is missing', () => {
    const services = [
      sortableService('a', {
        id: 3,
        networkId: 2,
        serviceId: 20,
        channel: undefined,
      }),
      sortableService('b', {
        id: 2,
        networkId: 1,
        serviceId: 10,
        channel: undefined,
      }),
      sortableService('c', {
        id: 1,
        networkId: 1,
        serviceId: 10,
        channel: undefined,
      }),
    ]

    expect(
      sortServicesForDisplay(services, channels).map((item) => item.name),
    ).toEqual(['c', 'b', 'a'])
  })
})

describe('isTerrestrialService', () => {
  it('is true only when remoteControlKeyId is set', () => {
    expect(isTerrestrialService(service({ remoteControlKeyId: 5 }))).toBe(true)
    expect(isTerrestrialService(service({}))).toBe(false)
  })

  it('is false for satellite services even when remoteControlKeyId is present', () => {
    expect(
      isTerrestrialService(
        service({ networkId: 0x0004, remoteControlKeyId: 0 }),
      ),
    ).toBe(false)
  })
})

describe('isStableEpgService', () => {
  it('requires epgReady and a non-false eitScheduleFlag', () => {
    expect(
      isStableEpgService(service({ epgReady: true, eitScheduleFlag: true })),
    ).toBe(true)
    expect(isStableEpgService(service({ epgReady: true }))).toBe(true)
    expect(
      isStableEpgService(service({ epgReady: true, eitScheduleFlag: false })),
    ).toBe(false)
    expect(isStableEpgService(service({ epgReady: false }))).toBe(false)
  })
})

describe('channelLabel', () => {
  it('joins type and channel when both are present', () => {
    expect(channelLabel('GR', '27')).toBe('GR 27')
  })

  it('returns - when either is missing', () => {
    expect(channelLabel(undefined, '27')).toBe('-')
    expect(channelLabel('GR', undefined)).toBe('-')
  })
})

describe('serviceKey / epgServiceUnitKey', () => {
  it('prefers the numeric service id when present', () => {
    expect(epgServiceUnitKey(service({ id: 42 }))).toBe('service-id:42')
  })

  it('falls back to transport stream id, then network/service id', () => {
    expect(
      epgServiceUnitKey({
        id: undefined as unknown as number,
        networkId: 1,
        serviceId: 10,
        transportStreamId: 5,
      }),
    ).toBe('service:1:5:10')
    expect(
      epgServiceUnitKey({
        id: undefined as unknown as number,
        networkId: 1,
        serviceId: 10,
      }),
    ).toBe(serviceKey({ networkId: 1, serviceId: 10 }))
  })
})

describe('epgServiceGroupKey', () => {
  it('groups terrestrial services by channel/network/TSID/remote-control-key', () => {
    const svc = service({
      remoteControlKeyId: 5,
      transportStreamId: 32736,
      channel: { type: 'GR', channel: '27' },
    })
    expect(epgServiceGroupKey(svc)).toBe('terrestrial:GR:27:1:32736:5')
  })

  it('falls back to the unit key for non-terrestrial services', () => {
    const svc = service({ id: 7 })
    expect(epgServiceGroupKey(svc)).toBe(epgServiceUnitKey(svc))
  })
})

describe('channelKey', () => {
  it('returns empty string when type or channel is missing', () => {
    expect(channelKey(undefined)).toBe('')
    expect(channelKey({ type: '', channel: '27' })).toBe('')
  })

  it('builds a composite key otherwise', () => {
    expect(channelKey({ type: 'GR', channel: '27' })).toBe('channel:GR:27')
  })
})
