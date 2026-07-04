import type { Channel, Service } from '../api'

export function isVisibleService(service: Service) {
  return service.type === 0x01 || service.type === 0xad
}

export function sortServicesForDisplay(
  services: Service[],
  channels: Channel[],
) {
  const { channelOrder, channelTypeOrder } = displayOrder(channels)

  return [...services].sort(
    (a, b) =>
      compareNumbers(
        configuredChannelSortNumber(a, channelOrder),
        configuredChannelSortNumber(b, channelOrder),
      ) ||
      compareNumbers(
        channelTypeSortNumber(a, channelTypeOrder),
        channelTypeSortNumber(b, channelTypeOrder),
      ) ||
      compareStrings(a.channel?.type ?? '', b.channel?.type ?? '') ||
      compareDisplayRemoteControlKeys(a, b) ||
      compareNumbers(a.serviceId, b.serviceId) ||
      compareNumbers(a.networkId, b.networkId) ||
      compareOptionalNumbers(a.transportStreamId, b.transportStreamId) ||
      compareNumbers(a.id, b.id),
  )
}

function displayOrder(channels: Channel[]) {
  const channelTypeOrder = new Map<string, number>()
  const channelOrder = new Map<string, number>()
  const channelOrderByType = new Map<string, number>()

  for (const channel of channels) {
    if (!channelTypeOrder.has(channel.type)) {
      channelTypeOrder.set(channel.type, channelTypeOrder.size)
    }
    const key = channelKey(channel)
    if (!channelOrder.has(key)) {
      const typeOrder = channelOrderByType.get(channel.type) ?? 0
      channelOrder.set(key, typeOrder)
      channelOrderByType.set(channel.type, typeOrder + 1)
    }
  }

  return { channelOrder, channelTypeOrder }
}

function channelTypeSortNumber(
  service: Service,
  channelTypeOrder: Map<string, number>,
) {
  return channelTypeOrder.get(service.channel?.type ?? '') ?? Number.MAX_VALUE
}

function configuredChannelSortNumber(
  service: Service,
  channelOrder: Map<string, number>,
) {
  return channelOrder.has(channelKey(service.channel)) ? 0 : 1
}

export function isTerrestrialService(service: Service) {
  // remoteControlKeyId is set from TSInformationDescriptor (tag 0xCD), terrestrial NIT only
  return !isSatelliteService(service) && service.remoteControlKeyId != null
}

function isSatelliteService(service: Pick<Service, 'networkId'>) {
  return (
    service.networkId === 0x0004 ||
    service.networkId === 0x0006 ||
    service.networkId === 0x0007
  )
}

export function isStableEpgService(service: Service) {
  return service.eitScheduleFlag !== false && service.epgReady === true
}

export function channelLabel(type?: string, channel?: string) {
  return type && channel ? `${type} ${channel}` : '-'
}

export function serviceKey(service: Pick<Service, 'networkId' | 'serviceId'>) {
  return `service:${service.networkId}:${service.serviceId}`
}

export function epgServiceUnitKey(
  service: Pick<
    Service,
    'id' | 'networkId' | 'serviceId' | 'transportStreamId'
  >,
) {
  if (service.id != null) {
    return `service-id:${service.id}`
  }
  if (service.transportStreamId != null) {
    return `service:${service.networkId}:${service.transportStreamId}:${service.serviceId}`
  }
  return serviceKey(service)
}

export function epgServiceGroupKey(service: Service) {
  if (
    isTerrestrialService(service) &&
    service.transportStreamId != null &&
    service.remoteControlKeyId != null
  ) {
    return [
      'terrestrial',
      service.channel?.type ?? '',
      service.channel?.channel ?? '',
      service.networkId,
      service.transportStreamId,
      service.remoteControlKeyId,
    ].join(':')
  }
  return epgServiceUnitKey(service)
}

export function channelKey(channel?: Pick<Channel, 'type' | 'channel'>) {
  if (!channel?.type || !channel.channel) return ''
  return `channel:${channel.type}:${channel.channel}`
}

function compareDisplayRemoteControlKeys(a: Service, b: Service) {
  return compareOptionalNumbers(
    displayRemoteControlKeyId(a),
    displayRemoteControlKeyId(b),
  )
}

function displayRemoteControlKeyId(service: Service) {
  return isTerrestrialService(service) ? service.remoteControlKeyId : undefined
}

function compareOptionalNumbers(a: number | undefined, b: number | undefined) {
  if (a == null && b == null) return 0
  if (a == null) return 1
  if (b == null) return -1
  return compareNumbers(a, b)
}

function compareNumbers(a: number, b: number) {
  return a - b
}

function compareStrings(a: string, b: string) {
  return a.localeCompare(b)
}
