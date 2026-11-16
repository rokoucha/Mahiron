import type {
  Event,
  EventType,
  Program as APIProgram,
  Status,
  Channel,
  Service,
  TunerDevice as Tuner,
  JobItem as Job,
  JobScheduleItem as JobSchedule,
} from './generated/types.gen'

export type {
  Status,
  Channel,
  Service,
  TunerDevice as Tuner,
  JobItem as Job,
  JobResult,
  JobResultItem,
  JobScheduleItem as JobSchedule,
} from './generated/types.gen'

// The schema leaves extended untyped so Go preserves broadcast item order.
// The server encodes it as an object of strings (see api.yml).
export type Program = Omit<APIProgram, 'extended'> & {
  extended?: Record<string, string>
}
export type ProgramEventType = EventType
export type ProgramRemoveEventData = Pick<Program, 'id'>
export type ProgramEventData = Program | ProgramRemoveEventData

// Stream input is untrusted and may contain future event kinds or malformed data.
// Empty server event payloads can also be null.
export type EventItem = Omit<Event, 'resource' | 'type' | 'data'> & {
  resource: string
  type: string
  data: unknown
}

export function parseProgramEventData(
  event: EventItem,
): ProgramEventData | null {
  if (event.resource !== 'program') return null
  if (event.type === 'remove') {
    return isProgramRemoveEventData(event.data) ? event.data : null
  }
  if (event.type === 'create' || event.type === 'update') {
    return isProgram(event.data) ? event.data : null
  }
  return null
}

async function apiFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(path, {
    ...init,
    headers: {
      Accept: 'application/json',
      ...init?.headers,
    },
  })
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}`)
  }
  const text = await response.text()
  if (text === '') {
    return undefined as T
  }
  return JSON.parse(text) as T
}

export const api = {
  status: () => apiFetch<Status>('/api/status'),
  channels: () => apiFetch<Channel[]>('/api/channels'),
  services: () => apiFetch<Service[]>('/api/services'),
  programs: (params: { startAt?: number; endAt?: number } = {}) => {
    const query = new URLSearchParams()
    if (params.startAt != null) query.set('startAt', String(params.startAt))
    if (params.endAt != null) query.set('endAt', String(params.endAt))
    const suffix = query.size === 0 ? '' : `?${query}`
    return apiFetch<Program[]>(`/api/programs${suffix}`)
  },
  tuners: () => apiFetch<Tuner[]>('/api/tuners'),
  jobs: () => apiFetch<Job[]>('/api/jobs'),
  schedules: () => apiFetch<JobSchedule[]>('/api/job-schedules'),
  events: () => apiFetch<EventItem[]>('/api/events'),
  log: async () => {
    const response = await fetch('/api/log')
    if (!response.ok) {
      throw new Error(`${response.status} ${response.statusText}`)
    }
    return response.text()
  },
  rerunJob: (id: string) =>
    apiFetch<void>(`/api/jobs/${encodeURIComponent(id)}/rerun`, {
      method: 'PUT',
    }),
  abortJob: (id: string) =>
    apiFetch<void>(`/api/jobs/${encodeURIComponent(id)}/abort`, {
      method: 'PUT',
    }),
  runSchedule: (key: string) =>
    apiFetch<void>(`/api/job-schedules/${encodeURIComponent(key)}/run`, {
      method: 'PUT',
    }),
}

export async function streamEvents(
  signal: AbortSignal,
  onEvent: (event: EventItem) => void,
  onOpen?: () => void,
) {
  const response = await fetch('/api/events/stream', { signal })
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}`)
  }
  if (!response.body) {
    throw new Error('events stream is not readable')
  }

  onOpen?.()
  const reader = response.body.pipeThrough(new TextDecoderStream()).getReader()
  let buffer = ''
  for (;;) {
    const { value, done } = await reader.read()
    if (done) return
    buffer += value
    const lines = buffer.split(/\r?\n/)
    buffer = lines.pop() ?? ''
    for (const line of lines) {
      const event = parseEventStreamLine(line)
      if (event) onEvent(event)
    }
  }
}

export async function streamLog(
  signal: AbortSignal,
  onChunk: (chunk: string) => void,
) {
  const response = await fetch('/api/log/stream', { signal })
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}`)
  }
  if (!response.body) {
    throw new Error('log stream is not readable')
  }

  const reader = response.body.pipeThrough(new TextDecoderStream()).getReader()
  for (;;) {
    const { value, done } = await reader.read()
    if (done) return
    if (value) onChunk(value)
  }
}

function parseEventStreamLine(line: string): EventItem | null {
  const trimmed = line.trim()
  if (trimmed === '' || trimmed === '[' || trimmed === ']' || trimmed === ',') {
    return null
  }
  const json = trimmed.endsWith(',') ? trimmed.slice(0, -1) : trimmed
  if (json === '') {
    return null
  }
  return JSON.parse(json) as EventItem
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null
}

function isProgramRemoveEventData(
  value: unknown,
): value is ProgramRemoveEventData {
  return isRecord(value) && typeof value.id === 'number'
}

function isProgram(value: unknown): value is Program {
  return (
    isRecord(value) &&
    typeof value.id === 'number' &&
    typeof value.eventId === 'number' &&
    typeof value.serviceId === 'number' &&
    typeof value.networkId === 'number' &&
    typeof value.startAt === 'number' &&
    typeof value.duration === 'number' &&
    typeof value.isFree === 'boolean'
  )
}
