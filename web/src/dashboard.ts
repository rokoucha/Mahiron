import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import {
  api,
  parseProgramEventData,
  streamEvents,
  type EventItem,
  type Channel,
  type Job,
  type Program,
  type ProgramEventData,
  type Service,
  type Status,
  type Tuner,
} from './api'
import { useAutoResource } from './hooks'

export type StreamConnectionState =
  'connected' | 'reconnecting' | 'disconnected'
export type DashboardResource =
  'status' | 'tuners' | 'services' | 'jobs' | 'programs' | 'channels'

export type DashboardData = {
  status: Status | null
  tuners: Tuner[] | null
  services: Service[] | null
  channels: Channel[] | null
  jobs: Job[] | null
  programs: Program[] | null
}

export type DashboardResourceState<T> = {
  data: T | null
  error: string | null
  loading: boolean
  reload: () => Promise<void>
}

export type DashboardState = {
  data: DashboardData
  status: DashboardResourceState<Status>
  tuners: DashboardResourceState<Tuner[]>
  services: DashboardResourceState<Service[]>
  channels: DashboardResourceState<Channel[]>
  jobs: DashboardResourceState<Job[]>
  programs: DashboardResourceState<Program[]>
  streamState: StreamConnectionState
  streamError: string | null
  lastEvent: EventItem | null
  refresh: (resources?: DashboardResource[]) => void
}

const pollIntervalMs = 30_000
const refreshDebounceMs = 250
const maxReconnectDelayMs = 30_000

function isFullProgramEventData(data: ProgramEventData): data is Program {
  return 'eventId' in data
}

// Which dashboard resources a given event should trigger a refresh for.
export function resourcesToRefresh(event: EventItem): DashboardResource[] {
  switch (event.resource) {
    case 'tuner':
      return ['tuners', 'status']
    case 'service':
      return ['services']
    case 'job':
    case 'job_schedule':
      return ['jobs', 'status']
    case 'program':
      return ['programs']
    default:
      return ['status']
  }
}

// Applies a program create/update/remove event to a program list. Returns the same
// array reference when the event doesn't concern the list, so callers can pass this
// straight to a state setter without causing a spurious re-render.
export function nextProgramList(
  current: Program[],
  event: EventItem,
): Program[] {
  const data = parseProgramEventData(event)
  if (!data) return current
  if (event.type === 'remove') {
    return current.filter((program) => program.id !== data.id)
  }
  if (!isFullProgramEventData(data)) {
    return current
  }
  const index = current.findIndex((program) => program.id === data.id)
  if (index < 0) {
    return [...current, data]
  }
  return current.map((program, currentIndex) =>
    currentIndex === index ? data : program,
  )
}

export function useDashboard(): DashboardState {
  const status = useAutoResource(api.status, { intervalMs: pollIntervalMs })
  const tuners = useAutoResource(api.tuners, { intervalMs: pollIntervalMs })
  const services = useAutoResource(api.services, {
    intervalMs: pollIntervalMs,
  })
  const channels = useAutoResource(api.channels, {
    intervalMs: pollIntervalMs,
  })
  const jobs = useAutoResource(api.jobs, { intervalMs: pollIntervalMs })
  const programs = useAutoResource(api.programs, {
    intervalMs: pollIntervalMs,
  })
  const [streamState, setStreamState] =
    useState<StreamConnectionState>('reconnecting')
  const [streamError, setStreamError] = useState<string | null>(null)
  const [lastEvent, setLastEvent] = useState<EventItem | null>(null)
  const refreshTimers = useRef<Partial<Record<DashboardResource, number>>>({})

  const reloaders = useMemo<Record<DashboardResource, () => Promise<void>>>(
    () => ({
      status: status.reload,
      tuners: tuners.reload,
      services: services.reload,
      channels: channels.reload,
      jobs: jobs.reload,
      programs: programs.reload,
    }),
    [
      jobs.reload,
      channels.reload,
      programs.reload,
      services.reload,
      status.reload,
      tuners.reload,
    ],
  )

  const refresh = useCallback(
    (
      resources: DashboardResource[] = [
        'status',
        'tuners',
        'services',
        'channels',
        'jobs',
        'programs',
      ],
    ) => {
      for (const resource of resources) {
        if (refreshTimers.current[resource] != null) continue
        refreshTimers.current[resource] = window.setTimeout(() => {
          delete refreshTimers.current[resource]
          void reloaders[resource]()
        }, refreshDebounceMs)
      }
    },
    [reloaders],
  )

  const onEvent = useCallback(
    (event: EventItem) => {
      setLastEvent(event)
      programs.setData((current) => nextProgramList(current ?? [], event))
      refresh(resourcesToRefresh(event))
    },
    [programs.setData, refresh],
  )

  useEffect(() => {
    let cancelled = false
    let controller: AbortController | null = null
    let reconnectDelayMs = 1_000

    function wait(ms: number) {
      return new Promise<void>((resolve) => {
        window.setTimeout(resolve, ms)
      })
    }

    async function connect() {
      setStreamState('reconnecting')
      while (!cancelled) {
        controller = new AbortController()
        let opened = false
        try {
          await streamEvents(controller.signal, onEvent, () => {
            opened = true
            reconnectDelayMs = 1_000
            setStreamState('connected')
            setStreamError(null)
            refresh()
          })
          if (cancelled || controller.signal.aborted) return
          setStreamState('reconnecting')
          setStreamError('events stream closed')
        } catch (err: unknown) {
          if (cancelled || controller.signal.aborted) return
          setStreamState(opened ? 'reconnecting' : 'disconnected')
          setStreamError(err instanceof Error ? err.message : String(err))
        }
        await wait(reconnectDelayMs)
        reconnectDelayMs = Math.min(reconnectDelayMs * 2, maxReconnectDelayMs)
        if (!cancelled) {
          setStreamState('reconnecting')
        }
      }
    }

    void connect()
    return () => {
      cancelled = true
      controller?.abort()
    }
  }, [onEvent, refresh])

  useEffect(() => {
    return () => {
      for (const timer of Object.values(refreshTimers.current)) {
        if (timer != null) window.clearTimeout(timer)
      }
    }
  }, [])

  return {
    data: {
      status: status.data,
      tuners: tuners.data,
      services: services.data,
      channels: channels.data,
      jobs: jobs.data,
      programs: programs.data,
    },
    status,
    tuners,
    services,
    channels,
    jobs,
    programs,
    streamState,
    streamError,
    lastEvent,
    refresh,
  }
}
