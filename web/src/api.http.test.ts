import { afterEach, describe, expect, it, vi } from 'vitest'
import { api, streamEvents, streamLog, type EventItem } from './api'

function streamOf(chunks: string[]): ReadableStream<Uint8Array> {
  return new ReadableStream({
    start(controller) {
      for (const chunk of chunks)
        controller.enqueue(new TextEncoder().encode(chunk))
      controller.close()
    },
  })
}

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('api.status (apiFetch)', () => {
  it('parses a successful JSON response', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(() =>
        Promise.resolve(
          new Response(JSON.stringify({ version: '5.0.0' }), { status: 200 }),
        ),
      ),
    )

    await expect(api.status()).resolves.toEqual({ version: '5.0.0' })
  })

  it('treats an empty body as undefined', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(() => Promise.resolve(new Response('', { status: 200 }))),
    )

    await expect(api.status()).resolves.toBeUndefined()
  })

  it('rejects with the status text when the response is not ok', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(() =>
        Promise.resolve(
          new Response('', {
            status: 500,
            statusText: 'Internal Server Error',
          }),
        ),
      ),
    )

    await expect(api.status()).rejects.toThrow('500 Internal Server Error')
  })
})

describe('api.programs', () => {
  it('adds the requested time window to the query string', async () => {
    const fetchMock = vi.fn(() =>
      Promise.resolve(new Response('[]', { status: 200 })),
    )
    vi.stubGlobal('fetch', fetchMock)

    await api.programs({ startAt: 1_000, endAt: 2_000 })

    expect(fetchMock).toHaveBeenCalledWith(
      '/api/programs?startAt=1000&endAt=2000',
      expect.objectContaining({
        headers: { Accept: 'application/json' },
      }),
    )
  })
})

describe('api job actions', () => {
  it('PUTs to the encoded rerun endpoint', async () => {
    const fetchMock = vi.fn(() =>
      Promise.resolve(new Response('', { status: 200 })),
    )
    vi.stubGlobal('fetch', fetchMock)

    await api.rerunJob('job/with slash')

    expect(fetchMock).toHaveBeenCalledWith(
      '/api/jobs/job%2Fwith%20slash/rerun',
      expect.objectContaining({ method: 'PUT' }),
    )
  })

  it('PUTs to the encoded abort endpoint', async () => {
    const fetchMock = vi.fn(() =>
      Promise.resolve(new Response('', { status: 200 })),
    )
    vi.stubGlobal('fetch', fetchMock)

    await api.abortJob('job/with slash')

    expect(fetchMock).toHaveBeenCalledWith(
      '/api/jobs/job%2Fwith%20slash/abort',
      expect.objectContaining({ method: 'PUT' }),
    )
  })

  it('PUTs to the encoded schedule run endpoint', async () => {
    const fetchMock = vi.fn(() =>
      Promise.resolve(new Response('', { status: 200 })),
    )
    vi.stubGlobal('fetch', fetchMock)

    await api.runSchedule('schedule/with slash')

    expect(fetchMock).toHaveBeenCalledWith(
      '/api/job-schedules/schedule%2Fwith%20slash/run',
      expect.objectContaining({ method: 'PUT' }),
    )
  })
})

describe('streamEvents', () => {
  it('parses newline-delimited JSON events and skips array framing tokens', async () => {
    const event: EventItem = {
      resource: 'tuner',
      type: 'update',
      data: {},
      time: 1,
    }
    const body = streamOf(['[\n', `${JSON.stringify(event)},\n`, ']\n'])
    vi.stubGlobal(
      'fetch',
      vi.fn(() => Promise.resolve(new Response(body, { status: 200 }))),
    )

    const onEvent = vi.fn()
    const onOpen = vi.fn()
    await streamEvents(new AbortController().signal, onEvent, onOpen)

    expect(onOpen).toHaveBeenCalledOnce()
    expect(onEvent).toHaveBeenCalledExactlyOnceWith(event)
  })

  it('throws when the response is not ok', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(() =>
        Promise.resolve(
          new Response('', { status: 503, statusText: 'Unavailable' }),
        ),
      ),
    )

    await expect(
      streamEvents(new AbortController().signal, vi.fn()),
    ).rejects.toThrow('503 Unavailable')
  })

  it('throws when the response has no body', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(() => Promise.resolve(new Response(null, { status: 200 }))),
    )

    await expect(
      streamEvents(new AbortController().signal, vi.fn()),
    ).rejects.toThrow('events stream is not readable')
  })
})

describe('streamLog', () => {
  it('forwards each decoded chunk', async () => {
    const body = streamOf(['hello ', 'world\n'])
    vi.stubGlobal(
      'fetch',
      vi.fn(() => Promise.resolve(new Response(body, { status: 200 }))),
    )

    const onChunk = vi.fn()
    await streamLog(new AbortController().signal, onChunk)

    expect(onChunk.mock.calls.map((call) => call[0]).join('')).toBe(
      'hello world\n',
    )
  })

  it('throws when the response is not ok', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(() =>
        Promise.resolve(new Response('', { status: 500, statusText: 'Boom' })),
      ),
    )

    await expect(
      streamLog(new AbortController().signal, vi.fn()),
    ).rejects.toThrow('500 Boom')
  })
})
