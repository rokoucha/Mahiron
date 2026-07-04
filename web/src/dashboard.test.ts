import { describe, expect, it } from 'vitest'
import type { EventItem, Program } from './api'
import { nextProgramList, resourcesToRefresh } from './dashboard'

const program = (overrides: Partial<Program>): Program => ({
  id: 1,
  eventId: 100,
  serviceId: 10,
  networkId: 1,
  startAt: 0,
  duration: 0,
  isFree: true,
  ...overrides,
})

function event(overrides: Partial<EventItem>): EventItem {
  return {
    resource: 'program',
    type: 'update',
    data: null,
    time: 0,
    ...overrides,
  }
}

describe('resourcesToRefresh', () => {
  it.each([
    ['tuner', ['tuners', 'status']],
    ['service', ['services']],
    ['job', ['jobs', 'status']],
    ['job_schedule', ['jobs', 'status']],
    ['program', []],
    ['something-unrecognized', ['status']],
  ] as const)('maps resource %s to %j', (resource, expected) => {
    expect(resourcesToRefresh(event({ resource }))).toEqual(expected)
  })
})

describe('nextProgramList', () => {
  it('appends a new program on a create event', () => {
    const created = program({ id: 2 })
    const result = nextProgramList([], event({ type: 'create', data: created }))
    expect(result).toEqual([created])
  })

  it('replaces the matching program on an update event', () => {
    const existing = program({ id: 3, name: 'before' })
    const updated = { ...existing, name: 'after' }
    const result = nextProgramList(
      [existing],
      event({ type: 'update', data: updated }),
    )
    expect(result).toEqual([updated])
  })

  it('removes the matching program on a remove event', () => {
    const existing = program({ id: 4 })
    const result = nextProgramList(
      [existing],
      event({ type: 'remove', data: { id: 4 } }),
    )
    expect(result).toEqual([])
  })

  it("returns the same array reference when the event isn't a program event", () => {
    const current = [program({ id: 5 })]
    const result = nextProgramList(
      current,
      event({ resource: 'tuner', type: 'update', data: {} }),
    )
    expect(result).toBe(current)
  })

  it('returns the same array reference for a malformed program payload', () => {
    const current = [program({ id: 6 })]
    const result = nextProgramList(
      current,
      event({ type: 'create', data: { not: 'a program' } }),
    )
    expect(result).toBe(current)
  })

  it("leaves the list unchanged when removing an id that isn't present", () => {
    const existing = program({ id: 7 })
    const result = nextProgramList(
      [existing],
      event({ type: 'remove', data: { id: 999 } }),
    )
    expect(result).toEqual([existing])
  })
})
