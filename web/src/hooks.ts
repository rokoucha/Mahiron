import { useCallback, useEffect, useRef, useState, type UIEvent } from 'react'

export function useAsync<T>(loader: () => Promise<T>, deps: unknown[] = []) {
  const [data, setData] = useState<T | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)
    loader()
      .then((value) => {
        if (!cancelled) setData(value)
      })
      .catch((err: unknown) => {
        if (!cancelled)
          setError(err instanceof Error ? err.message : String(err))
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => {
      cancelled = true
    }
  }, deps)

  return { data, error, loading, setData }
}

export function useAutoResource<T>(
  loader: () => Promise<T>,
  options: { intervalMs?: number; enabled?: boolean } = {},
) {
  const [data, setData] = useState<T | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)
  const requestId = useRef(0)
  const mounted = useRef(false)

  const reload = useCallback(async () => {
    const currentRequest = requestId.current + 1
    requestId.current = currentRequest
    setLoading(true)
    try {
      const value = await loader()
      if (!mounted.current || requestId.current !== currentRequest) return
      setData(value)
      setError(null)
    } catch (err: unknown) {
      if (!mounted.current || requestId.current !== currentRequest) return
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      if (mounted.current && requestId.current === currentRequest) {
        setLoading(false)
      }
    }
  }, [loader])

  useEffect(() => {
    mounted.current = true
    if (options.enabled !== false) void reload()
    return () => {
      mounted.current = false
    }
  }, [options.enabled, reload])

  useEffect(() => {
    if (options.enabled === false || !options.intervalMs) return
    const interval = window.setInterval(() => {
      void reload()
    }, options.intervalMs)
    return () => window.clearInterval(interval)
  }, [options.enabled, options.intervalMs, reload])

  return { data, error, loading, reload, setData }
}

export function useAutoScroll<T extends HTMLElement>(deps: unknown[]) {
  const ref = useRef<T | null>(null)
  const shouldFollowRef = useRef(true)
  const [following, setFollowing] = useState(true)

  function updateFollowing(element: T) {
    const distanceToBottom =
      element.scrollHeight - element.scrollTop - element.clientHeight
    const nextFollowing = distanceToBottom < 48
    shouldFollowRef.current = nextFollowing
    setFollowing(nextFollowing)
  }

  function scrollToBottom() {
    const element = ref.current
    if (!element) return
    shouldFollowRef.current = true
    setFollowing(true)
    element.scrollTop = element.scrollHeight
  }

  function onScroll(event: UIEvent<T>) {
    updateFollowing(event.currentTarget)
  }

  useEffect(() => {
    const element = ref.current
    if (!element) return

    const onScroll = () => {
      updateFollowing(element)
    }

    onScroll()
    element.addEventListener('scroll', onScroll, { passive: true })
    return () => element.removeEventListener('scroll', onScroll)
  }, [])

  useEffect(() => {
    const element = ref.current
    if (!element || !shouldFollowRef.current) return
    element.scrollTop = element.scrollHeight
  }, deps)

  return { ref, following, onScroll, scrollToBottom }
}
