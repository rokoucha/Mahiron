import { useCallback, useEffect, useRef, useState } from "react";

export function useAsync<T>(loader: () => Promise<T>, deps: unknown[] = []) {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    loader()
      .then((value) => {
        if (!cancelled) setData(value);
      })
      .catch((err: unknown) => {
        if (!cancelled) setError(err instanceof Error ? err.message : String(err));
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, deps);

  return { data, error, loading, setData };
}

export function useAutoResource<T>(loader: () => Promise<T>, options: { intervalMs?: number } = {}) {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const requestId = useRef(0);
  const mounted = useRef(false);

  const reload = useCallback(async () => {
    const currentRequest = requestId.current + 1;
    requestId.current = currentRequest;
    setLoading(true);
    try {
      const value = await loader();
      if (!mounted.current || requestId.current !== currentRequest) return;
      setData(value);
      setError(null);
    } catch (err: unknown) {
      if (!mounted.current || requestId.current !== currentRequest) return;
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      if (mounted.current && requestId.current === currentRequest) {
        setLoading(false);
      }
    }
  }, [loader]);

  useEffect(() => {
    mounted.current = true;
    void reload();
    return () => {
      mounted.current = false;
    };
  }, [reload]);

  useEffect(() => {
    if (!options.intervalMs) return;
    const interval = window.setInterval(() => {
      void reload();
    }, options.intervalMs);
    return () => window.clearInterval(interval);
  }, [options.intervalMs, reload]);

  return { data, error, loading, reload, setData };
}
