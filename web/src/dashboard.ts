import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { api, streamEvents, type EventItem, type Job, type Program, type Service, type Status, type Tuner } from "./api";
import { useAutoResource } from "./hooks";

export type StreamConnectionState = "connected" | "reconnecting" | "disconnected";
export type DashboardResource = "status" | "tuners" | "services" | "jobs" | "programs";

export type DashboardData = {
  status: Status | null;
  tuners: Tuner[] | null;
  services: Service[] | null;
  jobs: Job[] | null;
  programs: Program[] | null;
};

export type DashboardResourceState<T> = {
  data: T | null;
  error: string | null;
  loading: boolean;
  reload: () => Promise<void>;
};

export type DashboardState = {
  data: DashboardData;
  status: DashboardResourceState<Status>;
  tuners: DashboardResourceState<Tuner[]>;
  services: DashboardResourceState<Service[]>;
  jobs: DashboardResourceState<Job[]>;
  programs: DashboardResourceState<Program[]>;
  streamState: StreamConnectionState;
  streamError: string | null;
  lastEvent: EventItem | null;
  refresh: (resources?: DashboardResource[]) => void;
};

const pollIntervalMs = 30_000;
const refreshDebounceMs = 250;
const maxReconnectDelayMs = 30_000;

export function useDashboard(): DashboardState {
  const status = useAutoResource(api.status, { intervalMs: pollIntervalMs });
  const tuners = useAutoResource(api.tuners, { intervalMs: pollIntervalMs });
  const services = useAutoResource(api.services, { intervalMs: pollIntervalMs });
  const jobs = useAutoResource(api.jobs, { intervalMs: pollIntervalMs });
  const programs = useAutoResource(api.programs, { intervalMs: pollIntervalMs });
  const [streamState, setStreamState] = useState<StreamConnectionState>("reconnecting");
  const [streamError, setStreamError] = useState<string | null>(null);
  const [lastEvent, setLastEvent] = useState<EventItem | null>(null);
  const refreshTimers = useRef<Partial<Record<DashboardResource, number>>>({});

  const reloaders = useMemo<Record<DashboardResource, () => Promise<void>>>(() => ({
    status: status.reload,
    tuners: tuners.reload,
    services: services.reload,
    jobs: jobs.reload,
    programs: programs.reload,
  }), [jobs.reload, programs.reload, services.reload, status.reload, tuners.reload]);

  const refresh = useCallback((resources: DashboardResource[] = ["status", "tuners", "services", "jobs", "programs"]) => {
    for (const resource of resources) {
      if (refreshTimers.current[resource] != null) continue;
      refreshTimers.current[resource] = window.setTimeout(() => {
        delete refreshTimers.current[resource];
        void reloaders[resource]();
      }, refreshDebounceMs);
    }
  }, [reloaders]);

  const onEvent = useCallback((event: EventItem) => {
    setLastEvent(event);
    switch (event.resource) {
      case "tuner":
        refresh(["tuners", "status"]);
        break;
      case "service":
        refresh(["services"]);
        break;
      case "job":
      case "job_schedule":
        refresh(["jobs", "status"]);
        break;
      case "program":
        refresh(["programs"]);
        break;
      default:
        refresh(["status"]);
        break;
    }
  }, [refresh]);

  useEffect(() => {
    let cancelled = false;
    let controller: AbortController | null = null;
    let reconnectDelayMs = 1_000;

    function wait(ms: number) {
      return new Promise<void>((resolve) => {
        window.setTimeout(resolve, ms);
      });
    }

    async function connect() {
      setStreamState("reconnecting");
      while (!cancelled) {
        controller = new AbortController();
        let opened = false;
        try {
          await streamEvents(controller.signal, onEvent, () => {
            opened = true;
            reconnectDelayMs = 1_000;
            setStreamState("connected");
            setStreamError(null);
            refresh();
          });
          if (cancelled || controller.signal.aborted) return;
          setStreamState("reconnecting");
          setStreamError("events stream closed");
        } catch (err: unknown) {
          if (cancelled || controller.signal.aborted) return;
          setStreamState(opened ? "reconnecting" : "disconnected");
          setStreamError(err instanceof Error ? err.message : String(err));
        }
        await wait(reconnectDelayMs);
        reconnectDelayMs = Math.min(reconnectDelayMs * 2, maxReconnectDelayMs);
        if (!cancelled) {
          setStreamState("reconnecting");
        }
      }
    }

    void connect();
    return () => {
      cancelled = true;
      controller?.abort();
    };
  }, [onEvent, refresh]);

  useEffect(() => {
    return () => {
      for (const timer of Object.values(refreshTimers.current)) {
        if (timer != null) window.clearTimeout(timer);
      }
    };
  }, []);

  return {
    data: {
      status: status.data,
      tuners: tuners.data,
      services: services.data,
      jobs: jobs.data,
      programs: programs.data,
    },
    status,
    tuners,
    services,
    jobs,
    programs,
    streamState,
    streamError,
    lastEvent,
    refresh,
  };
}
