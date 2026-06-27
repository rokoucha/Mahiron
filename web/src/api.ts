export type Status = {
  version?: string;
  process?: {
    pid?: number;
    memoryUsage?: {
      rss?: number;
      heapTotal?: number;
      heapUsed?: number;
    };
  };
  epg?: {
    gatheringNetworks?: number[];
    storedEvents?: number;
    staleServices?: number;
    failedServices?: number;
    lastUpdatedAt?: number;
  };
  streamCount?: {
    tunerDevice?: number;
    tsFilter?: number;
    decoder?: number;
  };
  errorCount?: Record<string, number>;
  timerAccuracy?: Record<string, unknown>;
};

export type Channel = {
  type: string;
  channel: string;
  name?: string;
};

export type Service = {
  id: number;
  serviceId: number;
  networkId: number;
  transportStreamId?: number;
  name: string;
  type: number;
  remoteControlKeyId?: number;
  epgReady?: boolean;
  epgUpdatedAt?: number;
  epgLastAttemptAt?: number;
  epgLastError?: string;
  channel?: Channel;
  hasLogoData?: boolean;
};

export type Program = {
  id: number;
  eventId: number;
  serviceId: number;
  networkId: number;
  startAt: number;
  duration: number;
  isFree: boolean;
  name?: string;
  description?: string;
  genres?: Array<{ lv1?: number; lv2?: number; un1?: number; un2?: number }>;
  video?: {
    type?: string;
    resolution?: string;
    streamContent?: number;
    componentType?: number;
  };
  audios?: Array<{
    componentType: number;
    componentTag?: number;
    isMain?: boolean;
    samplingRate?: number;
    langs?: string[];
  }>;
  extended?: Record<string, string>;
  series?: {
    id?: number;
    repeat?: number;
    pattern?: number;
    episode?: number;
    lastEpisode?: number;
    expiresAt?: number;
    name?: string;
  };
};

export type Tuner = {
  index: number;
  name: string;
  types: string[];
  command?: string;
  pid?: number;
  isAvailable: boolean;
  isRemote: boolean;
  isFree: boolean;
  isUsing: boolean;
  isFault: boolean;
  currentChannelType?: string;
  currentChannel?: string;
  tunedChannelType?: string;
  tunedChannel?: string;
  users: Array<{
    id: string;
    priority: number;
    agent?: string;
    url?: string;
    disableDecoder?: boolean;
    streamSetting?: {
      channel?: Channel;
      networkId?: number;
      serviceId?: number;
      eventId?: number;
      noProvide?: boolean;
      parseNIT?: boolean;
      parseSDT?: boolean;
      parseEIT?: boolean;
    };
    streamInfo?: Record<string, { packet: number; drop: number }>;
  }>;
};

export type Job = {
  id: string;
  key: string;
  name: string;
  status: "queued" | "standby" | "running" | "finished";
  retryCount: number;
  isAborting: boolean;
  hasAborted?: boolean;
  hasFailed?: boolean;
  createdAt: number;
  updatedAt: number;
  startedAt?: number;
  finishedAt?: number;
  nextRunAt?: number;
  duration?: number;
  error?: string;
};

export type JobSchedule = {
  key: string;
  schedule: string;
  job: {
    key: string;
    name: string;
  };
};

export type EventItem = {
  resource: string;
  type: string;
  data: unknown;
  time: number;
};

async function apiFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(path, {
    ...init,
    headers: {
      Accept: "application/json",
      ...init?.headers,
    },
  });
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}`);
  }
  const text = await response.text();
  if (text === "") {
    return undefined as T;
  }
  return JSON.parse(text) as T;
}

export const api = {
  status: () => apiFetch<Status>("/api/status"),
  services: () => apiFetch<Service[]>("/api/services"),
  programs: () => apiFetch<Program[]>("/api/programs"),
  tuners: () => apiFetch<Tuner[]>("/api/tuners"),
  jobs: () => apiFetch<Job[]>("/api/jobs"),
  schedules: () => apiFetch<JobSchedule[]>("/api/job-schedules"),
  events: () => apiFetch<EventItem[]>("/api/events"),
  log: async () => {
    const response = await fetch("/api/log");
    if (!response.ok) {
      throw new Error(`${response.status} ${response.statusText}`);
    }
    return response.text();
  },
  rerunJob: (id: string) => apiFetch<void>(`/api/jobs/${encodeURIComponent(id)}/rerun`, { method: "POST" }),
  abortJob: (id: string) => apiFetch<void>(`/api/jobs/${encodeURIComponent(id)}/abort`, { method: "POST" }),
  runSchedule: (key: string) =>
    apiFetch<void>(`/api/job-schedules/${encodeURIComponent(key)}/run`, { method: "POST" }),
};

export async function streamEvents(signal: AbortSignal, onEvent: (event: EventItem) => void, onOpen?: () => void) {
  const response = await fetch("/api/events/stream", { signal });
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}`);
  }
  if (!response.body) {
    throw new Error("events stream is not readable");
  }

  onOpen?.();
  const reader = response.body.pipeThrough(new TextDecoderStream()).getReader();
  let buffer = "";
  for (;;) {
    const { value, done } = await reader.read();
    if (done) return;
    buffer += value;
    const lines = buffer.split(/\r?\n/);
    buffer = lines.pop() ?? "";
    for (const line of lines) {
      const event = parseEventStreamLine(line);
      if (event) onEvent(event);
    }
  }
}

export async function streamLog(signal: AbortSignal, onChunk: (chunk: string) => void) {
  const response = await fetch("/api/log/stream", { signal });
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}`);
  }
  if (!response.body) {
    throw new Error("log stream is not readable");
  }

  const reader = response.body.pipeThrough(new TextDecoderStream()).getReader();
  for (;;) {
    const { value, done } = await reader.read();
    if (done) return;
    if (value) onChunk(value);
  }
}

function parseEventStreamLine(line: string): EventItem | null {
  const trimmed = line.trim();
  if (trimmed === "" || trimmed === "[" || trimmed === "]" || trimmed === ",") {
    return null;
  }
  const json = trimmed.endsWith(",") ? trimmed.slice(0, -1) : trimmed;
  if (json === "") {
    return null;
  }
  return JSON.parse(json) as EventItem;
}
