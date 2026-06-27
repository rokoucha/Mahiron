import { useEffect, useMemo, useState } from "react";
import { api, streamEvents, streamLog, type EventItem } from "../api";
import { useAsync } from "../hooks";
import { ErrorList, EventList, LogPanelActions, PageFrame, Panel, useAutoScroll } from "../shared";

export default function Logs() {
  const events = useAsync(api.events);
  const log = useAsync(api.log);
  const [liveEvents, setLiveEvents] = useState<EventItem[]>([]);
  const [liveLog, setLiveLog] = useState("");
  const [eventStreamError, setEventStreamError] = useState<string | null>(null);
  const [logStreamError, setLogStreamError] = useState<string | null>(null);
  const [eventStreamConnected, setEventStreamConnected] = useState(false);
  const [logStreamConnected, setLogStreamConnected] = useState(false);
  const eventAutoScroll = useAutoScroll<HTMLDivElement>([events.data, liveEvents]);
  const logAutoScroll = useAutoScroll<HTMLPreElement>([log.data, liveLog]);
  const allEvents = useMemo(() => {
    const combined = [...(events.data ?? []), ...liveEvents];
    return combined
      .sort((a, b) => a.time - b.time)
      .slice(-500);
  }, [events.data, liveEvents]);

  useEffect(() => {
    const controller = new AbortController();
    setEventStreamError(null);
    setEventStreamConnected(true);
    streamEvents(controller.signal, (event) => {
      setLiveEvents((current) => [...current, event].slice(-500));
    }, () => {
      setEventStreamConnected(true);
    }).catch((err: unknown) => {
      if (!controller.signal.aborted) {
        setEventStreamConnected(false);
        setEventStreamError(err instanceof Error ? err.message : String(err));
      }
    });
    return () => controller.abort();
  }, []);

  useEffect(() => {
    const controller = new AbortController();
    setLogStreamError(null);
    streamLog(controller.signal, (chunk) => {
      setLogStreamConnected(true);
      setLiveLog((current) => (current + chunk).slice(-200_000));
    }).catch((err: unknown) => {
      if (!controller.signal.aborted) {
        setLogStreamConnected(false);
        setLogStreamError(err instanceof Error ? err.message : String(err));
      }
    });
    return () => controller.abort();
  }, []);

  return (
    <PageFrame title="ログ" subtitle="APIイベントとサーバーログをリアルタイムに表示します。">
      <ErrorList errors={[events.error, log.error, eventStreamError, logStreamError]} />
      <div className="two-column logs-grid">
        <Panel
          title="イベント"
          action={<LogPanelActions connected={eventStreamConnected} following={eventAutoScroll.following} onFollow={eventAutoScroll.scrollToBottom} />}
        >
          <EventList events={allEvents} onScroll={eventAutoScroll.onScroll} scrollRef={eventAutoScroll.ref} />
        </Panel>
        <Panel
          title="ログ"
          action={<LogPanelActions connected={logStreamConnected} following={logAutoScroll.following} onFollow={logAutoScroll.scrollToBottom} />}
        >
          <pre className="log-view" onScroll={logAutoScroll.onScroll} ref={logAutoScroll.ref}>{(log.data ?? "") + liveLog || "ログはありません。"}</pre>
        </Panel>
      </div>
    </PageFrame>
  );
}
