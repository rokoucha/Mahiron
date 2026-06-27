import { useMemo, useState } from "react";
import { api, type Program } from "../api";
import { useAsync } from "../hooks";
import { Empty, ErrorList, floorHour, formatHourOnly, formatMinute, formatMonthDayWeekday, isSameDate, isVisibleService, Logo, makeEpgColumns, makeEpgProgramBlocks, PageFrame, ProgramModal, programGenreClass } from "../shared";

export default function EPG() {
  const services = useAsync(api.services);
  const programs = useAsync(api.programs);
  const [selected, setSelected] = useState<Program | null>(null);
  const now = Date.now();
  const windowStart = floorHour(now);
  const windowEnd = windowStart + 6 * 60 * 60 * 1000;
  const pxPerMinute = 4;
  const serviceWidth = 172;
  const timelineHeight = ((windowEnd - windowStart) / 60000) * pxPerMinute;
  const visibleServices = useMemo(() => (services.data ?? []).filter(isVisibleService), [services.data]);
  const serviceMap = useMemo(() => {
    return new Map(visibleServices.map((service) => [`${service.networkId}:${service.serviceId}`, service]));
  }, [visibleServices]);
  const programsByService = useMemo(() => {
    const grouped = new Map<string, Program[]>();
    for (const program of programs.data ?? []) {
      if (program.startAt + program.duration < windowStart || program.startAt > windowEnd) continue;
      const key = `${program.networkId}:${program.serviceId}`;
      grouped.set(key, [...(grouped.get(key) ?? []), program]);
    }
    for (const list of grouped.values()) {
      list.sort((a, b) => a.startAt - b.startAt || a.id - b.id);
    }
    return grouped;
  }, [programs.data, windowStart, windowEnd]);
  const epgColumns = useMemo(() => makeEpgColumns(visibleServices, programsByService), [visibleServices, programsByService]);
  const timelineWidth = epgColumns.length * serviceWidth;

  return (
    <PageFrame title="番組表" subtitle="現在時刻からの全局タイムラインです。">
      <ErrorList errors={[services.error, programs.error]} />
      <div className="epg-layout">
        <section className="timeline-shell">
          <div className="timeline-head">
            <div className="time-corner">時刻</div>
            <div className="station-head" style={{ width: timelineWidth }}>
              {epgColumns.map((column) => (
                <div
                  className={`station-cell ${column.foldedServices.length === 1 ? "subchannel" : ""}`}
                  key={column.key}
                  style={{ width: serviceWidth }}
                >
                  <Logo service={column.primaryService} />
                  <div>
                    <strong>{column.primaryService.name}</strong>
                    <span>
                      {column.primaryService.channel
                        ? `${column.primaryService.channel.type} ${column.primaryService.channel.channel}`
                        : `${column.primaryService.networkId}/${column.primaryService.serviceId}`}
                    </span>
                  </div>
                </div>
              ))}
            </div>
          </div>
          <div className="timeline-body" style={{ height: timelineHeight }}>
            <div className="time-rail">
              {Array.from({ length: 7 }).map((_, index) => {
                const time = windowStart + index * 60 * 60 * 1000;
                const showsDate = index > 0 && !isSameDate(time, time - 60 * 60 * 1000);
                return (
                  <span className={showsDate ? "day-boundary" : ""} key={time} style={{ top: index * 60 * pxPerMinute }}>
                    {showsDate && <em>{formatMonthDayWeekday(time)}</em>}
                    <strong>{formatHourOnly(time)}</strong>
                  </span>
                );
              })}
            </div>
            <div
              className="program-grid"
              style={{
                width: timelineWidth,
                height: timelineHeight,
                "--hour-height": `${60 * pxPerMinute}px`,
                "--service-width": `${serviceWidth}px`,
              } as React.CSSProperties}
            >
              <div className="now-line" style={{ top: ((now - windowStart) / 60000) * pxPerMinute }} />
              {epgColumns.map((column, index) => {
                const columnPrograms = makeEpgProgramBlocks(column, programsByService);
                return (
                  <div className="program-lane" key={column.key} style={{ left: index * serviceWidth, width: serviceWidth }}>
                    {columnPrograms.map(({ key, program, service }) => {
                      const visibleStart = Math.max(program.startAt, windowStart);
                      const visibleEnd = Math.min(program.startAt + program.duration, windowEnd);
                      const top = ((visibleStart - windowStart) / 60000) * pxPerMinute;
                      const height = Math.max(8, ((visibleEnd - visibleStart) / 60000) * pxPerMinute);
                      const isMicro = height < 22;
                      const isCompact = height < 34;
                      const isTight = height < 96;
                      return (
                        <button
                          className={[
                            "program-block",
                            programGenreClass(program),
                            isTight ? "tight" : "",
                            isCompact ? "compact" : "",
                            isMicro ? "micro" : "",
                            column.foldedServices.length === 1 ? "subchannel" : "",
                          ].filter(Boolean).join(" ")}
                          key={key}
                          style={{ top, height }}
                          onClick={() => setSelected(program)}
                          type="button"
                        >
                          <span>{formatMinute(program.startAt)}</span>
                          <strong>{program.name || "タイトルなし"}</strong>
                          {program.description && <small>{program.description}</small>}
                        </button>
                      );
                    })}
                  </div>
                );
              })}
            </div>
            {!services.loading && visibleServices.length === 0 && <Empty message="表示対象のテレビサービスがありません。" />}
          </div>
        </section>
        {selected && (
          <ProgramModal
            program={selected}
            service={serviceMap.get(`${selected.networkId}:${selected.serviceId}`)}
            onClose={() => setSelected(null)}
          />
        )}
      </div>
    </PageFrame>
  );
}
