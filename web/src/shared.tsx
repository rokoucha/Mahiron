import { useEffect, useRef, useState, type ReactNode, type UIEvent, type UIEventHandler } from "react";
import type { Channel, EventItem, Job, Program, Service, Tuner } from "./api";

type EpgColumn = {
  key: string;
  primaryService: Service;
  services: Service[];
  foldedServices: Service[];
};

type EpgProgramBlock = {
  key: string;
  program: Program;
  service: Service;
};

export function PageFrame({ title, subtitle, children }: { title: string; subtitle: string; children: ReactNode }) {
  return (
    <>
      <header className="page-header">
        <div>
          <h1>{title}</h1>
          <p>{subtitle}</p>
        </div>
      </header>
      {children}
    </>
  );
}

export function Panel({ title, action, children }: { title: string; action?: ReactNode; children: ReactNode }) {
  return (
    <section className="panel">
      <header>
        <h2>{title}</h2>
        {action}
      </header>
      {children}
    </section>
  );
}

export function Metric({ label, value, tone = "neutral" }: { label: string; value: string; tone?: "neutral" | "ok" | "warn" | "bad" }) {
  return (
    <section className={`metric ${tone}`}>
      <span>{label}</span>
      <strong>{value}</strong>
    </section>
  );
}

export function Definition({ label, value }: { label: string; value: string }) {
  return (
    <div className="definition">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

export function Logo({ service }: { service: Service }) {
  if (!service.hasLogoData) return <div className="logo-fallback">{service.name.slice(0, 2).toUpperCase()}</div>;
  return <img className="service-logo" src={`/api/services/${service.id}/logo`} alt="" loading="lazy" />;
}

export function ProgramModal({ program, service, onClose }: { program: Program; service?: Service; onClose: () => void }) {
  const genres = programGenreLabels(program);
  const extended = Object.entries(program.extended ?? {}).filter(([, value]) => value.trim() !== "");
  const audios = program.audios ?? [];
  const status = programStatus(program);

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [onClose]);

  return (
    <div className="modal-backdrop" onMouseDown={onClose} role="presentation">
      <aside className="program-modal" aria-modal="true" role="dialog" onMouseDown={(event) => event.stopPropagation()}>
        <header>
          <div>
            <span className="eyebrow">番組表 / {service?.name ?? `${program.networkId}/${program.serviceId}`}</span>
            <h2>{program.name || "タイトルなし"}</h2>
            <div className="program-meta-line">
              <span>{formatDate(program.startAt)} - {formatDate(program.startAt + program.duration)}</span>
              <span>{formatDuration(program.duration)}</span>
              <span>{status}</span>
            </div>
          </div>
          <button className="icon-button" onClick={onClose} type="button" aria-label="閉じる">x</button>
        </header>
        <section className="program-summary">
          <div className="summary-lead">
            <strong>概要</strong>
            <p>{program.description || "説明はありません。"}</p>
          </div>
          {extended.length > 0 && (
            <div className="extended-list">
              {extended.map(([label, value]) => (
                <div key={label}>
                  <strong>{label}</strong>
                  <p>{value}</p>
                </div>
              ))}
            </div>
          )}
        </section>
        {genres.length > 0 && (
          <section className="program-section">
            <h3>ジャンル</h3>
            <div className="tag-list">
              {genres.map((genre) => <span key={genre}>{genre}</span>)}
            </div>
          </section>
        )}
        <section className="program-section">
          <h3>放送情報</h3>
          <div className="definition-grid">
            <Definition label="サービス" value={service?.name ?? "-"} />
            <Definition label="チャンネル" value={service?.channel ? `${service.channel.type} ${service.channel.channel}` : "-"} />
            <Definition label="無料放送" value={program.isFree ? "はい" : "いいえ"} />
            <Definition label="映像" value={[program.video?.type, program.video?.resolution].filter(Boolean).join(" / ") || "-"} />
            <Definition label="番組ID" value={String(program.id)} />
            <Definition label="イベントID" value={formatCode(program.eventId)} />
            <Definition label="サービスID" value={formatCode(program.serviceId)} />
            <Definition label="ネットワークID" value={formatCode(program.networkId)} />
          </div>
        </section>
        {audios.length > 0 && (
          <section className="program-section">
            <h3>音声</h3>
            <div className="definition-grid">
              {audios.map((audio, index) => (
                <Definition
                  key={`${audio.componentTag ?? index}:${index}`}
                  label={audio.isMain ? `主音声 ${index + 1}` : `音声 ${index + 1}`}
                  value={audioLabel(audio)}
                />
              ))}
            </div>
          </section>
        )}
        {program.series && (
          <section className="program-section">
            <h3>シリーズ</h3>
            <div className="definition-grid">
              <Definition label="名前" value={program.series.name || "-"} />
              <Definition label="話数" value={program.series.episode ? `${program.series.episode}${program.series.lastEpisode ? ` / ${program.series.lastEpisode}` : ""}` : "-"} />
              <Definition label="再放送" value={formatNumber(program.series.repeat)} />
              <Definition label="パターン" value={formatNumber(program.series.pattern)} />
              <Definition label="有効期限" value={formatDate(program.series.expiresAt)} />
            </div>
          </section>
        )}
      </aside>
    </div>
  );
}

export function EventList({
  events,
  scrollRef,
  onScroll,
}: {
  events: EventItem[];
  scrollRef?: React.RefObject<HTMLDivElement | null>;
  onScroll?: UIEventHandler<HTMLDivElement>;
}) {
  if (events.length === 0) return <Empty message="イベントはありません。" />;
  return (
    <div className="event-list" onScroll={onScroll} ref={scrollRef}>
      {events.map((event, index) => (
        <div className="event-item" key={`${event.time}-${index}`}>
          <time>{formatDate(event.time)}</time>
          <strong>{event.resource} / {event.type}</strong>
          <code>{JSON.stringify(event.data)}</code>
        </div>
      ))}
    </div>
  );
}

export function StreamState({ connected }: { connected: boolean }) {
  return <span className={`stream-state ${connected ? "connected" : ""}`}>{connected ? "接続中" : "接続待ち"}</span>;
}

export function LogPanelActions({ connected, following, onFollow }: { connected: boolean; following: boolean; onFollow: () => void }) {
  return (
    <div className="log-panel-actions">
      <button className="follow-button" data-visible={!following} onClick={onFollow} tabIndex={following ? -1 : 0} type="button">
        下へ
      </button>
      <StreamState connected={connected} />
    </div>
  );
}

export function CopyRow({ label, value }: { label: string; value: string }) {
  const [copied, setCopied] = useState(false);
  async function copy() {
    await navigator.clipboard.writeText(value);
    setCopied(true);
    window.setTimeout(() => setCopied(false), 1400);
  }
  return (
    <div className="copy-row">
      <span>{label}</span>
      <code>{value}</code>
      <button onClick={copy} type="button">{copied ? "コピー済み" : "コピー"}</button>
    </div>
  );
}

export function StatusPill({ tuner }: { tuner: Tuner }) {
  const tone = tuner.isFault ? "bad" : tuner.isUsing ? "warn" : tuner.isAvailable && tuner.isFree ? "ok" : "neutral";
  const label = tuner.isFault ? "故障" : tuner.isUsing ? "使用中" : tuner.isFree ? "空き" : "ビジー";
  return <span className={`badge ${tone}`}>{label}</span>;
}

export function ErrorList({ errors }: { errors: Array<string | null> }) {
  const visible = errors.filter(Boolean);
  if (visible.length === 0) return null;
  return (
    <div className="error-list">
      {visible.map((error, index) => <span key={index}>{error}</span>)}
    </div>
  );
}

export function Empty({ message }: { message: string }) {
  return <div className="empty">{message}</div>;
}

export function useAutoScroll<T extends HTMLElement>(deps: unknown[]) {
  const ref = useRef<T | null>(null);
  const shouldFollowRef = useRef(true);
  const [following, setFollowing] = useState(true);

  function updateFollowing(element: T) {
    const distanceToBottom = element.scrollHeight - element.scrollTop - element.clientHeight;
    const nextFollowing = distanceToBottom < 48;
    shouldFollowRef.current = nextFollowing;
    setFollowing(nextFollowing);
  }

  function scrollToBottom() {
    const element = ref.current;
    if (!element) return;
    shouldFollowRef.current = true;
    setFollowing(true);
    element.scrollTop = element.scrollHeight;
  }

  function onScroll(event: UIEvent<T>) {
    updateFollowing(event.currentTarget);
  }

  useEffect(() => {
    const element = ref.current;
    if (!element) return;

    const onScroll = () => {
      updateFollowing(element);
    };

    onScroll();
    element.addEventListener("scroll", onScroll, { passive: true });
    return () => element.removeEventListener("scroll", onScroll);
  }, []);

  useEffect(() => {
    const element = ref.current;
    if (!element || !shouldFollowRef.current) return;
    element.scrollTop = element.scrollHeight;
  }, deps);

  return { ref, following, onScroll, scrollToBottom };
}

const genreLv1Names = [
  "ニュース／報道",
  "スポーツ",
  "情報／ワイドショー",
  "ドラマ",
  "音楽",
  "バラエティ",
  "映画",
  "アニメ／特撮",
  "ドキュメンタリー／教養",
  "劇場／公演",
  "趣味／教育",
  "福祉",
];

const genreLv2Names: Record<number, string[]> = {
  0: ["定時・総合", "天気", "特集・ドキュメント", "政治・国会", "経済・市況", "海外・国際", "解説", "討論・会談", "報道特番", "ローカル・地域", "交通"],
  1: ["スポーツニュース", "野球", "サッカー", "ゴルフ", "その他の球技", "相撲・格闘技", "オリンピック・国際大会", "マラソン・陸上・水泳", "モータースポーツ", "マリン・ウィンタースポーツ", "競馬・公営競技"],
  2: ["芸能・ワイドショー", "ファッション", "暮らし・住まい", "健康・医療", "ショッピング", "グルメ・料理", "イベント", "番組紹介・お知らせ"],
  3: ["国内ドラマ", "海外ドラマ", "時代劇"],
  4: ["国内ロック・ポップス", "海外ロック・ポップス", "クラシック・オペラ", "ジャズ・フュージョン", "歌謡曲・演歌", "ライブ・コンサート", "ランキング・リクエスト", "カラオケ・のど自慢", "民謡・邦楽", "童謡・キッズ", "民族音楽・ワールドミュージック"],
  5: ["クイズ", "ゲーム", "トークバラエティ", "お笑い・コメディ", "音楽バラエティ", "旅バラエティ", "料理バラエティ"],
  6: ["洋画", "邦画", "アニメ", "その他"],
  7: ["国内アニメ", "海外アニメ", "特撮"],
  8: ["社会・時事", "歴史・紀行", "自然・動物・環境", "宇宙・科学・医学", "カルチャー・伝統文化", "文学・文芸", "スポーツ", "ドキュメンタリー全般", "インタビュー・討論"],
  9: ["現代劇・新劇", "ミュージカル", "ダンス・バレエ", "落語・演芸", "歌舞伎・古典", "伝統芸能", "大衆演劇", "舞台"],
  10: ["旅・釣り・アウトドア", "園芸・ペット・手芸", "音楽・美術・工芸", "囲碁・将棋", "麻雀・パチンコ", "車・オートバイ", "コンピュータ・TVゲーム", "会話・語学", "幼児・小学生", "中学生・高校生", "大学生・受験", "生涯教育・資格", "教育問題"],
  11: ["高齢者", "障害者", "社会福祉", "ボランティア", "手話", "文字放送", "音声解説"],
};

export function formatNumber(value?: number) {
  return value == null ? "-" : new Intl.NumberFormat().format(value);
}

export function formatBytes(value?: number) {
  if (value == null) return "-";
  const units = ["B", "KiB", "MiB", "GiB"];
  let amount = value;
  let index = 0;
  while (amount >= 1024 && index < units.length - 1) {
    amount /= 1024;
    index += 1;
  }
  return `${amount.toFixed(index === 0 ? 0 : 1)} ${units[index]}`;
}

export function formatDate(value?: number) {
  if (value == null) return "-";
  return new Intl.DateTimeFormat(undefined, {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  }).format(new Date(value));
}

export function formatHourOnly(value: number) {
  return `${new Date(value).getHours().toString().padStart(2, "0")}時`;
}

export function formatMonthDayWeekday(value: number) {
  const date = new Date(value);
  const weekdays = ["日", "月", "火", "水", "木", "金", "土"];
  return `${date.getMonth() + 1}/${date.getDate()}(${weekdays[date.getDay()]})`;
}

export function formatMinute(value: number) {
  return new Date(value).getMinutes().toString().padStart(2, "0");
}

export function isSameDate(a: number, b: number) {
  const dateA = new Date(a);
  const dateB = new Date(b);
  return dateA.getFullYear() === dateB.getFullYear()
    && dateA.getMonth() === dateB.getMonth()
    && dateA.getDate() === dateB.getDate();
}

export function formatDuration(value?: number) {
  if (value == null) return "-";
  const minutes = Math.round(value / 60000);
  if (minutes < 60) return `${minutes}分`;
  const hours = Math.floor(minutes / 60);
  const rest = minutes % 60;
  return rest === 0 ? `${hours}時間` : `${hours}時間${rest}分`;
}

export function formatCode(value?: number) {
  if (value == null) return "-";
  return `${value} / 0x${value.toString(16).toUpperCase()}`;
}

export function programStatus(program: Program) {
  const now = Date.now();
  if (now < program.startAt) return "放送予定";
  if (now < program.startAt + program.duration) return "放送中";
  return "放送終了";
}

export function programGenreLabels(program: Program) {
  return (program.genres ?? []).map((genre) => {
    const lv1 = genre.lv1 ?? -1;
    const lv2 = genre.lv2 ?? -1;
    const main = genreLv1Names[lv1] ?? (lv1 === 15 ? "その他" : `ジャンル ${lv1}`);
    const sub = genreLv2Names[lv1]?.[lv2];
    return sub ? `${main} / ${sub}` : main;
  });
}

export function programGenreClass(program: Program) {
  const lv1 = program.genres?.[0]?.lv1;
  if (lv1 == null || lv1 < 0 || lv1 > 11) return "genre-other";
  return `genre-${lv1}`;
}

export function isVisibleService(service: Service) {
  return service.type === 0x01 || service.type === 0xad;
}

export function audioLabel(audio: NonNullable<Program["audios"]>[number]) {
  return [
    audio.langs?.join(", "),
    audio.componentType != null ? `コンポーネント ${audio.componentType}` : undefined,
    audio.componentTag != null ? `タグ ${audio.componentTag}` : undefined,
    audio.samplingRate != null ? `${audio.samplingRate} Hz` : undefined,
  ].filter(Boolean).join(" / ") || "-";
}

export function jobStatusLabel(status: Job["status"]) {
  const labels: Record<Job["status"], string> = {
    queued: "待機中",
    standby: "準備中",
    running: "実行中",
    finished: "完了",
  };
  return labels[status] ?? status;
}

export function currentGatheringNetworks(jobs: Job[]) {
  return jobs
    .filter((job) => job.status === "running" && job.key.startsWith("epg-gather:nid:"))
    .map((job) => job.key.slice("epg-gather:nid:".length))
    .filter(Boolean);
}

export function floorHour(value: number) {
  const date = new Date(value);
  date.setMinutes(0, 0, 0);
  return date.getTime();
}

export function makeEpgColumns(services: Service[], programsByService: Map<string, Program[]>): EpgColumn[] {
  const groups = new Map<string, Service[]>();
  for (const service of services) {
    const key = service.channel ? channelKey(service.channel) : serviceKey(service);
    groups.set(key, [...(groups.get(key) ?? []), service]);
  }

  const columns: EpgColumn[] = [];
  for (const [groupKey, groupServices] of groups) {
    const foldedServices = [...groupServices];
    const seenContent = new Set<string>();

    groupServices.forEach((service, index) => {
      const programs = programsByService.get(`${service.networkId}:${service.serviceId}`) ?? [];
      const contentKeys = programs.map(programContentKey);
      const hasDistinctContent = contentKeys.some((key) => !seenContent.has(key));

      if (index === 0 || hasDistinctContent) {
        columns.push({
          key: index === 0 ? groupKey : `${groupKey}:service:${service.id}`,
          primaryService: service,
          services: [service],
          foldedServices: index === 0 ? foldedServices : [service],
        });
      }

      for (const key of contentKeys) {
        seenContent.add(key);
      }
    });
  }

  return columns;
}

export function makeEpgProgramBlocks(
  column: EpgColumn,
  programsByService: Map<string, Program[]>,
): EpgProgramBlock[] {
  const uniquePrograms = new Map<string, { program: Program; service: Service }>();
  for (const service of column.services) {
    const programs = programsByService.get(`${service.networkId}:${service.serviceId}`) ?? [];
    for (const program of programs) {
      const key = programContentKey(program);
      if (!uniquePrograms.has(key)) {
        uniquePrograms.set(key, { program, service });
      }
    }
  }

  return [...uniquePrograms.values()]
    .sort((a, b) => a.program.startAt - b.program.startAt || a.program.id - b.program.id)
    .map(({ program, service }) => ({
      key: `${service.id}:${program.id}`,
      program,
      service,
    }));
}

export function programContentKey(program: Program) {
  return [
    program.startAt,
    program.duration,
    normalizeProgramText(program.name),
    normalizeProgramText(program.description),
  ].join(":");
}

export function normalizeProgramText(value?: string) {
  return (value ?? "").replace(/\s+/g, " ").trim();
}

export function channelLabel(type?: string, channel?: string) {
  return type && channel ? `${type} ${channel}` : "-";
}

export function serviceKey(service: Pick<Service, "networkId" | "serviceId">) {
  return `service:${service.networkId}:${service.serviceId}`;
}

export function channelKey(channel?: Pick<Channel, "type" | "channel">) {
  if (!channel?.type || !channel.channel) return "";
  return `channel:${channel.type}:${channel.channel}`;
}

export function openServiceMap(tuners: Tuner[]) {
  const services = new Map<string, Array<Tuner["users"][number]>>();
  for (const tuner of tuners) {
    for (const user of tuner.users) {
      const networkId = user.streamSetting?.networkId;
      const serviceId = user.streamSetting?.serviceId;
      if (networkId != null && serviceId != null) {
        appendOpenUser(services, `service:${networkId}:${serviceId}`, user);
      }

      const userChannelKey = channelKey(user.streamSetting?.channel);
      if (userChannelKey) {
        appendOpenUser(services, userChannelKey, user);
      }

      const tunedChannelKey = channelKey({ type: tuner.currentChannelType ?? "", channel: tuner.currentChannel ?? "" });
      if (tunedChannelKey) {
        appendOpenUser(services, tunedChannelKey, user);
      }
    }
  }
  return services;
}

export function openServiceUsers(openServices: Map<string, Array<Tuner["users"][number]>>, service: Service) {
  const byService = openServices.get(serviceKey(service)) ?? [];
  const byChannel = openServices.get(channelKey(service.channel)) ?? [];
  return [...byService, ...byChannel];
}

export function appendOpenUser(
  services: Map<string, Array<Tuner["users"][number]>>,
  key: string,
  user: Tuner["users"][number],
) {
  services.set(key, [...(services.get(key) ?? []), user]);
}
