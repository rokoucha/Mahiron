import { useMemo } from "react";
import type { DashboardState } from "../dashboard";
import { CopyRow, isVisibleService, PageFrame } from "../shared";

export default function Integrations({ dashboard }: { dashboard: DashboardState }) {
  const { services } = dashboard;
  const visibleServices = useMemo(() => (services.data ?? []).filter(isVisibleService), [services.data]);
  const origin = window.location.origin;
  const links = [
    ["M3Uプレイリスト", `${origin}/api/iptv/playlist`],
    ["XMLTV", `${origin}/api/iptv/xmltv`],
    ["検出情報", `${origin}/api/iptv/discover.json`],
    ["ラインアップ", `${origin}/api/iptv/lineup.json`],
    ["ラインアップ状態", `${origin}/api/iptv/lineup_status.json`],
  ];
  return (
    <PageFrame title="連携" subtitle="IPTVクライアントや外部プレイヤー向けのURLです。">
      <section className="table-section">
        <h2>IPTV</h2>
        <div className="copy-list">
          {links.map(([label, value]) => (
            <CopyRow key={label} label={label} value={value} />
          ))}
        </div>
      </section>
      <section className="table-section">
        <h2>サービス別ストリーム</h2>
        <div className="copy-list">
          {visibleServices.map((service) => (
            <CopyRow key={service.id} label={service.name} value={`${origin}/api/services/${service.id}/stream?decode=1`} />
          ))}
        </div>
      </section>
    </PageFrame>
  );
}
