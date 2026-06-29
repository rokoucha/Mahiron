import { lazy, Suspense, useEffect, useState } from "react";
import { useDashboard, type DashboardState, type StreamConnectionState } from "./dashboard";
import activeIconUrl from "./assets/brand/icon-active.svg?url";
import grayIconUrl from "./assets/brand/icon-gray.svg?url";
import iconUrl from "./assets/brand/icon.svg?url";

type Page = "overview" | "epg" | "jobs" | "logs" | "integrations";
type BrandState = "normal" | "active" | "gray";

const Overview = lazy(() => import("./pages/Overview"));
const EPG = lazy(() => import("./pages/EPG"));
const Jobs = lazy(() => import("./pages/Jobs"));
const Logs = lazy(() => import("./pages/Logs"));
const Integrations = lazy(() => import("./pages/Integrations"));

const pages: Array<{ id: Page; label: string; path: string }> = [
  { id: "overview", label: "概要", path: "/" },
  { id: "epg", label: "番組表", path: "/epg" },
  { id: "jobs", label: "ジョブ", path: "/jobs" },
  { id: "logs", label: "ログ", path: "/logs" },
  { id: "integrations", label: "連携", path: "/integrations" },
];

function pageFromPath(pathname: string): Page {
  return pages.find((page) => page.path === pathname)?.id ?? "overview";
}

function navigate(path: string) {
  window.history.pushState({}, "", path);
  window.dispatchEvent(new PopStateEvent("popstate"));
}

function brandStateIcon(state: BrandState) {
  if (state === "active") return activeIconUrl;
  if (state === "gray") return grayIconUrl;
  return iconUrl;
}

function setFavicon(href: string) {
  let link = document.querySelector<HTMLLinkElement>('link[rel="icon"]');
  if (!link) {
    link = document.createElement("link");
    link.rel = "icon";
    document.head.appendChild(link);
  }
  link.type = "image/svg+xml";
  link.href = href;
}

function connectionLabel(state: StreamConnectionState) {
  if (state === "connected") return "events 接続中";
  if (state === "reconnecting") return "events 再接続中";
  return "events 切断";
}

function brandState(dashboard: DashboardState): BrandState {
  if (dashboard.streamState !== "connected" || dashboard.status.error || dashboard.tuners.error) return "gray";
  return dashboard.tuners.data?.some((tuner) => tuner.isUsing) ? "active" : "normal";
}

export default function App() {
  const [page, setPage] = useState<Page>(() => pageFromPath(window.location.pathname));
  const dashboard = useDashboard();
  const currentBrandState = brandState(dashboard);
  const brandIconUrl = brandStateIcon(currentBrandState);

  useEffect(() => {
    const onPopState = () => setPage(pageFromPath(window.location.pathname));
    window.addEventListener("popstate", onPopState);
    return () => window.removeEventListener("popstate", onPopState);
  }, []);

  useEffect(() => {
    setFavicon(brandIconUrl);
  }, [brandIconUrl]);

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand">
          <img className="brand-mark" src={brandIconUrl} alt="Mahiron" />
          <div>
            <strong>Mahiron</strong>
            <span>{dashboard.status.data?.version ? `v${dashboard.status.data.version}` : "v-"}</span>
            <small className={`brand-stream ${dashboard.streamState}`}>{connectionLabel(dashboard.streamState)}</small>
          </div>
        </div>
        <nav>
          {pages.map((item) => (
            <button
              key={item.id}
              className={item.id === page ? "active" : ""}
              onClick={() => navigate(item.path)}
              type="button"
            >
              {item.label}
            </button>
          ))}
        </nav>
      </aside>
      <main className="content">
        <Suspense fallback={<div className="empty">読み込み中...</div>}>
          {page === "overview" && <Overview dashboard={dashboard} />}
          {page === "epg" && <EPG dashboard={dashboard} />}
          {page === "jobs" && <Jobs dashboard={dashboard} />}
          {page === "logs" && <Logs />}
          {page === "integrations" && <Integrations dashboard={dashboard} />}
        </Suspense>
      </main>
    </div>
  );
}
