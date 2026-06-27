import { lazy, Suspense, useEffect, useState } from "react";
import { api } from "./api";
import { useAsync } from "./hooks";

type Page = "overview" | "epg" | "jobs" | "logs" | "integrations";

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

export default function App() {
  const [page, setPage] = useState<Page>(() => pageFromPath(window.location.pathname));
  const status = useAsync(api.status);

  useEffect(() => {
    const onPopState = () => setPage(pageFromPath(window.location.pathname));
    window.addEventListener("popstate", onPopState);
    return () => window.removeEventListener("popstate", onPopState);
  }, []);

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand">
          <div className="brand-mark">M</div>
          <div>
            <strong>Mahiron</strong>
            <span>{status.data?.version ? `v${status.data.version}` : "v-"}</span>
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
          {page === "overview" && <Overview />}
          {page === "epg" && <EPG />}
          {page === "jobs" && <Jobs />}
          {page === "logs" && <Logs />}
          {page === "integrations" && <Integrations />}
        </Suspense>
      </main>
    </div>
  );
}
