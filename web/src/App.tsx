import { lazy, Suspense, useEffect, useState } from 'react'
import { useDashboard, type DashboardState } from './dashboard'
import activeIconUrl from './assets/brand/icon-active.svg?url'
import grayIconUrl from './assets/brand/icon-gray.svg?url'
import iconUrl from './assets/brand/icon.svg?url'

type Page = 'overview' | 'epg' | 'jobs' | 'logs' | 'integrations'
type BrandState = 'normal' | 'active' | 'gray'

const Overview = lazy(() => import('./pages/Overview'))
const EPG = lazy(() => import('./pages/EPG'))
const Jobs = lazy(() => import('./pages/Jobs'))
const Logs = lazy(() => import('./pages/Logs'))
const Integrations = lazy(() => import('./pages/Integrations'))

const pages: Array<{ id: Page; label: string; path: string }> = [
  { id: 'overview', label: '概要', path: '/' },
  { id: 'epg', label: '番組表', path: '/epg' },
  { id: 'jobs', label: 'ジョブ', path: '/jobs' },
  { id: 'logs', label: 'ログ', path: '/logs' },
  { id: 'integrations', label: '連携', path: '/integrations' },
]

function pageFromPath(pathname: string): Page {
  return pages.find((page) => page.path === pathname)?.id ?? 'overview'
}

function navigate(path: string) {
  window.history.pushState({}, '', path)
  window.dispatchEvent(new PopStateEvent('popstate'))
}

function brandStateIcon(state: BrandState) {
  if (state === 'active') return activeIconUrl
  if (state === 'gray') return grayIconUrl
  return iconUrl
}

function setFavicon(href: string) {
  const currentLink =
    document.querySelector<HTMLLinkElement>('link[rel="icon"]')
  const nextLink = document.createElement('link')
  nextLink.rel = 'icon'
  nextLink.type = 'image/svg+xml'
  nextLink.sizes = 'any'
  nextLink.href = href

  // Safari does not reliably repaint a favicon when only its href changes.
  // Replacing the element also makes the initial icon available before React
  // starts, while preserving the dashboard-state favicon updates.
  if (currentLink) {
    currentLink.replaceWith(nextLink)
  } else {
    document.head.appendChild(nextLink)
  }
}

function brandState(dashboard: DashboardState): BrandState {
  if (
    dashboard.streamState === 'disconnected' ||
    dashboard.status.error ||
    dashboard.tuners.error
  )
    return 'gray'
  return dashboard.tuners.data?.some((tuner) => tuner.isUsing)
    ? 'active'
    : 'normal'
}

export default function App() {
  const [page, setPage] = useState<Page>(() =>
    pageFromPath(window.location.pathname),
  )
  const dashboard = useDashboard({ loadPrograms: page === 'epg' })
  const currentBrandState = brandState(dashboard)
  const brandIconUrl = brandStateIcon(currentBrandState)

  useEffect(() => {
    const onPopState = () => setPage(pageFromPath(window.location.pathname))
    window.addEventListener('popstate', onPopState)
    return () => window.removeEventListener('popstate', onPopState)
  }, [])

  useEffect(() => {
    setFavicon(brandIconUrl)
  }, [brandIconUrl])

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand">
          <img className="brand-mark" src={brandIconUrl} alt="Mahiron" />
          <div>
            <strong>Mahiron</strong>
            <span>
              {dashboard.status.data?.version
                ? `v${dashboard.status.data.version}`
                : 'v-'}
            </span>
          </div>
        </div>
        <nav>
          {pages.map((item) => (
            <button
              key={item.id}
              className={item.id === page ? 'active' : ''}
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
          {page === 'overview' && <Overview dashboard={dashboard} />}
          {page === 'epg' && <EPG dashboard={dashboard} />}
          {page === 'jobs' && <Jobs dashboard={dashboard} />}
          {page === 'logs' && <Logs />}
          {page === 'integrations' && <Integrations dashboard={dashboard} />}
        </Suspense>
      </main>
    </div>
  )
}
