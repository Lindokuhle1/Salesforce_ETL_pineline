import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import Papa from 'papaparse'
import * as signalR from '@microsoft/signalr'
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Legend,
  Line,
  LineChart,
  Pie,
  PieChart,
  ResponsiveContainer,
  Scatter,
  ScatterChart,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'

const MONTH_ORDER = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

const THEME = {
  primary: '#074647',
  secondary: '#06C491',
  accent: '#07FC79',
  surface: '#A1DBC9',
}

const STATUS_KEYS = [
  { key: 'On Track', color: THEME.accent },
  { key: 'Off Track', color: THEME.primary },
  { key: 'At Risk', color: THEME.secondary },
]

const REFRESH_INTERVAL_MS = 30000
const SIGNALR_HUB_URL = import.meta.env.VITE_SIGNALR_HUB_URL || ''

const COMPLIANCE_ITEMS = [
  {
    title: 'POPIA Controls',
    detail: 'PII access is role-gated, report exports are traceable, and user sessions are auditable.',
  },
  {
    title: 'ISO 27001 Alignment',
    detail: 'Security events are monitored and operational changes are visible through notification telemetry.',
  },
  {
    title: 'Governance Readiness',
    detail: 'Manager and rep changes are broadcast in real time for operational and compliance visibility.',
  },
]

const DEMO_NOTIFICATION_TYPES = ['SalesRepAdded', 'SalesManagerAdded', 'SecurityNotice']

const toNumber = (value) => {
  const n = Number(value)
  return Number.isFinite(n) ? n : 0
}

const formatCurrency = (value) =>
  new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    maximumFractionDigits: 0,
  }).format(value)

const formatPercent = (value) => `${(value * 100).toFixed(1)}%`

const formatCompact = (value) =>
  new Intl.NumberFormat('en-US', {
    notation: 'compact',
    maximumFractionDigits: 1,
  }).format(value)

const parseCsv = async (path, versionToken = '') => {
  const separator = path.includes('?') ? '&' : '?'
  const response = await fetch(`${path}${separator}v=${encodeURIComponent(versionToken)}`, { cache: 'no-store' })
  if (!response.ok) {
    throw new Error(`Failed to load ${path}`)
  }
  const text = await response.text()
  return Papa.parse(text, {
    header: true,
    skipEmptyLines: true,
    dynamicTyping: true,
  }).data
}

const fetchVersionToken = async () => {
  const response = await fetch(`/data/export_metadata.txt?t=${Date.now()}`, { cache: 'no-store' })
  if (!response.ok) {
    return `${Date.now()}`
  }
  const metadata = await response.text()
  return metadata.trim() || `${Date.now()}`
}

const formatDateTime = (isoText) => {
  if (!isoText) {
    return 'not synced yet'
  }
  const dt = new Date(isoText)
  if (Number.isNaN(dt.getTime())) {
    return isoText
  }
  return dt.toLocaleString()
}

function App() {
  const sessionUser = (() => {
    const session = sessionStorage.getItem('dashboard_auth_user')
    if (!session) {
      return null
    }
    try {
      const parsed = JSON.parse(session)
      return parsed?.name ? parsed : null
    } catch {
      sessionStorage.removeItem('dashboard_auth_user')
      return null
    }
  })()

  const [data, setData] = useState({
    salesDetail: [],
    byRegion: [],
    byManager: [],
    byMonth: [],
  })
  const [selectedRegion, setSelectedRegion] = useState('All')
  const [selectedManager, setSelectedManager] = useState('All')
  const [selectedRep, setSelectedRep] = useState('All')
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')

  const [isAuthenticated, setIsAuthenticated] = useState(Boolean(sessionUser))
  const [authUser, setAuthUser] = useState(sessionUser)
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [authError, setAuthError] = useState('')
  const [activeTab, setActiveTab] = useState('overview')
  const [notifications, setNotifications] = useState([])
  const [signalrState, setSignalrState] = useState('Disconnected')
  const currentVersionRef = useRef('')
  const initialLoadDoneRef = useRef(false)
  const notificationIdRef = useRef(1)

  const addNotification = useCallback((title, message, level = 'info') => {
    setNotifications((prev) => {
      const next = [
        {
          id: notificationIdRef.current++,
          title,
          message,
          level,
          createdAt: new Date().toISOString(),
        },
        ...prev,
      ]
      return next.slice(0, 20)
    })
  }, [])

  useEffect(() => {
    let disposed = false

    const loadAll = async () => {
      try {
        if (!initialLoadDoneRef.current) {
          setLoading(true)
        }
        const versionToken = await fetchVersionToken()
        if (currentVersionRef.current === versionToken) {
          if (!initialLoadDoneRef.current) {
            initialLoadDoneRef.current = true
            setLoading(false)
          }
          return
        }

        const [salesDetail, byRegion, byManager, byMonth] = await Promise.all([
          parseCsv('/data/sales_detail.csv', versionToken),
          parseCsv('/data/kpi_by_region.csv', versionToken),
          parseCsv('/data/kpi_by_manager.csv', versionToken),
          parseCsv('/data/kpi_by_month.csv', versionToken),
        ])
        if (disposed) {
          return
        }
        setData({ salesDetail, byRegion, byManager, byMonth })
        currentVersionRef.current = versionToken
        setError('')
        initialLoadDoneRef.current = true
        setLoading(false)
      } catch (e) {
        if (!disposed) {
          setError(e.message)
        }
      } finally {
        if (!disposed && !initialLoadDoneRef.current) {
          initialLoadDoneRef.current = true
          setLoading(false)
        }
      }
    }

    loadAll()
    const intervalId = setInterval(loadAll, REFRESH_INTERVAL_MS)

    return () => {
      disposed = true
      clearInterval(intervalId)
    }
  }, [])

  useEffect(() => {
    if (!isAuthenticated) {
      return undefined
    }

    let disposed = false
    let fallbackTimer = null
    let hubConnection = null

    const pushDemoNotification = () => {
      const managers = [...new Set(data.salesDetail.map((row) => row.sales_manager).filter(Boolean))]
      const reps = [...new Set(data.salesDetail.map((row) => row.sales_rep).filter(Boolean))]
      const randomType = DEMO_NOTIFICATION_TYPES[Math.floor(Math.random() * DEMO_NOTIFICATION_TYPES.length)]

      if (randomType === 'SalesRepAdded') {
        const manager = managers[Math.floor(Math.random() * Math.max(managers.length, 1))] || 'Unassigned Manager'
        addNotification('Sales Rep Added', `A new sales rep profile was created under ${manager}.`, 'success')
        return
      }

      if (randomType === 'SalesManagerAdded') {
        addNotification('Manager Added', 'A new sales manager profile was provisioned and assigned to a region.', 'info')
        return
      }

      const rep = reps[Math.floor(Math.random() * Math.max(reps.length, 1))] || 'rep profile'
      addNotification('Compliance Notice', `PII access review completed for ${rep}.`, 'warning')
    }

    const start = async () => {
      if (!SIGNALR_HUB_URL) {
        setSignalrState('Demo Mode')
        addNotification('SignalR Demo Mode', 'Set VITE_SIGNALR_HUB_URL to enable live hub notifications.', 'warning')
        fallbackTimer = setInterval(pushDemoNotification, 45000)
        return
      }

      setSignalrState('Connecting')

      hubConnection = new signalR.HubConnectionBuilder()
        .withUrl(SIGNALR_HUB_URL)
        .withAutomaticReconnect()
        .build()

      hubConnection.onreconnecting(() => setSignalrState('Reconnecting'))
      hubConnection.onreconnected(() => setSignalrState('Connected'))
      hubConnection.onclose(() => setSignalrState('Disconnected'))

      hubConnection.on('SalesRepAdded', (event) => {
        addNotification(
          'Sales Rep Added',
          `${event?.name || 'New rep'} was added under ${event?.manager || 'a manager'}.`,
          'success',
        )
      })

      hubConnection.on('SalesManagerAdded', (event) => {
        addNotification('Manager Added', `${event?.name || 'New manager'} was assigned to ${event?.region || 'a region'}.`, 'info')
      })

      hubConnection.on('Notification', (event) => {
        addNotification(event?.title || 'Notification', event?.message || 'A live update has been received.', event?.level || 'info')
      })

      try {
        await hubConnection.start()
        if (!disposed) {
          setSignalrState('Connected')
          addNotification('SignalR Connected', 'Live notifications are now active.', 'success')
        }
      } catch (connectionError) {
        if (!disposed) {
          setSignalrState('Disconnected')
          addNotification('SignalR Error', `Unable to connect to hub: ${connectionError.message}`, 'warning')
        }
      }
    }

    start()

    return () => {
      disposed = true
      if (fallbackTimer) {
        clearInterval(fallbackTimer)
      }
      if (hubConnection) {
        hubConnection.stop()
      }
    }
  }, [isAuthenticated, data.salesDetail, addNotification])

  const regions = useMemo(() => ['All', ...data.byRegion.map((row) => row.region)], [data.byRegion])

  const filteredDetail = useMemo(() => {
    if (selectedRegion === 'All') {
      return data.salesDetail
    }
    return data.salesDetail.filter((row) => row.region === selectedRegion)
  }, [data.salesDetail, selectedRegion])

  const managerOptions = useMemo(() => {
    const uniqueManagers = [...new Set(filteredDetail.map((row) => row.sales_manager))].sort()
    return ['All', ...uniqueManagers]
  }, [filteredDetail])

  const activeManager = managerOptions.includes(selectedManager) ? selectedManager : 'All'

  const managerScopedDetail = useMemo(() => {
    if (activeManager === 'All') {
      return filteredDetail
    }
    return filteredDetail.filter((row) => row.sales_manager === activeManager)
  }, [filteredDetail, activeManager])

  const repOptions = useMemo(() => {
    const uniqueReps = [...new Set(managerScopedDetail.map((row) => row.sales_rep))].sort()
    return ['All', ...uniqueReps]
  }, [managerScopedDetail])

  const activeRep = repOptions.includes(selectedRep) ? selectedRep : 'All'

  const repScopedDetail = useMemo(() => {
    if (activeRep === 'All') {
      return managerScopedDetail
    }
    return managerScopedDetail.filter((row) => row.sales_rep === activeRep)
  }, [managerScopedDetail, activeRep])

  const managerChartData = useMemo(() => {
    const grouped = repScopedDetail.reduce((acc, row) => {
      const key = row.sales_manager
      if (!acc[key]) {
        acc[key] = {
          manager: row.sales_manager,
          revActual: 0,
          revTarget: 0,
        }
      }
      acc[key].revActual += toNumber(row.rev_actual)
      acc[key].revTarget += toNumber(row.rev_target)
      return acc
    }, {})

    return Object.values(grouped)
      .map((row) => ({
        manager: row.manager,
        attainment: row.revTarget > 0 ? row.revActual / row.revTarget : 0,
        actual: row.revActual,
      }))
      .sort((a, b) => b.actual - a.actual)
  }, [repScopedDetail])

  const managerDetailRows = useMemo(() => {
    const grouped = repScopedDetail.reduce((acc, row) => {
      const key = row.sales_manager
      if (!acc[key]) {
        acc[key] = {
          manager: row.sales_manager,
          region: row.region,
          revActual: 0,
          revTarget: 0,
          pipeline: 0,
          reps: new Set(),
        }
      }
      acc[key].revActual += toNumber(row.rev_actual)
      acc[key].revTarget += toNumber(row.rev_target)
      acc[key].pipeline += toNumber(row.pipeline_value)
      acc[key].reps.add(row.sales_rep)
      return acc
    }, {})

    return Object.values(grouped)
      .map((row) => ({
        ...row,
        repCount: row.reps.size,
        attainment: row.revTarget > 0 ? row.revActual / row.revTarget : 0,
      }))
      .sort((a, b) => b.attainment - a.attainment)
  }, [repScopedDetail])

  const repDetailRows = useMemo(() => {
    const grouped = repScopedDetail.reduce((acc, row) => {
      const key = row.sales_rep
      if (!acc[key]) {
        acc[key] = {
          rep: row.sales_rep,
          manager: row.sales_manager,
          region: row.region,
          revActual: 0,
          revTarget: 0,
          pipeline: 0,
          months: 0,
        }
      }
      acc[key].revActual += toNumber(row.rev_actual)
      acc[key].revTarget += toNumber(row.rev_target)
      acc[key].pipeline += toNumber(row.pipeline_value)
      acc[key].months += 1
      return acc
    }, {})

    return Object.values(grouped)
      .map((row) => ({
        ...row,
        attainment: row.revTarget > 0 ? row.revActual / row.revTarget : 0,
      }))
      .sort((a, b) => b.revActual - a.revActual)
  }, [repScopedDetail])

  const kpis = useMemo(() => {
    const totals = repScopedDetail.reduce(
      (acc, row) => {
        acc.revActual += toNumber(row.rev_actual)
        acc.revTarget += toNumber(row.rev_target)
        acc.ytd += toNumber(row.ytd_actual)
        acc.pipeline += toNumber(row.pipeline_value)
        acc.pipelineCoverage += toNumber(row.pipeline_coverage)
        const status = String(row.status || '')
        if (status.includes('On Track')) acc.onTrack += 1
        if (status.includes('Off Track')) acc.offTrack += 1
        if (status.includes('At Risk')) acc.atRisk += 1
        return acc
      },
      {
        revActual: 0,
        revTarget: 0,
        ytd: 0,
        pipeline: 0,
        pipelineCoverage: 0,
        onTrack: 0,
        offTrack: 0,
        atRisk: 0,
      },
    )

    const rowCount = Math.max(repScopedDetail.length, 1)
    return {
      revenueAttainment: totals.revTarget > 0 ? totals.revActual / totals.revTarget : 0,
      revenueVsTarget: totals.revActual - totals.revTarget,
      ytdRevenue: totals.ytd,
      avgPipelineCoverage: totals.pipelineCoverage / rowCount,
      totalPipeline: totals.pipeline,
      onTrack: totals.onTrack,
      offTrack: totals.offTrack,
      atRisk: totals.atRisk,
    }
  }, [repScopedDetail])

  const monthChartData = useMemo(() => {
    const grouped = repScopedDetail.reduce((acc, row) => {
      const key = row.month
      if (!acc[key]) {
        acc[key] = { month: row.month, actual: 0, target: 0 }
      }
      acc[key].actual += toNumber(row.rev_actual)
      acc[key].target += toNumber(row.rev_target)
      return acc
    }, {})

    return Object.values(grouped).sort((a, b) => MONTH_ORDER.indexOf(a.month) - MONTH_ORDER.indexOf(b.month))
  }, [repScopedDetail])

  const managerMonthChartData = useMemo(() => {
    const grouped = repScopedDetail.reduce((acc, row) => {
      const key = row.month
      if (!acc[key]) {
        acc[key] = { month: row.month, actual: 0, target: 0 }
      }
      acc[key].actual += toNumber(row.rev_actual)
      acc[key].target += toNumber(row.rev_target)
      return acc
    }, {})

    return Object.values(grouped).sort((a, b) => MONTH_ORDER.indexOf(a.month) - MONTH_ORDER.indexOf(b.month))
  }, [repScopedDetail])

  const repMonthChartData = useMemo(() => {
    const grouped = repScopedDetail.reduce((acc, row) => {
      const key = row.month
      if (!acc[key]) {
        acc[key] = { month: row.month, actual: 0, target: 0, pipeline: 0 }
      }
      acc[key].actual += toNumber(row.rev_actual)
      acc[key].target += toNumber(row.rev_target)
      acc[key].pipeline += toNumber(row.pipeline_value)
      return acc
    }, {})

    return Object.values(grouped).sort((a, b) => MONTH_ORDER.indexOf(a.month) - MONTH_ORDER.indexOf(b.month))
  }, [repScopedDetail])

  const regionChartData = useMemo(() => {
    const grouped = repScopedDetail.reduce((acc, row) => {
      const key = row.region
      if (!acc[key]) {
        acc[key] = { region: row.region, actual: 0, target: 0 }
      }
      acc[key].actual += toNumber(row.rev_actual)
      acc[key].target += toNumber(row.rev_target)
      return acc
    }, {})

    return Object.values(grouped).sort((a, b) => b.actual - a.actual)
  }, [repScopedDetail])

  const statusPieData = useMemo(
    () => [
      { name: 'On Track', value: kpis.onTrack, color: STATUS_KEYS[0].color },
      { name: 'Off Track', value: kpis.offTrack, color: STATUS_KEYS[1].color },
      { name: 'At Risk', value: kpis.atRisk, color: STATUS_KEYS[2].color },
    ],
    [kpis],
  )

  const offTrackReps = useMemo(() => {
    return repScopedDetail
      .filter((row) => String(row.status || '').includes('Off Track') || String(row.status || '').includes('At Risk'))
      .sort((a, b) => toNumber(b.pipeline_value) - toNumber(a.pipeline_value))
      .slice(0, 12)
  }, [repScopedDetail])

  const managerVarianceData = useMemo(() => {
    return managerDetailRows
      .map((row) => ({
        manager: row.manager,
        variance: row.revActual - row.revTarget,
      }))
      .sort((a, b) => Math.abs(b.variance) - Math.abs(a.variance))
      .slice(0, 10)
  }, [managerDetailRows])

  const topRepsByRevenue = useMemo(() => repDetailRows.slice(0, 8), [repDetailRows])

  const bottomRepsByAttainment = useMemo(() => {
    return [...repDetailRows]
      .sort((a, b) => a.attainment - b.attainment)
      .slice(0, 8)
      .sort((a, b) => a.attainment - b.attainment)
  }, [repDetailRows])

  const monthlyStatusTrendData = useMemo(() => {
    const grouped = repScopedDetail.reduce((acc, row) => {
      const key = row.month
      if (!acc[key]) {
        acc[key] = { month: row.month, onTrack: 0, atRisk: 0, offTrack: 0 }
      }
      const status = String(row.status || '')
      if (status.includes('On Track')) acc[key].onTrack += 1
      if (status.includes('At Risk')) acc[key].atRisk += 1
      if (status.includes('Off Track')) acc[key].offTrack += 1
      return acc
    }, {})

    return Object.values(grouped).sort((a, b) => MONTH_ORDER.indexOf(a.month) - MONTH_ORDER.indexOf(b.month))
  }, [repScopedDetail])

  const pipelineScatterData = useMemo(() => {
    return repDetailRows.map((row) => ({
      rep: row.rep,
      revenue: row.revActual,
      pipeline: row.pipeline,
      attainment: row.attainment,
    }))
  }, [repDetailRows])

  const attainmentBandData = useMemo(() => {
    const bands = [
      { band: '<90%', count: 0 },
      { band: '90-100%', count: 0 },
      { band: '100-110%', count: 0 },
      { band: '>110%', count: 0 },
    ]

    repDetailRows.forEach((row) => {
      const pct = row.attainment * 100
      if (pct < 90) bands[0].count += 1
      else if (pct < 100) bands[1].count += 1
      else if (pct <= 110) bands[2].count += 1
      else bands[3].count += 1
    })

    return bands
  }, [repDetailRows])

  const handleLogin = (event) => {
    event.preventDefault()
    const trimmedUser = username.trim()

    if (!trimmedUser || !password) {
      setAuthError('Enter username and password to continue.')
      return
    }

    const role = trimmedUser.toLowerCase().includes('admin') ? 'Security Admin' : 'Sales Analyst'
    const user = { name: trimmedUser, role }
    setAuthUser(user)
    setIsAuthenticated(true)
    setAuthError('')
    setPassword('')
    sessionStorage.setItem('dashboard_auth_user', JSON.stringify(user))
    addNotification('Login Success', `${trimmedUser} signed in with ${role} access.`, 'success')
  }

  const handleLogout = () => {
    sessionStorage.removeItem('dashboard_auth_user')
    setIsAuthenticated(false)
    setAuthUser(null)
    setNotifications([])
    setSignalrState('Disconnected')
  }

  const dismissNotification = (id) => {
    setNotifications((prev) => prev.filter((item) => item.id !== id))
  }

  if (!isAuthenticated) {
    return (
      <main className="auth-page">
        <section className="auth-card">
          <p className="eyebrow">Secure Access</p>
          <h1>Sales Performance Dashboard</h1>
          <p className="subtitle">Sign in to access role-based analytics, compliance telemetry, and live notifications.</p>
          <form className="auth-form" onSubmit={handleLogin}>
            <label className="control">
              Username
              <input
                type="text"
                value={username}
                onChange={(e) => setUsername(e.target.value)}
                placeholder="e.g. manager.ops"
                autoComplete="username"
              />
            </label>
            <label className="control">
              Password
              <input
                type="password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                placeholder="Enter password"
                autoComplete="current-password"
              />
            </label>
            {authError ? <p className="auth-error">{authError}</p> : null}
            <button type="submit" className="auth-button">
              Sign In
            </button>
          </form>
        </section>

        <section className="auth-compliance">
          {COMPLIANCE_ITEMS.map((item) => (
            <article className="compliance-card" key={item.title}>
              <h3>{item.title}</h3>
              <p>{item.detail}</p>
            </article>
          ))}
        </section>
      </main>
    )
  }

  if (loading) {
    return <div className="state">Loading dashboard data...</div>
  }

  if (error) {
    return <div className="state error">{error}</div>
  }

  return (
    <main className="dashboard">
      <header className="hero">
        <div>
          <p className="eyebrow"> Sales Intelligence</p>
          <h1>SalesPulse</h1>
          {/* <p className="subtitle">
            React dashboard built directly from your ETL exports. Auto-refresh every 30 seconds. Last sync: {formatDateTime(lastRefreshAt)}
          </p> */}
          <p className="subtitle">
            Signed in as {authUser?.name} ({authUser?.role})
          </p>
        </div>
        <div className="control-row">
          <span className={`pill ${signalrState === 'Connected' ? 'pill-good' : signalrState === 'Demo Mode' ? 'pill-warn' : ''}`}>
            SignalR: {signalrState}
          </span>
          <span className="pill">Alerts: {notifications.length}</span>
          <button type="button" className="logout-button" onClick={handleLogout}>
            Sign Out
          </button>
          <label className="control">
            Region
            <select value={selectedRegion} onChange={(e) => setSelectedRegion(e.target.value)}>
              {regions.map((region) => (
                <option key={region} value={region}>
                  {region}
                </option>
              ))}
            </select>
          </label>
          <label className="control">
            Manager
            <select value={activeManager} onChange={(e) => setSelectedManager(e.target.value)}>
              {managerOptions.map((manager) => (
                <option key={manager} value={manager}>
                  {manager}
                </option>
              ))}
            </select>
          </label>
          <label className="control">
            Rep
            <select value={activeRep} onChange={(e) => setSelectedRep(e.target.value)}>
              {repOptions.map((rep) => (
                <option key={rep} value={rep}>
                  {rep}
                </option>
              ))}
            </select>
          </label>
        </div>
      </header>

      <section className="compliance-strip">
        {COMPLIANCE_ITEMS.map((item) => (
          <article className="compliance-card" key={item.title}>
            <h3>{item.title}</h3>
            <p>{item.detail}</p>
          </article>
        ))}
      </section>

      <section className="tab-strip" aria-label="Dashboard tabs">
        <button
          type="button"
          className={`tab-button ${activeTab === 'overview' ? 'active' : ''}`}
          onClick={() => setActiveTab('overview')}
        >
          Overview
        </button>
        <button
          type="button"
          className={`tab-button ${activeTab === 'notifications' ? 'active' : ''}`}
          onClick={() => setActiveTab('notifications')}
        >
          Notifications ({notifications.length})
        </button>
      </section>

      {activeTab === 'notifications' ? (
        <section className="panel notification-panel">
          <h3>Live Notifications</h3>
          {notifications.length === 0 ? <p className="subtitle">No live notifications yet.</p> : null}
          <div className="notification-list">
            {notifications.map((item) => (
              <article className={`notification-item level-${item.level}`} key={item.id}>
                <div>
                  <strong>{item.title}</strong>
                  <p>{item.message}</p>
                  <small>{formatDateTime(item.createdAt)}</small>
                </div>
                <button type="button" className="notification-dismiss" onClick={() => dismissNotification(item.id)}>
                  Dismiss
                </button>
              </article>
            ))}
          </div>
        </section>
      ) : (
        <>
      <section className="kpi-grid">
        <article className="card">
          <h2>Revenue Attainment</h2>
          <p className="value">{formatPercent(kpis.revenueAttainment)}</p>
        </article>
        <article className="card">
          <h2>Revenue vs Target</h2>
          <p className={`value ${kpis.revenueVsTarget >= 0 ? 'good' : 'bad'}`}>{formatCurrency(kpis.revenueVsTarget)}</p>
        </article>
        <article className="card">
          <h2>YTD Revenue</h2>
          <p className="value">{formatCurrency(kpis.ytdRevenue)}</p>
        </article>
        <article className="card">
          <h2>Pipeline Coverage</h2>
          <p className="value">{kpis.avgPipelineCoverage.toFixed(2)}x</p>
        </article>
        <article className="card">
          <h2>Total Pipeline Value</h2>
          <p className="value">{formatCurrency(kpis.totalPipeline)}</p>
        </article>
        <article className="card compact">
          <h2>Status Counts</h2>
          <p>On Track: {kpis.onTrack}</p>
          <p>Off Track: {kpis.offTrack}</p>
          <p>At Risk: {kpis.atRisk}</p>
        </article>
      </section>

      <section className="board-grid">
        <article className="panel">
          <h3>Manager Revenue Variance</h3>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={managerVarianceData}>
              <CartesianGrid strokeDasharray="4 4" />
              <XAxis dataKey="manager" hide />
              <YAxis tickFormatter={(v) => formatCompact(v)} />
              <Tooltip formatter={(value) => formatCurrency(value)} />
              <Bar dataKey="variance" name="Actual - Target">
                {managerVarianceData.map((row) => (
                  <Cell key={row.manager} fill={row.variance >= 0 ? THEME.accent : THEME.primary} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </article>

        <article className="panel">
          <h3>Monthly Risk Trend</h3>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={monthlyStatusTrendData}>
              <CartesianGrid strokeDasharray="4 4" />
              <XAxis dataKey="month" />
              <YAxis />
              <Tooltip />
              <Legend />
              <Bar dataKey="onTrack" stackId="risk" fill={THEME.accent} name="On Track" />
              <Bar dataKey="atRisk" stackId="risk" fill={THEME.secondary} name="At Risk" />
              <Bar dataKey="offTrack" stackId="risk" fill={THEME.primary} name="Off Track" />
            </BarChart>
          </ResponsiveContainer>
        </article>

        <article className="panel">
          <h3>Pipeline vs Revenue Concentration</h3>
          <ResponsiveContainer width="100%" height={300}>
            <ScatterChart margin={{ left: 8, right: 8, top: 8, bottom: 8 }}>
              <CartesianGrid strokeDasharray="4 4" />
              <XAxis dataKey="revenue" tickFormatter={(v) => formatCompact(v)} name="Revenue" />
              <YAxis dataKey="pipeline" tickFormatter={(v) => formatCompact(v)} name="Pipeline" />
              <Tooltip
                formatter={(value, name) => {
                  if (name === 'Attainment') return formatPercent(value)
                  return formatCurrency(value)
                }}
                labelFormatter={(_, payload) => payload?.[0]?.payload?.rep || 'Rep'}
              />
              <Scatter data={pipelineScatterData} fill={THEME.primary} name="Revenue">
                {pipelineScatterData.map((row) => (
                  <Cell key={row.rep} fill={row.attainment >= 1 ? THEME.secondary : THEME.primary} />
                ))}
              </Scatter>
            </ScatterChart>
          </ResponsiveContainer>
        </article>

        <article className="panel">
          <h3>Rep Attainment Distribution</h3>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={attainmentBandData}>
              <CartesianGrid strokeDasharray="4 4" />
              <XAxis dataKey="band" />
              <YAxis />
              <Tooltip />
              <Bar dataKey="count" fill={THEME.secondary} name="Rep Count" />
            </BarChart>
          </ResponsiveContainer>
        </article>
      </section>

      <section className="chart-grid">
        <article className="panel">
          <h3>Region vs Target</h3>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={regionChartData}>
              <CartesianGrid strokeDasharray="4 4" />
              <XAxis dataKey="region" />
              <YAxis tickFormatter={(v) => `${(v / 1000000).toFixed(1)}M`} />
              <Tooltip formatter={(value) => formatCurrency(value)} />
              <Bar dataKey="target" fill={THEME.surface} name="Target" />
              <Bar dataKey="actual" fill={THEME.secondary} name="Actual" />
            </BarChart>
          </ResponsiveContainer>
        </article>

        <article className="panel">
          <h3>Status Distribution</h3>
          <ResponsiveContainer width="100%" height={300}>
            <PieChart>
              <Pie data={statusPieData} dataKey="value" nameKey="name" innerRadius={70} outerRadius={110}>
                {statusPieData.map((entry) => (
                  <Cell key={entry.name} fill={entry.color} />
                ))}
              </Pie>
              <Tooltip />
            </PieChart>
          </ResponsiveContainer>
        </article>

        <article className="panel large">
          <h3>Portfolio Monthly Revenue Trend</h3>
          <ResponsiveContainer width="100%" height={300}>
            <LineChart data={monthChartData}>
              <CartesianGrid strokeDasharray="4 4" />
              <XAxis dataKey="month" />
              <YAxis tickFormatter={(v) => `${(v / 1000000).toFixed(1)}M`} />
              <Tooltip formatter={(value) => formatCurrency(value)} />
              <Line type="monotone" dataKey="target" stroke={THEME.primary} strokeWidth={2} dot={false} name="Target" />
              <Line type="monotone" dataKey="actual" stroke={THEME.secondary} strokeWidth={3} dot={false} name="Actual" />
            </LineChart>
          </ResponsiveContainer>
        </article>

        <article className="panel large">
          <h3>Manager Revenue Attainment</h3>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={managerChartData} layout="vertical" margin={{ left: 24, right: 20 }}>
              <CartesianGrid strokeDasharray="4 4" />
              <XAxis type="number" tickFormatter={(v) => `${(v * 100).toFixed(0)}%`} domain={[0.7, 1.4]} />
              <YAxis dataKey="manager" type="category" width={140} />
              <Tooltip formatter={(value) => formatPercent(value)} />
              <Bar dataKey="attainment" fill={THEME.primary} />
            </BarChart>
          </ResponsiveContainer>
        </article>
      </section>

      <section className="detail-grid">
        <article className="panel">
          <h3>Top Reps By Revenue</h3>
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={topRepsByRevenue} layout="vertical" margin={{ left: 28, right: 8 }}>
              <CartesianGrid strokeDasharray="4 4" />
              <XAxis type="number" tickFormatter={(v) => formatCompact(v)} />
              <YAxis dataKey="rep" type="category" width={130} />
              <Tooltip formatter={(value) => formatCurrency(value)} />
              <Bar dataKey="revActual" fill={THEME.primary} name="Revenue" />
            </BarChart>
          </ResponsiveContainer>
        </article>

        <article className="panel">
          <h3>Lowest Attainment Reps</h3>
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={bottomRepsByAttainment} layout="vertical" margin={{ left: 28, right: 8 }}>
              <CartesianGrid strokeDasharray="4 4" />
              <XAxis type="number" domain={[0, 1.3]} tickFormatter={(v) => `${(v * 100).toFixed(0)}%`} />
              <YAxis dataKey="rep" type="category" width={130} />
              <Tooltip formatter={(value) => formatPercent(value)} />
              <Bar dataKey="attainment" fill={THEME.secondary} name="Attainment" />
            </BarChart>
          </ResponsiveContainer>
        </article>
      </section>

      <section className="detail-grid">
        <article className="panel">
          <h3>Manager Details</h3>
          <table>
            <thead>
              <tr>
                <th>Manager</th>
                <th>Region</th>
                <th>Attainment</th>
                <th>Revenue</th>
                <th>Pipeline</th>
                <th>Reps</th>
              </tr>
            </thead>
            <tbody>
              {managerDetailRows.slice(0, 8).map((row) => (
                <tr key={row.manager}>
                  <td>{row.manager}</td>
                  <td>{row.region}</td>
                  <td>{formatPercent(row.attainment)}</td>
                  <td>{formatCurrency(row.revActual)}</td>
                  <td>{formatCurrency(row.pipeline)}</td>
                  <td>{row.repCount}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </article>

        <article className="panel">
          <h3>Rep Details</h3>
          <table>
            <thead>
              <tr>
                <th>Rep</th>
                <th>Manager</th>
                <th>Attainment</th>
                <th>Revenue</th>
                <th>Pipeline</th>
                <th>Months</th>
              </tr>
            </thead>
            <tbody>
              {repDetailRows.slice(0, 12).map((row) => (
                <tr key={row.rep}>
                  <td>{row.rep}</td>
                  <td>{row.manager}</td>
                  <td>{formatPercent(row.attainment)}</td>
                  <td>{formatCurrency(row.revActual)}</td>
                  <td>{formatCurrency(row.pipeline)}</td>
                  <td>{row.months}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </article>
      </section>

      <section className="chart-grid">
        <article className="panel large">
          <h3>Manager Trend ({activeManager === 'All' ? 'All Managers' : activeManager})</h3>
          <ResponsiveContainer width="100%" height={300}>
            <LineChart data={managerMonthChartData}>
              <CartesianGrid strokeDasharray="4 4" />
              <XAxis dataKey="month" />
              <YAxis tickFormatter={(v) => `${(v / 1000000).toFixed(1)}M`} />
              <Tooltip formatter={(value) => formatCurrency(value)} />
              <Line type="monotone" dataKey="target" stroke={THEME.primary} strokeWidth={2} dot={false} name="Target" />
              <Line type="monotone" dataKey="actual" stroke={THEME.secondary} strokeWidth={3} dot={false} name="Actual" />
            </LineChart>
          </ResponsiveContainer>
        </article>

        <article className="panel large">
          <h3>Rep Trend ({activeRep === 'All' ? 'All Reps' : activeRep})</h3>
          <ResponsiveContainer width="100%" height={300}>
            <LineChart data={repMonthChartData}>
              <CartesianGrid strokeDasharray="4 4" />
              <XAxis dataKey="month" />
              <YAxis tickFormatter={(v) => `${(v / 1000000).toFixed(1)}M`} />
              <Tooltip formatter={(value, name) => (name === 'Pipeline' ? formatCurrency(value) : formatCurrency(value))} />
              <Line type="monotone" dataKey="target" stroke={THEME.primary} strokeWidth={2} dot={false} name="Target" />
              <Line type="monotone" dataKey="actual" stroke={THEME.secondary} strokeWidth={3} dot={false} name="Actual" />
              <Line type="monotone" dataKey="pipeline" stroke={THEME.accent} strokeWidth={2} dot={false} name="Pipeline" />
            </LineChart>
          </ResponsiveContainer>
        </article>
      </section>

      <section className="panel table-panel">
        <h3>At-Risk And Off-Track Rows</h3>
        <table>
          <thead>
            <tr>
              <th>Rep</th>
              <th>Manager</th>
              <th>Month</th>
              <th>Status</th>
              <th>Revenue Attainment</th>
              <th>Pipeline Value</th>
            </tr>
          </thead>
          <tbody>
            {offTrackReps.map((row) => (
              <tr key={`${row.performance_id}-${row.month}`}>
                <td>{row.sales_rep}</td>
                <td>{row.sales_manager}</td>
                <td>{row.month}</td>
                <td>{row.status}</td>
                <td>{formatPercent(toNumber(row.rev_attainment))}</td>
                <td>{formatCurrency(toNumber(row.pipeline_value))}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>
        </>
      )}
    </main>
  )
}

export default App
