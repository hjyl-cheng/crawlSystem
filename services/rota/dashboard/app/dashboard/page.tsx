"use client"

import * as React from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import {
  Activity,
  CheckCircle2,
  Clock,
  TrendingUp,
  TrendingDown,
  Network,
} from "lucide-react"
import { Status, StatusIndicator, StatusLabel } from "@/components/ui/shadcn-io/status"
import { Area, AreaChart, CartesianGrid, XAxis, YAxis } from "recharts"
import {
  ChartConfig,
  ChartContainer,
  ChartTooltip,
  ChartTooltipContent,
} from "@/components/ui/chart"
import { api } from "@/lib/api"
import { DashboardStats, ChartDataPoint } from "@/lib/types"

const chartConfig = {
  value: {
    label: "Response Time",
    color: "hsl(var(--chart-1))",
  },
  success: {
    label: "Success",
    color: "hsl(var(--chart-2))",
  },
  failure: {
    label: "Failure",
    color: "hsl(var(--chart-3))",
  },
} satisfies ChartConfig

function finiteNumber(value: unknown): number {
  return typeof value === "number" && Number.isFinite(value) ? value : 0
}

export default function DashboardPage() {
  const [stats, setStats] = React.useState<DashboardStats | null>(null)
  const [responseTimeData, setResponseTimeData] = React.useState<ChartDataPoint[]>([])
  const [successRateData, setSuccessRateData] = React.useState<ChartDataPoint[]>([])
  const [isLoading, setIsLoading] = React.useState(true)

  React.useEffect(() => {
    let cancelled = false
    let ws: { close: () => void } | null = null

    const fetchData = async () => {
      try {
        const [statsData, responseData, successData] = await Promise.all([
          api.getDashboardStats(),
          api.getResponseTimeChart("4h"),
          api.getSuccessRateChart("4h"),
        ])

        if (!cancelled) {
          setStats(statsData)
          setResponseTimeData(responseData.data)
          setSuccessRateData(successData.data)
        }
      } catch (error) {
        if (!cancelled) console.error("Failed to fetch dashboard data:", error)
      } finally {
        if (!cancelled) setIsLoading(false)
      }
    }

    fetchData()

    // Setup WebSocket for real-time updates
    try {
      ws = api.createDashboardWebSocket((data) => {
        if (!cancelled) setStats(data)
      })
    } catch (error) {
      console.error("Failed to connect to WebSocket:", error)
    }

    return () => {
      cancelled = true
      ws?.close()
    }
  }, [])

  if (isLoading || !stats) {
    return (
      <div className="space-y-4">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-3xl font-bold tracking-tight">Dashboard</h1>
            <p className="text-muted-foreground">Loading...</p>
          </div>
        </div>
      </div>
    )
  }

  const activeProxies = finiteNumber(stats.active_proxies)
  const archivedProxies = finiteNumber(stats.archived_proxies)
  const observedProxies = Math.max(0, finiteNumber(stats.total_proxies) - archivedProxies)
  const operationalPercent = observedProxies > 0
    ? Math.round((activeProxies / observedProxies) * 100)
    : 0
  const totalRequests = finiteNumber(stats.total_requests)
  const requestGrowth = finiteNumber(stats.request_growth)
  const successRate = finiteNumber(stats.avg_success_rate)
  const successRateGrowth = finiteNumber(stats.success_rate_growth)
  const avgResponseTime = finiteNumber(stats.avg_response_time)
  const responseTimeDelta = finiteNumber(stats.response_time_delta)

  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Dashboard</h1>
          <p className="text-muted-foreground">
            Real-time monitoring and statistics
          </p>
        </div>
        <Status status="online">
          <StatusIndicator />
          <StatusLabel>Live</StatusLabel>
        </Status>
      </div>

      {/* Stats Cards */}
      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Active Proxies</CardTitle>
            <Network className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{activeProxies}/{observedProxies}</div>
            <p className="text-xs text-muted-foreground">
              {operationalPercent}% operational / {archivedProxies} archived
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Total Requests</CardTitle>
            <Activity className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold" suppressHydrationWarning>
              {totalRequests.toLocaleString("en-US")}
            </div>
            <p className="text-xs text-muted-foreground">
              {requestGrowth >= 0 ? (
                <TrendingUp className="inline h-3 w-3" />
              ) : (
                <TrendingDown className="inline h-3 w-3" />
              )}{" "}
              {requestGrowth >= 0 ? "+" : ""}{requestGrowth.toFixed(1)}% from last hour
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Success Rate</CardTitle>
            <CheckCircle2 className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{successRate.toFixed(1)}%</div>
            <p className="text-xs text-muted-foreground">
              {successRateGrowth >= 0 ? (
                <TrendingUp className="inline h-3 w-3" />
              ) : (
                <TrendingDown className="inline h-3 w-3" />
              )}{" "}
              {successRateGrowth >= 0 ? "+" : ""}{successRateGrowth.toFixed(1)}% from yesterday
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Avg Response Time</CardTitle>
            <Clock className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{avgResponseTime}ms</div>
            <p className="text-xs text-muted-foreground">
              {responseTimeDelta <= 0 ? (
                <TrendingDown className="inline h-3 w-3" />
              ) : (
                <TrendingUp className="inline h-3 w-3" />
              )}{" "}
              {responseTimeDelta <= 0 ? "" : "+"}{responseTimeDelta}ms from yesterday
            </p>
          </CardContent>
        </Card>
      </div>

      {/* Charts */}
      <div className="grid gap-4 md:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Response Time</CardTitle>
            <CardDescription>Average response time over the last 24 hours</CardDescription>
          </CardHeader>
          <CardContent>
            <ChartContainer config={chartConfig}>
              <AreaChart data={responseTimeData}>
                <CartesianGrid strokeDasharray="3 3" vertical={false} />
                <XAxis
                  dataKey="time"
                  tickLine={false}
                  axisLine={false}
                  tickMargin={8}
                />
                <YAxis
                  tickLine={false}
                  axisLine={false}
                  tickMargin={8}
                  tickFormatter={(value) => `${value}ms`}
                />
                <ChartTooltip content={<ChartTooltipContent />} />
                <Area
                  type="monotone"
                  dataKey="value"
                  stroke="var(--color-value)"
                  fill="var(--color-value)"
                  fillOpacity={0.2}
                />
              </AreaChart>
            </ChartContainer>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Success Rate</CardTitle>
            <CardDescription>Success vs failure rate over the last 24 hours</CardDescription>
          </CardHeader>
          <CardContent>
            <ChartContainer config={chartConfig}>
              <AreaChart data={successRateData}>
                <CartesianGrid strokeDasharray="3 3" vertical={false} />
                <XAxis
                  dataKey="time"
                  tickLine={false}
                  axisLine={false}
                  tickMargin={8}
                />
                <YAxis
                  tickLine={false}
                  axisLine={false}
                  tickMargin={8}
                  tickFormatter={(value) => `${value}%`}
                />
                <ChartTooltip content={<ChartTooltipContent />} />
                <Area
                  type="monotone"
                  dataKey="success"
                  stackId="1"
                  stroke="var(--color-success)"
                  fill="var(--color-success)"
                  fillOpacity={0.4}
                />
                <Area
                  type="monotone"
                  dataKey="failure"
                  stackId="1"
                  stroke="var(--color-failure)"
                  fill="var(--color-failure)"
                  fillOpacity={0.4}
                />
              </AreaChart>
            </ChartContainer>
          </CardContent>
        </Card>
      </div>

    </div>
  )
}
