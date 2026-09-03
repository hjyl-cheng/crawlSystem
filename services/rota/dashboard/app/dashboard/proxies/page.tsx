"use client"

import * as React from "react"
import {
  ColumnDef,
  ColumnFiltersState,
  RowSelectionState,
  SortingState,
  VisibilityState,
  flexRender,
  getCoreRowModel,
  useReactTable,
} from "@tanstack/react-table"
import {
  ArrowUpDown,
  ChevronDown,
  ChevronLeft,
  ChevronRight,
  MoreHorizontal,
  Plus,
  Activity,
  Download,
  Trash2,
  Loader2,
  Upload,
  FileText,
  CheckCircle2,
  XCircle,
  AlertCircle,
  Filter,
  Archive,
  RotateCcw,
  Tag,
} from "lucide-react"

import { Button } from "@/components/ui/button"
import { Checkbox } from "@/components/ui/checkbox"
import {
  DropdownMenu,
  DropdownMenuCheckboxItem,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
import { Input } from "@/components/ui/input"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { Badge } from "@/components/ui/badge"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog"
import { Label } from "@/components/ui/label"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Textarea } from "@/components/ui/textarea"
import { api } from "@/lib/api"
import { Proxy, ProxyCapacity } from "@/lib/types"
import { toast } from "@/lib/toast"
import { TagInput } from "@/components/tag-input"

type ImportedProxy = {
  raw: string
  address: string
  protocol?: Proxy["protocol"]
  username?: string
  password?: string
}

type OriginTag = "origin:paid" | "origin:free" | "origin:unknown"

const originTags: OriginTag[] = ["origin:paid", "origin:free", "origin:unknown"]

const proxyProtocols: Proxy["protocol"][] = [
  "http", "https", "socks4", "socks4a", "socks5",
  "vless", "vmess", "trojan", "shadowsocks",
]
const shareProtocols = new Set<Proxy["protocol"]>(["vless", "vmess", "trojan", "shadowsocks"])

function isShareProtocol(protocol: Proxy["protocol"]) {
  return shareProtocols.has(protocol)
}

function shareScheme(protocol: Proxy["protocol"]) {
  return protocol === "shadowsocks" ? "ss" : protocol
}

function isValidProxyPort(value: string) {
  if (!/^\d+$/.test(value)) return false

  const port = Number(value)
  return Number.isInteger(port) && port >= 1 && port <= 65535
}

function getExplicitProxyPort(value: string) {
  const authority = value
    .replace(/^[a-z0-9+.-]+:\/\//i, "")
    .split(/[/?#]/, 1)[0]
  const hostAndPort = authority.slice(authority.lastIndexOf("@") + 1)
  return hostAndPort.match(/:(\d+)$/)?.[1] || ""
}

function parseProxyUrl(raw: string, value: string, protocol?: Proxy["protocol"]): ImportedProxy | null {
  try {
    const url = new URL(value)
    const port = url.port || getExplicitProxyPort(value)
    if (!url.hostname || !port || !isValidProxyPort(port)) return null

    return {
      raw,
      address: `${url.hostname}:${port}`,
      protocol,
      username: url.username ? decodeURIComponent(url.username) : undefined,
      password: url.password ? decodeURIComponent(url.password) : undefined,
    }
  } catch {
    return null
  }
}

function parseProxyParts(raw: string, protocol?: Proxy["protocol"]): ImportedProxy | null {
  const parts = raw.split(":")
  if (parts.length !== 2 && parts.length < 4) return null

  const host = parts[0]?.trim()
  const port = parts[1]?.trim()
  if (!host || !port || !isValidProxyPort(port)) return null

  const imported: ImportedProxy = {
    raw,
    address: `${host}:${port}`,
    protocol,
  }

  if (parts.length >= 4) {
    const username = parts[2]?.trim()
    const password = parts.slice(3).join(":").trim()
    if (!username || !password) return null
    imported.username = username
    imported.password = password
  }

  return imported
}

function parseImportedProxyLine(line: string): ImportedProxy | null {
  const raw = line.trim()
  if (!raw || raw.startsWith("#")) return null

  const schemeMatch = raw.match(/^([a-z0-9+.-]+):\/\//i)
  if (schemeMatch) {
    const scheme = schemeMatch[1].toLowerCase()
    const protocol = (scheme === "ss" ? "shadowsocks" : scheme) as Proxy["protocol"]
    if (!proxyProtocols.includes(protocol)) return null
    if (isShareProtocol(protocol)) {
      const parsed = parseProxyUrl(raw, raw, protocol)
      return { raw, address: parsed?.address || `${protocol.toUpperCase()} node`, protocol }
    }

    return parseProxyUrl(raw, raw, protocol)
      || parseProxyParts(raw.slice(schemeMatch[0].length), protocol)
  }

  if (raw.includes("@") || raw.startsWith("[")) {
    return parseProxyUrl(raw, `http://${raw}`)
  }

  return parseProxyParts(raw)
}

function parseImportedProxyText(text: string) {
  const seen = new Set<string>()

  return text.split(/\r?\n/)
    .map(parseImportedProxyLine)
    .filter((proxy): proxy is ImportedProxy => {
      if (!proxy) return false

      const key = isShareProtocol(proxy.protocol || "http")
        ? proxy.raw
        : [proxy.protocol || "default", proxy.address].join("\u0000")
      if (seen.has(key)) return false
      seen.add(key)
      return true
    })
}

function formatImportedProxyPreview(proxy: ImportedProxy) {
  if (proxy.protocol && isShareProtocol(proxy.protocol)) {
    return `${shareScheme(proxy.protocol)}://[credential]`
  }
  const protocol = proxy.protocol ? `${proxy.protocol}://` : ""
  const credentials = proxy.username || proxy.password
    ? `${proxy.username || ""}:${proxy.password ? "********" : ""}@`
    : ""
  return `${protocol}${credentials}${proxy.address}`
}

function lifecycleLabel(status: Proxy["status"]) {
  if (status === "idle") return "Pending validation"
  return status.charAt(0).toUpperCase() + status.slice(1)
}

function failureLabel(kind: string | undefined, fallback: string) {
  switch (kind) {
    case "hard_unreachable": return "Hard unreachable"
    case "soft_unreachable": return "Soft unreachable"
    case "youtube_unusable": return "YouTube unavailable"
    case "manual": return "Manual archive"
    default: return kind?.replaceAll("_", " ") || fallback
  }
}

function formatLifecycleTime(value?: string) {
  if (!value) return null
  const parsed = new Date(value)
  return Number.isNaN(parsed.getTime()) ? value : parsed.toLocaleString()
}

function finiteCellNumber(value: unknown) {
  const parsed = typeof value === "number" ? value : Number(value)
  return Number.isFinite(parsed) ? parsed : null
}

function proxyOrigin(tags: string[] = []): OriginTag {
  return originTags.find(tag => tags.includes(tag)) || "origin:unknown"
}

function withProxyOrigin(tags: string[] = [], origin: OriginTag) {
  return [...tags.filter(tag => !tag.startsWith("origin:")), origin]
}

function withoutProxyOrigin(tags: string[] = []) {
  return tags.filter(tag => !tag.startsWith("origin:"))
}

function originLabel(origin: OriginTag) {
  return origin.slice("origin:".length).replace(/^./, value => value.toUpperCase())
}

export default function ProxiesPage() {
  const [data, setData] = React.useState<Proxy[]>([])
  const [capacity, setCapacity] = React.useState<ProxyCapacity | null>(null)
  const [isLoading, setIsLoading] = React.useState(true)
  const latestProxyRequestRef = React.useRef(0)
  const [isAddDialogOpen, setIsAddDialogOpen] = React.useState(false)
  const [isEditDialogOpen, setIsEditDialogOpen] = React.useState(false)
  const [isImportDialogOpen, setIsImportDialogOpen] = React.useState(false)
  const [editingProxy, setEditingProxy] = React.useState<Proxy | null>(null)
  const [sorting, setSorting] = React.useState<SortingState>([])
  const [columnFilters, setColumnFilters] = React.useState<ColumnFiltersState>([])
  const [columnVisibility, setColumnVisibility] = React.useState<VisibilityState>({})
  const [rowSelection, setRowSelection] = React.useState<RowSelectionState>({})
  const [pagination, setPagination] = React.useState({
    page: 1,
    limit: 10,
    total: 0,
    total_pages: 0,
  })
  const [searchQuery, setSearchQuery] = React.useState("")
  const [debouncedSearchQuery, setDebouncedSearchQuery] = React.useState("")
  const [statusFilter, setStatusFilter] = React.useState<string>("all")
  const [protocolFilter, setProtocolFilter] = React.useState<string>("all")
  const [originFilter, setOriginFilter] = React.useState<string>("all")
  const [pageInput, setPageInput] = React.useState("1")

  const [newProxy, setNewProxy] = React.useState({
    address: "",
    protocol: "http" as Proxy["protocol"],
    username: "",
    password: "",
    origin: "origin:unknown" as OriginTag,
    tags: [] as string[],
  })

  const [allTags, setAllTags] = React.useState<string[]>([])
  const [isTagDialogOpen, setIsTagDialogOpen] = React.useState(false)
  const [bulkAddTags, setBulkAddTags] = React.useState<string[]>([])
  const [bulkRemoveTags, setBulkRemoveTags] = React.useState<string[]>([])
  const [isTagging, setIsTagging] = React.useState(false)

  // Import modal states
  const [importFile, setImportFile] = React.useState<File | null>(null)
  const [importText, setImportText] = React.useState("")
  const [importProtocol, setImportProtocol] = React.useState<Proxy["protocol"]>("http")
  const [importUsername, setImportUsername] = React.useState("")
  const [importPassword, setImportPassword] = React.useState("")
  const [importOrigin, setImportOrigin] = React.useState<OriginTag>("origin:unknown")
  const [parsedProxies, setParsedProxies] = React.useState<ImportedProxy[]>([])
  const [isImporting, setIsImporting] = React.useState(false)
  const [importProgress, setImportProgress] = React.useState({ current: 0, total: 0, success: 0, failed: 0, skipped: 0 })
  const [importResults, setImportResults] = React.useState<Array<{ address: string; status: string; error?: string }>>([])
  const [isDragging, setIsDragging] = React.useState(false)
  const [isReloading, setIsReloading] = React.useState(false)
  const [isBulkTesting, setIsBulkTesting] = React.useState(false)
  const [isLifecycleUpdating, setIsLifecycleUpdating] = React.useState(false)
  const [bulkTestProgress, setBulkTestProgress] = React.useState({ current: 0, total: 0 })
  const [deleteConfirm, setDeleteConfirm] = React.useState<{ open: boolean; proxyId: number | null }>({ open: false, proxyId: null })
  const [bulkDeleteConfirm, setBulkDeleteConfirm] = React.useState(false)
  const [deleteAllConfirm, setDeleteAllConfirm] = React.useState(false)

  const selectedProxies = React.useMemo(() => {
    const proxiesByID = new Map(data.map(proxy => [String(proxy.id), proxy]))
    return Object.entries(rowSelection)
      .filter(([, selected]) => selected)
      .map(([rowId]) => proxiesByID.get(rowId))
      .filter((proxy): proxy is Proxy => Boolean(proxy))
  }, [data, rowSelection])

  const selectedTestableProxies = React.useMemo(
    () => selectedProxies.filter(proxy => proxy.status !== "archived"),
    [selectedProxies],
  )
  const selectedArchivableProxies = React.useMemo(
    () => selectedProxies.filter(proxy => proxy.status !== "archived"),
    [selectedProxies],
  )
  const selectedArchivedProxies = React.useMemo(
    () => selectedProxies.filter(proxy => proxy.status === "archived"),
    [selectedProxies],
  )

  // Debounce search query
  React.useEffect(() => {
    if (searchQuery === debouncedSearchQuery) return

    const timer = setTimeout(() => {
      setDebouncedSearchQuery(searchQuery)
      setPagination(prev => prev.page === 1 ? prev : { ...prev, page: 1 })
    }, 500)

    return () => clearTimeout(timer)
  }, [searchQuery, debouncedSearchQuery])

  React.useEffect(() => {
    setPageInput(String(pagination.page))
  }, [pagination.page])

  const fetchProxies = React.useCallback(async () => {
    const requestId = ++latestProxyRequestRef.current

    try {
      setIsLoading(true)

      // Build sort parameters from sorting state
      const sortField = sorting.length > 0 ? sorting[0].id : undefined
      const sortOrder = sorting.length > 0 ? (sorting[0].desc ? "desc" : "asc") : undefined

      const response = await api.getProxies({
        page: pagination.page,
        limit: pagination.limit,
        search: debouncedSearchQuery || undefined,
        status: statusFilter === "all" ? undefined : statusFilter,
        protocol: protocolFilter === "all" ? undefined : protocolFilter,
        tag: originFilter === "all" ? undefined : originFilter,
        sort: sortField,
        order: sortOrder as "asc" | "desc" | undefined,
      })

      if (requestId !== latestProxyRequestRef.current) return

      setData(response.proxies)
      setPagination(response.pagination)
    } catch (error) {
      if (requestId === latestProxyRequestRef.current) {
        console.error("Failed to fetch proxies:", error)
      }
    } finally {
      if (requestId === latestProxyRequestRef.current) {
        setIsLoading(false)
      }
    }
  }, [pagination.page, pagination.limit, debouncedSearchQuery, statusFilter, protocolFilter, originFilter, sorting])

  React.useEffect(() => {
    fetchProxies()
  }, [fetchProxies])

  const fetchTagList = React.useCallback(async () => {
    try {
      setAllTags(await api.getTagList())
    } catch {
      // Suggestions are best-effort; tag editing remains available.
    }
  }, [])

  React.useEffect(() => {
    fetchTagList()
  }, [fetchTagList])

  React.useEffect(() => {
    let cancelled = false

    const fetchCapacity = async () => {
      try {
        const next = await api.getProxyCapacity()
        if (!cancelled) setCapacity(next)
      } catch (error) {
        if (!cancelled) console.error("Failed to fetch proxy capacity:", error)
      }
    }

    void fetchCapacity()
    const interval = window.setInterval(fetchCapacity, 10_000)
    return () => {
      cancelled = true
      window.clearInterval(interval)
    }
  }, [])

  const goToPage = (requestedPage: number) => {
    const lastPage = Math.max(pagination.total_pages, 1)
    const nextPage = Math.min(Math.max(Math.trunc(requestedPage), 1), lastPage)

    setPageInput(String(nextPage))
    setPagination(prev => prev.page === nextPage ? prev : { ...prev, page: nextPage })
  }

  const changePageBy = (delta: number) => {
    setPagination(prev => {
      const lastPage = Math.max(prev.total_pages, 1)
      const nextPage = Math.min(Math.max(prev.page + delta, 1), lastPage)
      return prev.page === nextPage ? prev : { ...prev, page: nextPage }
    })
  }

  const handlePageJump = (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault()

    if (!pageInput.trim()) {
      setPageInput(String(pagination.page))
      return
    }

    const requestedPage = Number(pageInput)
    if (!Number.isInteger(requestedPage)) {
      setPageInput(String(pagination.page))
      return
    }

    goToPage(requestedPage)
  }

  const handleAddProxy = async () => {
    try {
      await api.addProxy({
        address: newProxy.address,
        protocol: newProxy.protocol,
        username: newProxy.username || undefined,
        password: newProxy.password || undefined,
        tags: withProxyOrigin(newProxy.tags, newProxy.origin),
      })
      setIsAddDialogOpen(false)
      setNewProxy({ address: "", protocol: "http", username: "", password: "", origin: "origin:unknown", tags: [] })
      toast.success("Proxy added successfully")
      fetchProxies()
      fetchTagList()
    } catch (error) {
      console.error("Failed to add proxy:", error)
      toast.error("Failed to add proxy", error instanceof Error ? error.message : "Unknown error")
    }
  }

  const handleEditProxy = async () => {
    if (!editingProxy) return

    try {
      await api.updateProxy(editingProxy.id, {
        address: editingProxy.address,
        protocol: editingProxy.protocol,
        username: editingProxy.username,
        tags: editingProxy.tags,
      })
      setIsEditDialogOpen(false)
      setEditingProxy(null)
      toast.success("Proxy updated successfully")
      fetchProxies()
      fetchTagList()
    } catch (error) {
      console.error("Failed to update proxy:", error)
      toast.error("Failed to update proxy", error instanceof Error ? error.message : "Unknown error")
    }
  }

  const handleDeleteProxy = async (id: number) => {
    setDeleteConfirm({ open: true, proxyId: id })
  }

  const confirmDelete = async () => {
    if (!deleteConfirm.proxyId) return

    try {
      await api.deleteProxy(deleteConfirm.proxyId)
      toast.success("Proxy deleted successfully")
      fetchProxies()
    } catch (error) {
      console.error("Failed to delete proxy:", error)
      toast.error("Failed to delete proxy", error instanceof Error ? error.message : "Unknown error")
    } finally {
      setDeleteConfirm({ open: false, proxyId: null })
    }
  }

  const handleTestProxy = async (id: number) => {
    try {
      const result = await api.testProxy(id)
      if (result.status === "active") {
        const responseTime = result.response_time || result.duration || 0
        toast.success(
          "Proxy test successful",
          `${result.address} - Response time: ${responseTime}ms`
        )
      } else {
        toast.error(
          "Proxy test failed",
          `${result.address} - ${result.error || "Unknown error"}`
        )
      }
      fetchProxies()
    } catch (error) {
      console.error("Failed to test proxy:", error)
      toast.error("Failed to test proxy", error instanceof Error ? error.message : "Unknown error")
    }
  }

  const handleArchiveProxy = async (id: number) => {
    try {
      setIsLifecycleUpdating(true)
      const result = await api.archiveProxy(id)
      toast.success(result.archived ? "Proxy archived" : "Proxy is already archived")
      await fetchProxies()
    } catch (error) {
      toast.error("Failed to archive proxy", error instanceof Error ? error.message : "Unknown error")
    } finally {
      setIsLifecycleUpdating(false)
    }
  }

  const handleRestoreProxy = async (id: number) => {
    try {
      setIsLifecycleUpdating(true)
      const result = await api.restoreProxy(id)
      toast.success(result.restored ? "Proxy restored to pending validation" : "Proxy was not archived")
      await fetchProxies()
    } catch (error) {
      toast.error("Failed to restore proxy", error instanceof Error ? error.message : "Unknown error")
    } finally {
      setIsLifecycleUpdating(false)
    }
  }

  const handleBulkArchive = async () => {
    const ids = selectedArchivableProxies.map(proxy => proxy.id)
    if (ids.length === 0) return
    try {
      setIsLifecycleUpdating(true)
      const result = await api.bulkArchiveProxies(ids)
      setRowSelection({})
      toast.success(`${result.archived} proxies archived`)
      await fetchProxies()
    } catch (error) {
      toast.error("Failed to archive proxies", error instanceof Error ? error.message : "Unknown error")
    } finally {
      setIsLifecycleUpdating(false)
    }
  }

  const handleBulkRestore = async () => {
    const ids = selectedArchivedProxies.map(proxy => proxy.id)
    if (ids.length === 0) return
    try {
      setIsLifecycleUpdating(true)
      const result = await api.bulkRestoreProxies(ids)
      setRowSelection({})
      toast.success(`${result.restored} proxies restored to pending validation`)
      await fetchProxies()
    } catch (error) {
      toast.error("Failed to restore proxies", error instanceof Error ? error.message : "Unknown error")
    } finally {
      setIsLifecycleUpdating(false)
    }
  }

  const handleBulkOrigin = async (origin: OriginTag) => {
    if (selectedProxies.length === 0) return
    setIsTagging(true)
    try {
      const result = await api.bulkTagProxies({
        ids: selectedProxies.map(proxy => proxy.id),
        add: [origin],
      })
      setRowSelection({})
      toast.success(`${result.updated} proxies marked as ${originLabel(origin).toLowerCase()}`)
      await fetchProxies()
      await fetchTagList()
    } catch (error) {
      toast.error("Failed to classify proxies", error instanceof Error ? error.message : "Unknown error")
    } finally {
      setIsTagging(false)
    }
  }

  const handleBulkTag = async () => {
    if (selectedProxies.length === 0) return
    if (bulkAddTags.length === 0 && bulkRemoveTags.length === 0) {
      toast.error("Add or remove at least one tag")
      return
    }

    setIsTagging(true)
    try {
      const result = await api.bulkTagProxies({
        ids: selectedProxies.map(proxy => proxy.id),
        add: bulkAddTags,
        remove: bulkRemoveTags,
      })
      setIsTagDialogOpen(false)
      setBulkAddTags([])
      setBulkRemoveTags([])
      setRowSelection({})
      toast.success(`Tags updated on ${result.updated} proxies`)
      await fetchProxies()
      await fetchTagList()
    } catch (error) {
      toast.error("Failed to update tags", error instanceof Error ? error.message : "Unknown error")
    } finally {
      setIsTagging(false)
    }
  }

  const handleBulkTestProxies = async () => {
    if (selectedTestableProxies.length === 0 || isBulkTesting) return

    const proxiesToTest = [...selectedTestableProxies]
    const total = proxiesToTest.length
    const concurrency = Math.min(5, total)
    let nextIndex = 0
    let completed = 0
    let active = 0
    let failed = 0

    setIsBulkTesting(true)
    setBulkTestProgress({ current: 0, total })

    const runWorker = async () => {
      while (nextIndex < total) {
        const proxy = proxiesToTest[nextIndex]
        nextIndex++

        try {
          const result = await api.testProxy(proxy.id)
          if (result.status === "active") {
            active++
          } else {
            failed++
          }
        } catch (error) {
          failed++
          console.error("Failed to test proxy:", proxy.address, error)
        } finally {
          completed++
          setBulkTestProgress({ current: completed, total })
        }
      }
    }

    try {
      await Promise.all(Array.from({ length: concurrency }, runWorker))
      toast.success(
        "Bulk proxy test completed",
        `${active} active, ${failed} failed out of ${total}`
      )
      fetchProxies()
    } finally {
      setIsBulkTesting(false)
    }
  }

  const handleBulkDelete = async () => {
    const selectedIds = selectedProxies.map(proxy => proxy.id)
    if (selectedIds.length === 0) return
    setBulkDeleteConfirm(true)
  }

  const confirmBulkDelete = async () => {
    const selectedIds = selectedProxies.map(proxy => proxy.id)
    if (selectedIds.length === 0) {
      setBulkDeleteConfirm(false)
      return
    }

    try {
      await api.bulkDeleteProxies({ ids: selectedIds })
      setRowSelection({})
      toast.success(`${selectedIds.length} proxies deleted successfully`)
      fetchProxies()
    } catch (error) {
      console.error("Failed to delete proxies:", error)
      toast.error("Failed to delete proxies", error instanceof Error ? error.message : "Unknown error")
    } finally {
      setBulkDeleteConfirm(false)
    }
  }

  const confirmDeleteAll = async () => {
    try {
      const res = await api.deleteAllProxies()
      setRowSelection({})
      toast.success(`${res.deleted} proxies deleted`)
      fetchProxies()
    } catch {
      toast.error("Failed to delete all proxies")
    } finally {
      setDeleteAllConfirm(false)
    }
  }

  const handleExport = async (format: "txt" | "json" | "csv") => {
    try {
      const blob = await api.exportProxies(format)
      const url = URL.createObjectURL(blob)
      const a = document.createElement("a")
      a.href = url
      a.download = `proxies.${format}`
      a.click()
      URL.revokeObjectURL(url)
      toast.success(`Proxies exported as ${format.toUpperCase()}`)
    } catch (error) {
      console.error("Failed to export proxies:", error)
      toast.error("Failed to export proxies", error instanceof Error ? error.message : "Unknown error")
    }
  }

  const handleFileUpload = (file: File) => {
    if (!file.name.toLowerCase().endsWith('.txt')) {
      toast.error('Invalid file type', 'Please upload a .txt file')
      return
    }

    const reader = new FileReader()
    reader.onload = (e) => {
      const text = e.target?.result
      if (typeof text !== "string") {
        toast.error("Failed to read file", "The selected file did not contain readable text")
        return
      }
      setParsedProxies(parseImportedProxyText(text))
      setImportFile(file)
      setImportText("")
      setImportProgress({ current: 0, total: 0, success: 0, failed: 0, skipped: 0 })
      setImportResults([])
    }
    reader.onerror = () => {
      toast.error("Failed to read file", "The selected file could not be read")
    }
    reader.onabort = () => {
      toast.error("File read cancelled")
    }
    reader.readAsText(file)
  }

  const handleImportTextChange = (text: string) => {
    setImportText(text)
    setImportFile(null)
    setParsedProxies(parseImportedProxyText(text))
    setImportProgress({ current: 0, total: 0, success: 0, failed: 0, skipped: 0 })
    setImportResults([])
  }

  const handleDragOver = (e: React.DragEvent) => {
    e.preventDefault()
    setIsDragging(true)
  }

  const handleDragLeave = () => {
    setIsDragging(false)
  }

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault()
    setIsDragging(false)

    const files = Array.from(e.dataTransfer.files)
    const txtFile = files.find(f => f.name.toLowerCase().endsWith('.txt'))

    if (txtFile) {
      handleFileUpload(txtFile)
    } else {
      toast.error('Invalid file type', 'Please upload a .txt file')
    }
  }

  const handleImport = async () => {
    if (parsedProxies.length === 0) {
      toast.error('No proxies to import', 'Enter at least one valid proxy')
      return
    }

    setIsImporting(true)
    setImportProgress({ current: 0, total: parsedProxies.length, success: 0, failed: 0, skipped: 0 })
    setImportResults([])

    const results: Array<{ address: string; status: string; error?: string }> = []
    let success = 0
    let failed = 0
    let skipped = 0

    for (let i = 0; i < parsedProxies.length; i++) {
      const proxy = parsedProxies[i]
      const protocol = proxy.protocol || importProtocol
      const isShareNode = isShareProtocol(protocol)

      try {
        await api.addProxy({
          address: isShareNode && proxy.protocol === protocol ? proxy.raw : proxy.address,
          protocol,
          username: isShareNode ? undefined : proxy.username || importUsername || undefined,
          password: isShareNode ? undefined : proxy.password || importPassword || undefined,
          tags: [importOrigin],
        })

        success++
        results.push({ address: proxy.address, status: 'success' })
      } catch (error) {
        const errorMessage = error instanceof Error ? error.message : 'Unknown error'

        // Check if it's a duplicate error
        if (errorMessage.includes('already exists')) {
          skipped++
          results.push({
            address: proxy.address,
            status: 'skipped',
            error: 'Already exists (skipped)'
          })
        } else {
          failed++
          results.push({
            address: proxy.address,
            status: 'failed',
            error: errorMessage
          })
        }
      }

      setImportProgress({
        current: i + 1,
        total: parsedProxies.length,
        success,
        failed,
        skipped,
      })
      setImportResults([...results])
    }

    setIsImporting(false)
    // Refresh the proxy list
    setTimeout(() => {
      fetchProxies()
    }, 1000)
  }

  const resetImportDialog = () => {
    setImportFile(null)
    setImportText("")
    setParsedProxies([])
    setImportProtocol("http")
    setImportUsername("")
    setImportPassword("")
    setImportOrigin("origin:unknown")
    setIsImporting(false)
    setImportProgress({ current: 0, total: 0, success: 0, failed: 0, skipped: 0 })
    setImportResults([])
  }

  const handleReloadProxies = async () => {
    try {
      setIsReloading(true)
      await api.reloadProxies()
      toast.success('Proxy pool reloaded', 'All proxies from database are now available for rotation')
    } catch (error) {
      console.error('Failed to reload proxies:', error)
      toast.error('Failed to reload proxy pool', error instanceof Error ? error.message : "Unknown error")
    } finally {
      setIsReloading(false)
    }
  }

  const columns: ColumnDef<Proxy>[] = [
    {
      id: "select",
      header: ({ table }) => (
        <Checkbox
          checked={
            table.getIsAllPageRowsSelected() ||
            (table.getIsSomePageRowsSelected() && "indeterminate")
          }
          onCheckedChange={(value) => table.toggleAllPageRowsSelected(!!value)}
          aria-label="Select all"
        />
      ),
      cell: ({ row }) => (
        <Checkbox
          checked={row.getIsSelected()}
          onCheckedChange={(value) => row.toggleSelected(!!value)}
          aria-label="Select row"
        />
      ),
      enableSorting: false,
      enableHiding: false,
    },
    {
      accessorKey: "address",
      header: ({ column }) => {
        return (
          <Button
            variant="ghost"
            onClick={() => column.toggleSorting(column.getIsSorted() === "asc")}
          >
            Address
            <ArrowUpDown className="ml-2 h-4 w-4" />
          </Button>
        )
      },
      cell: ({ row }) => <div className="font-mono">{row.getValue("address")}</div>,
    },
    {
      accessorKey: "protocol",
      header: "Protocol",
      cell: ({ row }) => (
        <Badge variant="outline" className="uppercase">
          {row.getValue("protocol")}
        </Badge>
      ),
    },
    {
      id: "origin",
      header: "Origin",
      cell: ({ row }) => {
        const origin = proxyOrigin(row.original.tags)
        return <Badge variant="outline">{originLabel(origin)}</Badge>
      },
    },
    {
      accessorKey: "tags",
      header: "Tags",
      enableSorting: false,
      cell: ({ row }) => {
        const tags = withoutProxyOrigin(row.original.tags)
        if (tags.length === 0) {
          return <span className="text-muted-foreground">None</span>
        }
        return (
          <div className="flex max-w-52 flex-wrap gap-1">
            {tags.slice(0, 3).map(tag => (
              <Badge key={tag} variant="secondary" className="max-w-36 text-xs" title={tag}>
                <span className="truncate">{tag}</span>
              </Badge>
            ))}
            {tags.length > 3 && (
              <Badge variant="outline" className="text-xs" title={tags.slice(3).join(", ")}>
                +{tags.length - 3}
              </Badge>
            )}
          </div>
        )
      },
    },
    {
      accessorKey: "country_name",
      header: "Country",
      cell: ({ row }) => {
        const countryCode = row.original.country_code?.toUpperCase()
        const countryName = row.original.country_name

        if (!countryCode && !countryName) {
          return (
            <span className="text-muted-foreground">
              {row.original.geo_updated_at ? "Unknown" : "Pending"}
            </span>
          )
        }

        return (
          <div className="flex min-w-36 items-center gap-2">
            {countryCode && (
              <Badge variant="outline" className="min-w-10 justify-center font-mono">
                {countryCode}
              </Badge>
            )}
            <span>{countryName || countryCode}</span>
          </div>
        )
      },
    },
    {
      accessorKey: "status",
      header: ({ column }) => {
        return (
          <Button
            variant="ghost"
            onClick={() => column.toggleSorting(column.getIsSorted() === "asc")}
          >
            Status
            <ArrowUpDown className="ml-2 h-4 w-4" />
          </Button>
        )
      },
      cell: ({ row }) => {
        const proxy = row.original
        const status = proxy.status
        const statusColors = {
          active: "text-green-600",
          failed: "text-red-600",
          idle: "text-yellow-600",
          archived: "text-muted-foreground",
        }
        const detail = status === "archived"
          ? [failureLabel(proxy.archive_reason, "Archived"), formatLifecycleTime(proxy.archived_at)].filter(Boolean).join(" / ")
          : status === "failed"
            ? [failureLabel(proxy.failure_episode_kind, "Failure under observation"), formatLifecycleTime(proxy.failed_since)].filter(Boolean).join(" / ")
            : null
        return (
          <div className="min-w-44">
            <div className={`flex items-center gap-2 ${statusColors[status]}`}>
              <div className={`h-2 w-2 shrink-0 rounded-full ${
                status === "active" ? "bg-green-600" :
                status === "failed" ? "bg-red-600" :
                status === "archived" ? "bg-muted-foreground" :
                "bg-yellow-600"
              }`} />
              <span className="font-medium">{lifecycleLabel(status)}</span>
            </div>
            {detail && <div className="mt-1 text-xs text-muted-foreground">{detail}</div>}
          </div>
        )
      },
    },
    {
      accessorKey: "requests",
      header: ({ column }) => {
        return (
          <Button
            variant="ghost"
            onClick={() => column.toggleSorting(column.getIsSorted() === "asc")}
          >
            Requests
            <ArrowUpDown className="ml-2 h-4 w-4" />
          </Button>
        )
      },
      cell: ({ row }) => {
        const value = finiteCellNumber(row.getValue("requests"))
        return (
          <div suppressHydrationWarning>
            {value === null ? "-" : value.toLocaleString("en-US")}
          </div>
        )
      },
    },
    {
      accessorKey: "success_rate",
      header: ({ column }) => {
        return (
          <Button
            variant="ghost"
            onClick={() => column.toggleSorting(column.getIsSorted() === "asc")}
          >
            Success Rate
            <ArrowUpDown className="ml-2 h-4 w-4" />
          </Button>
        )
      },
      cell: ({ row }) => {
        const value = finiteCellNumber(row.getValue("success_rate"))
        return <div>{value === null ? "-" : `${value.toFixed(1)}%`}</div>
      },
    },
    {
      accessorKey: "avg_response_time",
      header: "Avg Response",
      cell: ({ row }) => {
        const value = finiteCellNumber(row.getValue("avg_response_time"))
        return <div>{value === null ? "-" : `${value}ms`}</div>
      },
    },
    {
      accessorKey: "last_check",
      header: "Last Check",
      cell: ({ row }) => {
        const raw = row.getValue("last_check") as string | null | undefined
        if (!raw || raw === "idle") {
          return <div className="text-muted-foreground">-</div>
        }
        const date = new Date(raw)
        if (Number.isNaN(date.getTime())) {
          return <div className="text-muted-foreground">{raw}</div>
        }
        return <div suppressHydrationWarning>{date.toLocaleString()}</div>
      },
    },
    {
      id: "actions",
      header: () => (
        <Button
          variant="outline"
          size="sm"
          onClick={handleBulkTestProxies}
          disabled={selectedTestableProxies.length === 0 || isBulkTesting}
          className="h-8 whitespace-nowrap"
        >
          {isBulkTesting ? (
            <Loader2 className="mr-2 h-4 w-4 animate-spin" />
          ) : (
            <Activity className="mr-2 h-4 w-4" />
          )}
          {isBulkTesting
            ? `Testing ${bulkTestProgress.current}/${bulkTestProgress.total}`
            : `Test (${selectedTestableProxies.length})`}
        </Button>
      ),
      enableHiding: false,
      cell: ({ row }) => {
        const proxy = row.original

        return (
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button variant="ghost" className="h-8 w-8 p-0">
                <span className="sr-only">Open menu</span>
                <MoreHorizontal className="h-4 w-4" />
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end">
              <DropdownMenuLabel>Actions</DropdownMenuLabel>
              <DropdownMenuItem
                onClick={() => navigator.clipboard.writeText(proxy.address)}
              >
                Copy address
              </DropdownMenuItem>
              <DropdownMenuSeparator />
              <DropdownMenuItem
                onClick={() => handleTestProxy(proxy.id)}
                disabled={proxy.status === "archived"}
              >
                Test proxy
              </DropdownMenuItem>
              {proxy.status === "archived" ? (
                <DropdownMenuItem
                  onClick={() => handleRestoreProxy(proxy.id)}
                  disabled={isLifecycleUpdating}
                >
                  <RotateCcw className="mr-2 h-4 w-4" />
                  Restore to pending validation
                </DropdownMenuItem>
              ) : (
                <DropdownMenuItem
                  onClick={() => handleArchiveProxy(proxy.id)}
                  disabled={isLifecycleUpdating}
                >
                  <Archive className="mr-2 h-4 w-4" />
                  Archive
                </DropdownMenuItem>
              )}
              <DropdownMenuItem onClick={() => {
                setEditingProxy(proxy)
                setIsEditDialogOpen(true)
              }}>
                Edit
              </DropdownMenuItem>
              <DropdownMenuItem
                className="text-red-600"
                onClick={() => handleDeleteProxy(proxy.id)}
              >
                Delete
              </DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu>
        )
      },
    },
  ]

  const table = useReactTable({
    data,
    columns,
    getRowId: row => String(row.id),
    onSortingChange: setSorting,
    onColumnFiltersChange: setColumnFilters,
    getCoreRowModel: getCoreRowModel(),
    onColumnVisibilityChange: setColumnVisibility,
    onRowSelectionChange: setRowSelection,
    manualPagination: true,
    manualSorting: true,
    manualFiltering: true,
    pageCount: pagination.total_pages,
    state: {
      sorting,
      columnFilters,
      columnVisibility,
      rowSelection,
    },
  })

  if (isLoading && data.length === 0) {
    return (
      <div className="flex items-center justify-center h-96">
        <Loader2 className="h-8 w-8 animate-spin" />
      </div>
    )
  }

  return (
    <div className="space-y-4">
      <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Proxy Management</h1>
          <p className="text-muted-foreground">
            Manage and monitor your proxy infrastructure
          </p>
        </div>
        {capacity && (
          <div className="grid w-full grid-cols-3 divide-x border-y lg:w-auto lg:min-w-[30rem]">
            <div className="px-4 py-2">
              <div className="text-xs text-muted-foreground">Warm reserve</div>
              <div className={capacity.reserve_below_minimum ? "text-xl font-semibold text-red-600" : "text-xl font-semibold text-green-600"}>
                {capacity.reserve.toLocaleString("en-US")}
              </div>
              <div className="text-xs text-muted-foreground">Minimum {capacity.minimum_reserve}</div>
            </div>
            <div className="px-4 py-2">
              <div className="text-xs text-muted-foreground">Bound slots</div>
              <div className="text-xl font-semibold">{capacity.running.toLocaleString("en-US")}</div>
            </div>
            <div className="px-4 py-2">
              <div className="text-xs text-muted-foreground">Eligible total</div>
              <div className="text-xl font-semibold">{capacity.active.toLocaleString("en-US")}</div>
            </div>
          </div>
        )}
      </div>

      <Card>
        <CardHeader>
          <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
            <div>
              <CardTitle>Proxies</CardTitle>
              <CardDescription>
                {pagination.total} total proxies
              </CardDescription>
            </div>
            <div className="flex flex-wrap items-center gap-2 sm:justify-end">
              <Button onClick={() => setIsAddDialogOpen(true)}>
                <Plus className="mr-2 h-4 w-4" />
                Add Proxy
              </Button>
              <Button
                variant="outline"
                onClick={handleReloadProxies}
                disabled={isReloading}
              >
                <Loader2 className={`mr-2 h-4 w-4 ${isReloading ? 'animate-spin' : ''}`} />
                Reload Pool
              </Button>
              <DropdownMenu>
                <DropdownMenuTrigger asChild>
                  <Button variant="outline">
                    Bulk Actions
                    <ChevronDown className="ml-2 h-4 w-4" />
                  </Button>
                </DropdownMenuTrigger>
                <DropdownMenuContent align="end">
                  <DropdownMenuItem onClick={() => setIsImportDialogOpen(true)}>
                    <Upload className="mr-2 h-4 w-4" />
                    Import Proxies
                  </DropdownMenuItem>
                  <DropdownMenuItem
                    onClick={handleBulkTestProxies}
                    disabled={selectedTestableProxies.length === 0 || isBulkTesting}
                  >
                    {isBulkTesting ? (
                      <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                    ) : (
                      <Activity className="mr-2 h-4 w-4" />
                    )}
                    Test ({selectedTestableProxies.length})
                  </DropdownMenuItem>
                  <DropdownMenuItem
                    onClick={handleBulkArchive}
                    disabled={selectedArchivableProxies.length === 0 || isLifecycleUpdating}
                  >
                    <Archive className="mr-2 h-4 w-4" />
                    Archive selected ({selectedArchivableProxies.length})
                  </DropdownMenuItem>
                  <DropdownMenuItem
                    onClick={handleBulkRestore}
                    disabled={selectedArchivedProxies.length === 0 || isLifecycleUpdating}
                  >
                    <RotateCcw className="mr-2 h-4 w-4" />
                    Restore selected ({selectedArchivedProxies.length})
                  </DropdownMenuItem>
                  <DropdownMenuSeparator />
                  <DropdownMenuItem
                    onClick={() => setIsTagDialogOpen(true)}
                    disabled={selectedProxies.length === 0 || isTagging}
                  >
                    <Tag className="mr-2 h-4 w-4" />
                    Edit tags ({selectedProxies.length})
                  </DropdownMenuItem>
                  <DropdownMenuSeparator />
                  <DropdownMenuLabel>Set origin</DropdownMenuLabel>
                  <DropdownMenuItem
                    onClick={() => handleBulkOrigin("origin:paid")}
                    disabled={selectedProxies.length === 0 || isTagging}
                  >
                    Mark selected as paid
                  </DropdownMenuItem>
                  <DropdownMenuItem
                    onClick={() => handleBulkOrigin("origin:free")}
                    disabled={selectedProxies.length === 0 || isTagging}
                  >
                    Mark selected as free
                  </DropdownMenuItem>
                  <DropdownMenuItem
                    onClick={() => handleBulkOrigin("origin:unknown")}
                    disabled={selectedProxies.length === 0 || isTagging}
                  >
                    Mark selected as unknown
                  </DropdownMenuItem>
                  <DropdownMenuSeparator />
                  <DropdownMenuItem onClick={() => handleExport("txt")}>
                    <Download className="mr-2 h-4 w-4" />
                    Export as TXT
                  </DropdownMenuItem>
                  <DropdownMenuItem onClick={() => handleExport("json")}>
                    <Download className="mr-2 h-4 w-4" />
                    Export as JSON
                  </DropdownMenuItem>
                  <DropdownMenuItem onClick={() => handleExport("csv")}>
                    <Download className="mr-2 h-4 w-4" />
                    Export as CSV
                  </DropdownMenuItem>
                  <DropdownMenuSeparator />
                  <DropdownMenuItem
                    className="text-red-600"
                    onClick={handleBulkDelete}
                    disabled={selectedProxies.length === 0}
                  >
                    <Trash2 className="mr-2 h-4 w-4" />
                    Delete selected ({selectedProxies.length})
                  </DropdownMenuItem>
                  <DropdownMenuSeparator />
                  <DropdownMenuItem
                    className="text-red-600 font-semibold"
                    onClick={() => setDeleteAllConfirm(true)}
                  >
                    <Trash2 className="mr-2 h-4 w-4" />
                    Delete ALL proxies
                  </DropdownMenuItem>
                </DropdownMenuContent>
              </DropdownMenu>
            </div>
          </div>
        </CardHeader>
        <CardContent>
          <div className="space-y-4">
            <div className="flex flex-wrap items-center gap-2">
              <Input
                placeholder="Search by address..."
                value={searchQuery}
                onChange={(event) => setSearchQuery(event.target.value)}
                className="w-full sm:max-w-sm"
              />
              <DropdownMenu>
                <DropdownMenuTrigger asChild>
                  <Button variant="outline" size="icon" className="relative">
                    <Filter className="h-4 w-4" />
                    {(statusFilter !== "all" || protocolFilter !== "all" || originFilter !== "all") && (
                      <span className="absolute -top-1 -right-1 h-3 w-3 rounded-full bg-primary" />
                    )}
                  </Button>
                </DropdownMenuTrigger>
                <DropdownMenuContent align="end" className="w-56">
                  <DropdownMenuLabel>Filters</DropdownMenuLabel>
                  <DropdownMenuSeparator />
                  <div className="px-2 py-2">
                    <Label className="text-xs text-muted-foreground mb-2 block">Status</Label>
                    <Select
                      value={statusFilter}
                      onValueChange={(value) => {
                        setStatusFilter(value)
                        setPagination(prev => ({ ...prev, page: 1 }))
                      }}
                    >
                      <SelectTrigger className="w-full">
                        <SelectValue placeholder="All statuses" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="all">All statuses</SelectItem>
                        <SelectItem value="active">Active</SelectItem>
                        <SelectItem value="failed">Failed</SelectItem>
                        <SelectItem value="idle">Pending validation</SelectItem>
                        <SelectItem value="archived">Archived</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                  <DropdownMenuSeparator />
                  <div className="px-2 py-2">
                    <Label className="text-xs text-muted-foreground mb-2 block">Origin</Label>
                    <Select
                      value={originFilter}
                      onValueChange={(value) => {
                        setOriginFilter(value)
                        setPagination(prev => ({ ...prev, page: 1 }))
                      }}
                    >
                      <SelectTrigger className="w-full">
                        <SelectValue placeholder="All origins" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="all">All origins</SelectItem>
                        <SelectItem value="origin:paid">Paid</SelectItem>
                        <SelectItem value="origin:free">Free</SelectItem>
                        <SelectItem value="origin:unknown">Unknown</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                  <DropdownMenuSeparator />
                  <div className="px-2 py-2">
                    <Label className="text-xs text-muted-foreground mb-2 block">Protocol</Label>
                    <Select
                      value={protocolFilter}
                      onValueChange={(value) => {
                        setProtocolFilter(value)
                        setPagination(prev => ({ ...prev, page: 1 }))
                      }}
                    >
                      <SelectTrigger className="w-full">
                        <SelectValue placeholder="All protocols" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="all">All protocols</SelectItem>
                        <SelectItem value="http">HTTP</SelectItem>
                        <SelectItem value="https">HTTPS</SelectItem>
                        <SelectItem value="socks4">SOCKS4</SelectItem>
                        <SelectItem value="socks4a">SOCKS4A</SelectItem>
                        <SelectItem value="socks5">SOCKS5</SelectItem>
                        <SelectItem value="vless">VLESS</SelectItem>
                        <SelectItem value="vmess">VMess</SelectItem>
                        <SelectItem value="trojan">Trojan</SelectItem>
                        <SelectItem value="shadowsocks">Shadowsocks</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                </DropdownMenuContent>
              </DropdownMenu>
              <DropdownMenu>
                <DropdownMenuTrigger asChild>
                  <Button variant="outline">
                    Columns <ChevronDown className="ml-2 h-4 w-4" />
                  </Button>
                </DropdownMenuTrigger>
                <DropdownMenuContent align="end">
                  {table
                    .getAllColumns()
                    .filter((column) => column.getCanHide())
                    .map((column) => {
                      return (
                        <DropdownMenuCheckboxItem
                          key={column.id}
                          className="capitalize"
                          checked={column.getIsVisible()}
                          onCheckedChange={(value) =>
                            column.toggleVisibility(!!value)
                          }
                        >
                          {column.id}
                        </DropdownMenuCheckboxItem>
                      )
                    })}
                </DropdownMenuContent>
              </DropdownMenu>
            </div>
            <div className="relative overflow-x-auto rounded-md border" aria-busy={isLoading}>
              {isLoading && data.length > 0 && (
                <div
                  className="absolute inset-0 z-10 flex items-center justify-center bg-background/60"
                  role="status"
                  aria-label="Loading proxies"
                >
                  <Loader2 className="h-6 w-6 animate-spin" />
                </div>
              )}
              <Table>
                <TableHeader>
                  {table.getHeaderGroups().map((headerGroup) => (
                    <TableRow key={headerGroup.id}>
                      {headerGroup.headers.map((header) => {
                        return (
                          <TableHead key={header.id}>
                            {header.isPlaceholder
                              ? null
                              : flexRender(
                                  header.column.columnDef.header,
                                  header.getContext()
                                )}
                          </TableHead>
                        )
                      })}
                    </TableRow>
                  ))}
                </TableHeader>
                <TableBody>
                  {table.getRowModel().rows?.length ? (
                    table.getRowModel().rows.map((row) => (
                      <TableRow
                        key={row.id}
                        data-state={row.getIsSelected() && "selected"}
                      >
                        {row.getVisibleCells().map((cell) => (
                          <TableCell key={cell.id}>
                            {flexRender(
                              cell.column.columnDef.cell,
                              cell.getContext()
                            )}
                          </TableCell>
                        ))}
                      </TableRow>
                    ))
                  ) : (
                    <TableRow>
                      <TableCell
                        colSpan={columns.length}
                        className="h-24 text-center"
                      >
                        No results.
                      </TableCell>
                    </TableRow>
                  )}
                </TableBody>
              </Table>
            </div>
            <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
              <div className="flex-1 text-sm text-muted-foreground">
                {table.getFilteredSelectedRowModel().rows.length} of{" "}
                {table.getFilteredRowModel().rows.length} row(s) selected.
              </div>
              <nav
                aria-label="Proxy pagination"
                className="flex flex-wrap items-center gap-x-4 gap-y-2 lg:justify-end"
              >
                <div className="whitespace-nowrap text-sm text-muted-foreground">
                  Page {pagination.page} of {pagination.total_pages} ({pagination.total} total proxies)
                </div>
                <form className="flex items-center gap-2" onSubmit={handlePageJump} noValidate>
                  <Label
                    htmlFor="proxy-page-jump"
                    className="whitespace-nowrap text-sm text-muted-foreground"
                  >
                    Go to page
                  </Label>
                  <Input
                    id="proxy-page-jump"
                    type="number"
                    min={1}
                    max={Math.max(pagination.total_pages, 1)}
                    required
                    value={pageInput}
                    onChange={(event) => setPageInput(event.target.value)}
                    className="h-8 w-20"
                    disabled={pagination.total_pages <= 1}
                  />
                  <Button
                    type="submit"
                    variant="outline"
                    size="sm"
                    disabled={pagination.total_pages <= 1}
                  >
                    Go
                  </Button>
                </form>
                <div className="flex items-center space-x-2">
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => changePageBy(-1)}
                    disabled={pagination.page <= 1}
                  >
                    <ChevronLeft />
                    Previous
                  </Button>
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => changePageBy(1)}
                    disabled={pagination.page >= pagination.total_pages}
                  >
                    Next
                    <ChevronRight />
                  </Button>
                </div>
              </nav>
            </div>
          </div>
        </CardContent>
      </Card>

      <p className="text-right text-xs text-muted-foreground">
        <a
          href="https://db-ip.com"
          target="_blank"
          rel="noreferrer"
          className="underline-offset-4 hover:underline"
        >
          IP Geolocation by DB-IP
        </a>
      </p>

      {/* Add Proxy Dialog */}
      <Dialog open={isAddDialogOpen} onOpenChange={setIsAddDialogOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Add New Proxy</DialogTitle>
            <DialogDescription>
              Add a new proxy to your pool
            </DialogDescription>
          </DialogHeader>
          <div className="grid gap-4 py-4">
            <div className="grid gap-2">
              <Label htmlFor="address">
                {isShareProtocol(newProxy.protocol) ? `${newProxy.protocol.toUpperCase()} URI` : "Address"}
              </Label>
              <Input
                id="address"
                type={isShareProtocol(newProxy.protocol) ? "password" : "text"}
                placeholder={isShareProtocol(newProxy.protocol) ? `${shareScheme(newProxy.protocol)}://...` : "192.168.1.100:8001"}
                className="font-mono"
                value={newProxy.address}
                onChange={(e) => setNewProxy({ ...newProxy, address: e.target.value })}
              />
            </div>
            <div className="grid gap-2">
              <Label htmlFor="protocol">Protocol</Label>
              <Select
                value={newProxy.protocol}
                onValueChange={(value: Proxy["protocol"]) => setNewProxy({
                  ...newProxy,
                  protocol: value,
                  ...(isShareProtocol(value) ? { username: "", password: "" } : {}),
                })}
              >
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="http">HTTP</SelectItem>
                  <SelectItem value="https">HTTPS</SelectItem>
                  <SelectItem value="socks4">SOCKS4</SelectItem>
                  <SelectItem value="socks4a">SOCKS4A</SelectItem>
                  <SelectItem value="socks5">SOCKS5</SelectItem>
                  <SelectItem value="vless">VLESS</SelectItem>
                  <SelectItem value="vmess">VMess</SelectItem>
                  <SelectItem value="trojan">Trojan</SelectItem>
                  <SelectItem value="shadowsocks">Shadowsocks</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div className="grid gap-2">
              <Label htmlFor="origin">Origin</Label>
              <Select
                value={newProxy.origin}
                onValueChange={(value: OriginTag) => setNewProxy({ ...newProxy, origin: value })}
              >
                <SelectTrigger id="origin">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="origin:paid">Paid</SelectItem>
                  <SelectItem value="origin:free">Free</SelectItem>
                  <SelectItem value="origin:unknown">Unknown</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div className="grid gap-2">
              <Label htmlFor="tags">Tags (optional)</Label>
              <TagInput
                id="tags"
                value={newProxy.tags}
                onChange={tags => setNewProxy({ ...newProxy, tags })}
                suggestions={withoutProxyOrigin(allTags)}
              />
            </div>
            {!isShareProtocol(newProxy.protocol) && (
              <div className="grid gap-2">
                <Label htmlFor="username">Username (optional)</Label>
                <Input
                  id="username"
                  value={newProxy.username}
                  onChange={(e) => setNewProxy({ ...newProxy, username: e.target.value })}
                />
              </div>
            )}
            {!isShareProtocol(newProxy.protocol) && (
              <div className="grid gap-2">
                <Label htmlFor="password">Password (optional)</Label>
                <Input
                  id="password"
                  type="password"
                  value={newProxy.password}
                  onChange={(e) => setNewProxy({ ...newProxy, password: e.target.value })}
                />
              </div>
            )}
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setIsAddDialogOpen(false)}>
              Cancel
            </Button>
            <Button onClick={handleAddProxy}>
              Add Proxy
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Edit Proxy Dialog */}
      <Dialog open={isEditDialogOpen} onOpenChange={setIsEditDialogOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Edit Proxy</DialogTitle>
            <DialogDescription>
              Update proxy configuration
            </DialogDescription>
          </DialogHeader>
          {editingProxy && (
            <div className="grid gap-4 py-4">
              <div className="grid gap-2">
                <Label htmlFor="edit-address">Address</Label>
                <Input
                  id="edit-address"
                  placeholder="192.168.1.100:8001"
                  className="font-mono"
                  value={editingProxy.address}
                  disabled={isShareProtocol(editingProxy.protocol)}
                  onChange={(e) => setEditingProxy({ ...editingProxy, address: e.target.value })}
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="edit-protocol">Protocol</Label>
                <Select
                  value={editingProxy.protocol}
                  disabled={isShareProtocol(editingProxy.protocol)}
                  onValueChange={(value: Proxy["protocol"]) => setEditingProxy({ ...editingProxy, protocol: value })}
                >
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="http">HTTP</SelectItem>
                    <SelectItem value="https">HTTPS</SelectItem>
                    <SelectItem value="socks4">SOCKS4</SelectItem>
                    <SelectItem value="socks4a">SOCKS4A</SelectItem>
                    <SelectItem value="socks5">SOCKS5</SelectItem>
                    <SelectItem value="vless" disabled={editingProxy.protocol !== "vless"}>VLESS</SelectItem>
                    <SelectItem value="vmess" disabled={editingProxy.protocol !== "vmess"}>VMess</SelectItem>
                    <SelectItem value="trojan" disabled={editingProxy.protocol !== "trojan"}>Trojan</SelectItem>
                    <SelectItem value="shadowsocks" disabled={editingProxy.protocol !== "shadowsocks"}>Shadowsocks</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div className="grid gap-2">
                <Label htmlFor="edit-origin">Origin</Label>
                <Select
                  value={proxyOrigin(editingProxy.tags)}
                  onValueChange={(value: OriginTag) => setEditingProxy({
                    ...editingProxy,
                    tags: withProxyOrigin(editingProxy.tags, value),
                  })}
                >
                  <SelectTrigger id="edit-origin">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="origin:paid">Paid</SelectItem>
                    <SelectItem value="origin:free">Free</SelectItem>
                    <SelectItem value="origin:unknown">Unknown</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div className="grid gap-2">
                <Label htmlFor="edit-tags">Tags</Label>
                <TagInput
                  id="edit-tags"
                  value={withoutProxyOrigin(editingProxy.tags)}
                  onChange={tags => setEditingProxy({
                    ...editingProxy,
                    tags: withProxyOrigin(tags, proxyOrigin(editingProxy.tags)),
                  })}
                  suggestions={withoutProxyOrigin(allTags)}
                />
              </div>
              {!isShareProtocol(editingProxy.protocol) && (
                <div className="grid gap-2">
                  <Label htmlFor="edit-username">Username (optional)</Label>
                  <Input
                    id="edit-username"
                    value={editingProxy.username || ""}
                    onChange={(e) => setEditingProxy({ ...editingProxy, username: e.target.value })}
                  />
                </div>
              )}
            </div>
          )}
          <DialogFooter>
            <Button variant="outline" onClick={() => setIsEditDialogOpen(false)}>
              Cancel
            </Button>
            <Button onClick={handleEditProxy}>
              Save Changes
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={isTagDialogOpen} onOpenChange={open => {
        setIsTagDialogOpen(open)
        if (!open) {
          setBulkAddTags([])
          setBulkRemoveTags([])
        }
      }}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Edit tags for {selectedProxies.length} proxies</DialogTitle>
          </DialogHeader>
          <div className="grid gap-4 py-4">
            <div className="grid gap-2">
              <Label htmlFor="bulk-add-tags">Add tags</Label>
              <TagInput
                id="bulk-add-tags"
                value={bulkAddTags}
                onChange={setBulkAddTags}
                suggestions={withoutProxyOrigin(allTags)}
                disabled={isTagging}
              />
            </div>
            <div className="grid gap-2">
              <Label htmlFor="bulk-remove-tags">Remove tags</Label>
              <TagInput
                id="bulk-remove-tags"
                value={bulkRemoveTags}
                onChange={setBulkRemoveTags}
                suggestions={withoutProxyOrigin(allTags)}
                disabled={isTagging}
              />
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setIsTagDialogOpen(false)} disabled={isTagging}>
              Cancel
            </Button>
            <Button
              onClick={handleBulkTag}
              disabled={isTagging || (bulkAddTags.length === 0 && bulkRemoveTags.length === 0)}
            >
              {isTagging && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
              Apply Tags
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Import Proxies Dialog */}
      <Dialog open={isImportDialogOpen} onOpenChange={(open) => {
        setIsImportDialogOpen(open)
        if (!open) resetImportDialog()
      }}>
        <DialogContent className="max-w-2xl max-h-[90vh] overflow-y-auto">
          <DialogHeader>
            <DialogTitle>Import Proxies</DialogTitle>
            <DialogDescription>
              Upload a TXT file or paste one proxy per line.
            </DialogDescription>
          </DialogHeader>

          <div className="grid gap-4 py-4">
            {!importFile ? (
              <>
                <div
                  className={`border-2 border-dashed rounded-lg p-6 text-center cursor-pointer transition-colors ${
                    isDragging
                      ? 'border-primary bg-primary/5'
                      : 'border-muted-foreground/25 hover:border-primary/50'
                  }`}
                  onDragOver={handleDragOver}
                  onDragLeave={handleDragLeave}
                  onDrop={handleDrop}
                  onClick={() => {
                    const input = document.createElement('input')
                    input.type = 'file'
                    input.accept = '.txt,text/plain'
                    input.onchange = (e) => {
                      const file = (e.target as HTMLInputElement).files?.[0]
                      if (file) handleFileUpload(file)
                    }
                    input.click()
                  }}
                >
                  <FileText className="mx-auto h-10 w-10 text-muted-foreground mb-3" />
                  <p className="font-medium mb-1">
                    Drop a TXT file here or click to browse
                  </p>
                  <p className="text-sm text-muted-foreground">
                    HOST:PORT, USER:PASS@HOST:PORT, or a VLESS URI
                  </p>
                </div>

                <div className="grid gap-2">
                  <Label htmlFor="import-text">Paste proxies</Label>
                  <Textarea
                    id="import-text"
                    value={importText}
                    onChange={(event) => handleImportTextChange(event.target.value)}
                    disabled={isImporting}
                    rows={7}
                    className="min-h-40 font-mono text-sm"
                    placeholder={'host:port\nhttp://username:password@host:port\nvless://...\nvmess://...\ntrojan://...\nss://...'}
                  />
                  {importText.trim() && (
                    <p
                      className={`text-sm ${parsedProxies.length > 0 ? 'text-muted-foreground' : 'text-destructive'}`}
                      aria-live="polite"
                    >
                      {parsedProxies.length > 0
                        ? `${parsedProxies.length} valid proxies found`
                        : 'No valid proxies found'}
                    </p>
                  )}
                </div>
              </>
            ) : (
              <div className="flex items-center justify-between p-4 bg-muted rounded-lg">
                <div className="flex items-center gap-3 min-w-0">
                  <FileText className="h-5 w-5 text-primary shrink-0" />
                  <div className="min-w-0">
                    <p className="font-medium truncate">{importFile.name}</p>
                    <p className="text-sm text-muted-foreground">
                      {parsedProxies.length} valid proxies found
                    </p>
                  </div>
                </div>
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={() => {
                    setImportFile(null)
                    setParsedProxies([])
                    setImportProgress({ current: 0, total: 0, success: 0, failed: 0, skipped: 0 })
                    setImportResults([])
                  }}
                  disabled={isImporting}
                >
                  Change File
                </Button>
              </div>
            )}

            {parsedProxies.length > 0 && (
              <>
                    <div className="grid gap-2">
                      <Label>Preview (first 10, passwords hidden)</Label>
                      <div className="border rounded-md p-3 bg-muted/30 max-h-32 overflow-y-auto">
                        <div className="font-mono text-sm space-y-1">
                          {parsedProxies.slice(0, 10).map((proxy, idx) => (
                            <div key={idx} className="text-muted-foreground">
                              {formatImportedProxyPreview(proxy)}
                            </div>
                          ))}
                          {parsedProxies.length > 10 && (
                            <div className="text-xs text-muted-foreground pt-1">
                              ... and {parsedProxies.length - 10} more
                            </div>
                          )}
                        </div>
                      </div>
                    </div>

                    <div className="grid gap-2">
                      <Label htmlFor="import-protocol">Protocol</Label>
                      <Select
                        value={importProtocol}
                        onValueChange={(value) => setImportProtocol(value as Proxy["protocol"])}
                        disabled={isImporting}
                      >
                        <SelectTrigger>
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="http">HTTP</SelectItem>
                          <SelectItem value="https">HTTPS</SelectItem>
                          <SelectItem value="socks4">SOCKS4</SelectItem>
                          <SelectItem value="socks4a">SOCKS4A</SelectItem>
                          <SelectItem value="socks5">SOCKS5</SelectItem>
                          <SelectItem value="vless">VLESS</SelectItem>
                          <SelectItem value="vmess">VMess</SelectItem>
                          <SelectItem value="trojan">Trojan</SelectItem>
                          <SelectItem value="shadowsocks">Shadowsocks</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>

                    <div className="grid gap-2">
                      <Label htmlFor="import-origin">Origin</Label>
                      <Select
                        value={importOrigin}
                        onValueChange={(value: OriginTag) => setImportOrigin(value)}
                        disabled={isImporting}
                      >
                        <SelectTrigger id="import-origin">
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="origin:paid">Paid</SelectItem>
                          <SelectItem value="origin:free">Free</SelectItem>
                          <SelectItem value="origin:unknown">Unknown</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>

                    {!isShareProtocol(importProtocol) && (
                      <div className="grid gap-2">
                        <Label htmlFor="import-username">Username (optional)</Label>
                        <Input
                          id="import-username"
                          value={importUsername}
                          onChange={(e) => setImportUsername(e.target.value)}
                          disabled={isImporting}
                          placeholder="Leave empty if not required"
                        />
                      </div>
                    )}

                    {!isShareProtocol(importProtocol) && (
                      <div className="grid gap-2">
                        <Label htmlFor="import-password">Password (optional)</Label>
                        <Input
                          id="import-password"
                          type="password"
                          value={importPassword}
                          onChange={(e) => setImportPassword(e.target.value)}
                          disabled={isImporting}
                          placeholder="Leave empty if not required"
                        />
                      </div>
                    )}

                    {isImporting && (
                      <div className="space-y-3">
                        <div className="flex items-center justify-between text-sm">
                          <span className="text-muted-foreground">
                            Progress: {importProgress.current} / {importProgress.total}
                          </span>
                          <div className="flex gap-3 text-muted-foreground">
                            <span>
                              <span className="text-green-600 font-medium">{importProgress.success}</span> success
                            </span>
                            {importProgress.skipped > 0 && (
                              <span>
                                <span className="text-yellow-600 font-medium">{importProgress.skipped}</span> skipped
                              </span>
                            )}
                            {importProgress.failed > 0 && (
                              <span>
                                <span className="text-red-600 font-medium">{importProgress.failed}</span> failed
                              </span>
                            )}
                          </div>
                        </div>
                        <div className="w-full bg-secondary rounded-full h-2.5">
                          <div
                            className="bg-primary h-2.5 rounded-full transition-all duration-300"
                            style={{
                              width: `${(importProgress.current / importProgress.total) * 100}%`
                            }}
                          />
                        </div>
                      </div>
                    )}

                    {importProgress.current === importProgress.total && importProgress.total > 0 && (
                      <div className="border rounded-lg p-4 space-y-3 bg-muted/30">
                        <div className="flex items-center justify-between">
                          <h4 className="font-medium">Import Complete</h4>
                          <div className="flex gap-4 text-sm">
                            <span className="flex items-center gap-1 text-green-600">
                              <CheckCircle2 className="h-4 w-4" />
                              {importProgress.success} successful
                            </span>
                            {importProgress.skipped > 0 && (
                              <span className="flex items-center gap-1 text-yellow-600">
                                <AlertCircle className="h-4 w-4" />
                                {importProgress.skipped} skipped
                              </span>
                            )}
                            {importProgress.failed > 0 && (
                              <span className="flex items-center gap-1 text-red-600">
                                <XCircle className="h-4 w-4" />
                                {importProgress.failed} failed
                              </span>
                            )}
                          </div>
                        </div>

                        {(importResults.filter(r => r.status === 'skipped').length > 0 ||
                          importResults.filter(r => r.status === 'failed').length > 0) && (
                          <div className="max-h-48 overflow-y-auto text-sm space-y-2">
                            {importResults.filter(r => r.status === 'skipped').length > 0 && (
                              <div>
                                <p className="font-medium text-yellow-600 mb-1">Skipped proxies (duplicates):</p>
                                <div className="space-y-0.5">
                                  {importResults
                                    .filter(r => r.status === 'skipped')
                                    .map((result, idx) => (
                                      <div key={idx} className="font-mono text-xs text-yellow-600/80">
                                        {result.address}
                                      </div>
                                    ))}
                                </div>
                              </div>
                            )}
                            {importResults.filter(r => r.status === 'failed').length > 0 && (
                              <div>
                                <p className="font-medium text-red-600 mb-1">Failed proxies:</p>
                                <div className="space-y-0.5">
                                  {importResults
                                    .filter(r => r.status === 'failed')
                                    .map((result, idx) => (
                                      <div key={idx} className="font-mono text-xs text-red-600">
                                        {result.address}: {result.error}
                                      </div>
                                    ))}
                                </div>
                              </div>
                            )}
                          </div>
                        )}
                      </div>
                    )}
              </>
            )}
          </div>

          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => {
                setIsImportDialogOpen(false)
                resetImportDialog()
              }}
              disabled={isImporting}
            >
              {importProgress.current === importProgress.total && importProgress.total > 0 ? 'Close' : 'Cancel'}
            </Button>
            {parsedProxies.length > 0 && (
              <Button
                onClick={handleImport}
                disabled={isImporting || (importProgress.current === importProgress.total && importProgress.total > 0)}
              >
                {isImporting ? (
                  <>
                    <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                    Importing...
                  </>
                ) : importProgress.current === importProgress.total && importProgress.total > 0 ? (
                  'Import Complete'
                ) : (
                  `Import ${parsedProxies.length} Proxies`
                )}
              </Button>
            )}
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Delete Confirmation Dialog */}
      <AlertDialog open={deleteConfirm.open} onOpenChange={(open) => setDeleteConfirm({ open, proxyId: null })}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Are you sure?</AlertDialogTitle>
            <AlertDialogDescription>
              This action cannot be undone. This will permanently delete the proxy.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction onClick={confirmDelete} className="bg-red-600 hover:bg-red-700">
              Delete
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      {/* Bulk Delete Confirmation Dialog */}
       <AlertDialog open={bulkDeleteConfirm} onOpenChange={setBulkDeleteConfirm}>
         <AlertDialogContent>
           <AlertDialogHeader>
             <AlertDialogTitle>Delete {selectedProxies.length} proxies?</AlertDialogTitle>
             <AlertDialogDescription>
               This action cannot be undone. This will permanently delete the selected proxies.
             </AlertDialogDescription>
           </AlertDialogHeader>
           <AlertDialogFooter>
             <AlertDialogCancel>Cancel</AlertDialogCancel>
             <AlertDialogAction onClick={confirmBulkDelete} className="bg-red-600 hover:bg-red-700">
               Delete
             </AlertDialogAction>
           </AlertDialogFooter>
         </AlertDialogContent>
       </AlertDialog>

       <AlertDialog open={deleteAllConfirm} onOpenChange={setDeleteAllConfirm}>
         <AlertDialogContent>
           <AlertDialogHeader>
             <AlertDialogTitle>Delete ALL proxies?</AlertDialogTitle>
             <AlertDialogDescription>
               This will permanently delete <strong>every proxy</strong> in the database,
               including those in pools. This action cannot be undone.
             </AlertDialogDescription>
           </AlertDialogHeader>
           <AlertDialogFooter>
             <AlertDialogCancel>Cancel</AlertDialogCancel>
             <AlertDialogAction onClick={confirmDeleteAll} className="bg-red-600 hover:bg-red-700">
               Delete All
             </AlertDialogAction>
           </AlertDialogFooter>
         </AlertDialogContent>
       </AlertDialog>
    </div>
  )
}
