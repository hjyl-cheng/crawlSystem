# QY Rota

Rota owns proxy inventory, health evidence, lifecycle state, reserve selection,
worker slots, and atomic route replacement for the QY crawler.

## Layout

```text
core/       Go control API and authenticated forward proxy
dashboard/  Next.js proxy management interface
```

The portable repository includes source and an empty-database deployment only.
It does not include production proxy rows, credentials, source subscriptions,
or historical health data. Configure proxy sources after deployment.

## Worker Interface

Workers claim a stable slot through the Proxy Control interface. Rota may
replace the proxy behind that slot atomically after a reportable route failure.
Workers retain YouTube.js, yt-dlp, cookies, visitor data, locale, timezone, and
job retry state; Rota never owns crawler parsing or business job state.

Rota owns the identity-policy catalog. It is embedded in the Rota binary and
copied from that same source file into the qybullmq image at build time, so the
two runtimes cannot drift between different source copies.

## Source Ownership

Rota is derived from the Apache-2.0 `alpkeskin/rota` project and contains QY
proxy-control, lifecycle, source-inventory, and maintenance extensions. See
`LICENSE`, `AUTHORS`, and `core/THIRD_PARTY_NOTICES.md`.

Run the core tests with:

```bash
cd services/rota/core
go test ./...
```
