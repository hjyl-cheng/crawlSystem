# Public Edge

The bundled `deploy/compose.yml` Nginx listens on a host-only high port. A
shared host Nginx terminates public TLS and forwards requests to that port.

For the isolated newcrawler control plane:

- `newcrawler-dashboard.conf` exposes Dashboard, BullMQ, Rota, and MinIO on
  four authenticated hostnames backed by the same internal gateway.
- `renew-newcrawler-dashboard-certificate.sh` renews only the dedicated
  four-host `newcrawdashboard-nip-io` certificate and reloads Nginx after
  validation.
- `newcrawler-certbot-renew.service` and `.timer` run the renewal check daily.

Current host installation paths are:

```text
/opt/nginx-proxy/qy.conf
/opt/nginx-proxy/renew-newcrawler-dashboard-certificate.sh
/etc/systemd/system/newcrawler-certbot-renew.service
/etc/systemd/system/newcrawler-certbot-renew.timer
```

The shared host Nginx mounts `qy.conf` as a single file. Replacing that host
file with a new inode requires recreating only the edge Nginx container before
reload; an in-place update is visible immediately. Always run `nginx -t`
before reload and verify the existing QY endpoint after a change.

The `newcrawdashboard-nip-io` certificate covers all four newcrawler
hostnames. It is separate from `qy-admin-nip-io`; do not add newcrawler names
to or replace the existing QY certificate.
