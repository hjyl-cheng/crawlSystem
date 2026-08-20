# Public Edge

The bundled `deploy/compose.yml` Nginx listens on a host-only high port. A
shared host Nginx terminates public TLS and forwards requests to that port.

For the isolated newcrawler Dashboard:

- `newcrawler-dashboard.conf` is the public Nginx server fragment.
- `renew-newcrawler-dashboard-certificate.sh` renews only the dedicated
  `newcrawdashboard-nip-io` certificate and reloads Nginx after validation.
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

The newcrawler certificate is separate from `qy-admin-nip-io`. Do not add the
new hostname to or replace the existing QY certificate.
