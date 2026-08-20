#!/bin/sh
set -eu

CERTBOT_IMAGE=${CERTBOT_IMAGE:-certbot/certbot:latest}
CERTBOT_CONFIG_DIR=${CERTBOT_CONFIG_DIR:-/etc/letsencrypt}
CERTBOT_WEBROOT=${CERTBOT_WEBROOT:-/opt/nginx-proxy/certbot}
NGINX_CONTAINER=${NGINX_CONTAINER:-nginx-proxy-nginx-1}

/usr/bin/docker run --rm \
  -v "$CERTBOT_CONFIG_DIR:/etc/letsencrypt" \
  -v "$CERTBOT_WEBROOT:/var/www/certbot" \
  "$CERTBOT_IMAGE" renew \
  --cert-name newcrawdashboard-nip-io \
  --webroot \
  --webroot-path /var/www/certbot \
  --no-random-sleep-on-renew \
  --quiet

/usr/bin/docker exec "$NGINX_CONTAINER" nginx -t
/usr/bin/docker exec "$NGINX_CONTAINER" nginx -s reload
