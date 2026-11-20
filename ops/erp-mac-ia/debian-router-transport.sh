#!/bin/sh
# Own transport only. Cron retries disconnected sessions; old ERP cron lines are preserved.
exec /usr/bin/flock -n "$HOME/.local/share/pastelerias/local-router/transport.lock" \
  /usr/bin/ssh -NT -F "$HOME/.ssh/pastelerias_local_router_vps.conf" pastelerias-local-router-vps
