# Patolake Production Runbook

This runbook covers a production deployment of Patolake.

## 1. Server Hardening

1. Create server config at `/etc/patolake/server.yaml`:

```yaml
port: 3488
app_url: https://patolake.example.com
app_secret_key: "replace-with-long-random-secret"
allowed_origins:
  - https://patolake.example.com
database_path: /var/lib/patolake/patolake.db
```

2. Keep runtime state in writable directories:

```bash
sudo mkdir -p /var/lib/patolake/run
sudo mkdir -p /var/lib/patolake
sudo chown -R patolake:patolake /var/lib/patolake
```

3. Use lifecycle commands with explicit PID file:

```bash
patolake server start -c /etc/patolake/server.yaml --detach --pid-file /var/lib/patolake/run/patolake-server.pid
patolake server status -c /etc/patolake/server.yaml --pid-file /var/lib/patolake/run/patolake-server.pid
patolake server stop -c /etc/patolake/server.yaml --pid-file /var/lib/patolake/run/patolake-server.pid
```

## 2. systemd Service (recommended)

Create `/etc/systemd/system/patolake-server.service`:

```ini
[Unit]
Description=Patolake Server
After=network.target

[Service]
Type=simple
User=patolake
Group=patolake
WorkingDirectory=/var/lib/patolake
ExecStart=/usr/local/bin/patolake server start -c /etc/patolake/server.yaml --pid-file /var/lib/patolake/run/patolake-server.pid
ExecStop=/usr/local/bin/patolake server stop -c /etc/patolake/server.yaml --pid-file /var/lib/patolake/run/patolake-server.pid
Restart=always
RestartSec=5
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
```

Then:

```bash
sudo systemctl daemon-reload
sudo systemctl enable patolake-server
sudo systemctl start patolake-server
sudo systemctl status patolake-server
```

## 3. Reverse Proxy (TLS)

Your proxy must:

- route app traffic to `127.0.0.1:3488`
- keep long-enough timeouts

## 4. Network Policy

- Inbound: `443` (or your TLS port)
- Inbound: `3488` only from localhost/reverse-proxy path

## 5. Monitoring and Backups

1. Health endpoint:

```bash
curl -fsS http://127.0.0.1:3488/health
```

2. Back up SQLite:

- file: `/var/lib/patolake/patolake.db`
- schedule daily snapshot + retention policy
- verify restore procedure quarterly

3. Log collection:

- `journalctl -u patolake-server`

## 6. Upgrade Procedure

1. Replace binary.
2. Restart service:

```bash
sudo systemctl restart patolake-server
```

3. Validate:

```bash
patolake version
patolake server status -c /etc/patolake/server.yaml --pid-file /var/lib/patolake/run/patolake-server.pid
```

## 7. Notes on Older Binaries

Older builds did not support server lifecycle subcommands (`status/stop/restart`).
If `patolake server status` starts the server, replace the binary with a newer build and retry.
