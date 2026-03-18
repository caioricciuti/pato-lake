<p align="center">
  <img src="ui/src/assets/logo.png" alt="Patolake Logo" width="88" />
</p>

<h1 align="center">Patolake</h1>

<p align="center">
  DuckDB workspace for teams that ship fast.<br/>
  Querying, governance, AI copilot, scheduling, and operations in one binary.
</p>

<p align="center">
  <a href="https://github.com/caioricciuti/pato-lake/releases"><img src="https://img.shields.io/github/v/release/caioricciuti/pato-lake?label=version" alt="Version" /></a>
  <a href="https://github.com/caioricciuti/pato-lake/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-blue" alt="License" /></a>
  <a href="https://github.com/caioricciuti/pato-lake/stargazers"><img src="https://img.shields.io/github/stars/caioricciuti/pato-lake" alt="Stars" /></a>
</p>

---

## Table Of Contents

- [What Is Patolake?](#what-is-patolake)
- [Why Teams Use Patolake](#why-teams-use-patolake)
- [Architecture](#architecture)
- [Feature Overview](#feature-overview)
- [Quick Start (Local)](#quick-start-local)
- [Quick Start (Docker)](#quick-start-docker)
- [Can't Login?](#cant-login)
- [CLI Reference](#cli-reference)
- [Configuration](#configuration)
- [Production Checklist](#production-checklist)
- [Governance, Brain, and Alerts](#governance-brain-and-alerts)
- [Troubleshooting](#troubleshooting)
- [Development](#development)
- [Upgrade](#upgrade)
- [Legal](#legal)
- [Contributing](#contributing)

---

## What Is Patolake?

Patolake is a self-hosted DuckDB workspace that runs as a single executable.

It includes:
- A multi-tab SQL editor and explorer
- Dashboards and scheduled jobs
- Governance and access visibility
- Brain (AI assistant with chat history and artifacts)
- Admin workflows for users, providers, alerts, and operations

No Docker requirement. No extra backend services required to start.

---

## Why Teams Use Patolake

- Fast setup: download binary, run, open browser
- Real operations: status, restart/stop, connector lifecycle, sync visibility
- DuckDB power: query local files, S3, Parquet, CSV, JSON via extensions
- Governance value: lineage, incidents, policies, access matrix
- AI that is practical: model/provider control, persisted chats, SQL-aware artifacts

---

## Architecture

```
Browser ↔ Go Server (:3488) + Embedded Frontend
               ↓
          SQLite (WAL mode) — app metadata, users, sessions
          DuckDB — query engine (persistent file or in-memory)
```

---

## Feature Overview

### Community (Apache 2.0)
- SQL editor + result views
- Database/table explorer
- Saved queries
- Local-first single-binary runtime

### Pro (license required)
- Dashboards and panel builder
- Schedules and execution history
- Brain (multi-chat, model/provider management, artifacts)
- Governance (metadata, access, incidents, policies, lineage)
- Admin panel and multi-connection operations
- Alerts (SMTP, Resend, Brevo) for policy/schedule/governance events

See: [`docs/license.md`](docs/license.md)

---

## Community vs Pro

| Capability | Community | Pro |
|---|:---:|:---:|
| SQL editor + explorer | Yes | Yes |
| Saved queries | Yes | Yes |
| Dashboards | - | Yes |
| Schedules | - | Yes |
| Brain (AI) | - | Yes |
| Governance + incidents + policies | - | Yes |
| Alerting channels/rules | - | Yes |
| Multi-connection admin workflows | - | Yes |

---

## Quick Start (Local)

### 1) Download

Linux (amd64):
```bash
curl -L -o patolake https://github.com/caioricciuti/pato-lake/releases/latest/download/patolake-linux-amd64
chmod +x patolake
```

Linux (arm64):
```bash
curl -L -o patolake https://github.com/caioricciuti/pato-lake/releases/latest/download/patolake-linux-arm64
chmod +x patolake
```

macOS (Apple Silicon):
```bash
curl -L -o patolake https://github.com/caioricciuti/pato-lake/releases/latest/download/patolake-darwin-arm64
chmod +x patolake
```

macOS (Intel):
```bash
curl -L -o patolake https://github.com/caioricciuti/pato-lake/releases/latest/download/patolake-darwin-amd64
chmod +x patolake
```

### Optional: verify checksum

```bash
curl -L -o checksums.txt https://github.com/caioricciuti/pato-lake/releases/latest/download/checksums.txt
sha256sum -c checksums.txt --ignore-missing
```

### 2) Start server

Install globally and run with `patolake`:

```bash
sudo install -m 755 patolake /usr/local/bin/patolake
patolake
```

If you prefer not to install globally, run `./patolake` from the download folder.

Default address: `http://localhost:3488`

### 3) Log in

Use the default credentials or the ones configured for your DuckDB instance.
For local setup, Patolake starts with an embedded DuckDB database.

---

## Quick Start (Docker)

Run the official image:

```bash
docker run --rm \
  -p 3488:3488 \
  -v patolake-data:/app/data \
  ghcr.io/caioricciuti/pato-lake:latest
```

Notes:
- Persisted state is stored in `/app/data/patolake.db` (volume: `patolake-data`).

---

## Can't login?

Use this path when login fails and you need fast recovery from the login screen.

1. In login, click **Can't login?**.
2. Follow the setup guidance to configure your connection.
3. Restart Patolake:

```bash
patolake server
```

Or local binary:

```bash
./patolake server
```

Docker:

```bash
docker run --rm -p 3488:3488 -v patolake-data:/app/data \
  ghcr.io/caioricciuti/pato-lake:latest
```

Quick diagnosis:
- `Authentication failed`: wrong credentials for selected connection.
- `Connection unavailable` / `Unreachable`: connection offline or misconfigured.
- `Too many login attempts`: wait retry window; fix setup and restart first.

Full guide: [`docs/cant-login.md`](docs/cant-login.md)

---

## CLI Reference

### If you are new, run these first

Local machine (fastest way):
```bash
patolake
```

### Full command map

Top-level commands:
```bash
patolake
patolake server
patolake service
patolake update
patolake version
patolake completion
patolake help
```

### `server` (run Patolake web app/API)

```bash
patolake server
patolake server start --detach
patolake server status
patolake server stop
patolake server restart
```

Common flags:
- `--port, -p` HTTP port (default `3488`)
- `--config, -c` path to `server.yaml`
- `--detach` run in background
- `--pid-file` PID file location
- `--stop-timeout` graceful stop timeout
- `--dev` development mode (frontend proxy)

### `service` (install as OS service)

```bash
patolake service install
patolake service status
patolake service start
patolake service stop
patolake service restart
patolake service logs -f
patolake service uninstall
```

### Other commands

```bash
patolake update            # update binary to latest release
patolake version           # print version
patolake completion bash   # generate shell completion
patolake help              # show help
```

---

## Configuration

Good news: Patolake works without config files.

You only need config files when:
- you want production defaults
- you want service-managed startup
- you want to avoid passing flags every time

### Where config files live

- Server config (`server.yaml`)
- macOS: `~/.config/patolake/server.yaml`
- Linux: `/etc/patolake/server.yaml`

### How values are chosen (priority)

Server:
- CLI flags > environment variables > `server.yaml` > built-in defaults

### Server config (`server.yaml`) explained

```yaml
port: 3488
app_url: https://patolake.yourcompany.com
database_path: /var/lib/patolake/patolake.db
app_secret_key: "change-this-in-production"
allowed_origins:
  - https://patolake.yourcompany.com
```

What each key means:

| Key | Example | Default | Why it matters |
|---|---|---|---|
| `port` | `3488` | `3488` | HTTP port used by Patolake server |
| `app_url` | `https://patolake.example.com` | `http://localhost:<port>` | Public URL for links |
| `database_path` | `/var/lib/patolake/patolake.db` | `./data/patolake.db` | Where Patolake stores app state |
| `app_secret_key` | random long string | auto-generated per install (persisted at `<database_dir>/.app_secret_key`) | Encrypts session credentials; set explicitly in production |
| `allowed_origins` | `["https://patolake.example.com"]` | empty | CORS allowlist |

Server environment variables:
- `PORT`
- `APP_URL`
- `DATABASE_PATH`
- `APP_SECRET_KEY`
- `ALLOWED_ORIGINS` (comma-separated)

If `APP_SECRET_KEY` is not configured, Patolake generates a strong local key and persists it next to the database path as `.app_secret_key`.

### Minimal production template (copy/paste)

Server (`/etc/patolake/server.yaml`):
```yaml
port: 3488
app_url: https://patolake.example.com
database_path: /var/lib/patolake/patolake.db
app_secret_key: "replace-with-a-long-random-secret"
allowed_origins:
  - https://patolake.example.com
```

---

## Production Checklist

- Set a strong `APP_SECRET_KEY`
- Set `APP_URL` to your public HTTPS URL
- Configure `ALLOWED_ORIGINS`
- Put Patolake behind TLS reverse proxy
- Back up SQLite database (`database_path`)

### Backup and restore

Patolake state is stored in SQLite (`database_path`), so backup is simple:

```bash
cp /var/lib/patolake/patolake.db /var/backups/patolake-$(date +%F).db
```

Restore by replacing the DB file while server is stopped.

---

## Governance, Brain, and Alerts

### Governance
- Metadata sync (databases/tables/columns)
- Query log + lineage ingestion
- Access sync (users/roles/grants/matrix)
- Policies and incidents workflow

### Brain
- Multiple chats per user/connection
- Provider layer (OpenAI, OpenAI-compatible, Ollama)
- Admin-controlled provider/model activation and defaults
- Artifacts persisted in database

### Alerts
- Channel providers: SMTP, Resend, Brevo
- Rules by event type/severity
- Route-level delivery and escalation options

---

## Troubleshooting

### `listen tcp :3488: bind: address already in use`
Another process is already using the port.

Check:
```bash
patolake server status
```
Then stop old process:
```bash
patolake server stop
```

If status says PID file is missing but port is in use, you likely upgraded from an older binary without PID management. Stop the old process once, then restart with current build.

### Login fails but no clarity
Patolake now surfaces explicit login states (invalid credentials, offline connection, retry window). Verify selected connection is online.

### Health check
```bash
curl http://localhost:3488/health
```

---

## Development

Requirements:
- Go 1.24+
- Bun

```bash
git clone https://github.com/caioricciuti/pato-lake.git
cd patolake
make build
patolake
```

Dev mode:
```bash
make dev
# in another terminal
cd ui && bun install && bun run dev
```

Useful targets:
- `make build`
- `make build-frontend`
- `make build-go`
- `make test`
- `make vet`
- `make clean`

---

## Upgrade

```bash
patolake update
```

The updater downloads the latest release asset for your OS/arch, verifies checksum when available, and replaces the running binary on disk.

---

## Legal

- Core license: [`LICENSE`](LICENSE)
- Patolake licensing details: [`docs/license.md`](docs/license.md)
- Terms: [`docs/legal/terms-of-service.md`](docs/legal/terms-of-service.md)
- Privacy: [`docs/legal/privacy-policy.md`](docs/legal/privacy-policy.md)

---

## Contributing

Issues and PRs are welcome.

If you are contributing features, include:
- reproduction steps
- expected behavior
- migration notes (if schema/API changed)
- screenshots for UI changes
