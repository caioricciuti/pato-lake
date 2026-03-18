# Privacy Policy

**Effective date:** February 12, 2026
**Last updated:** February 12, 2026

Patolake ("we", "our", "us") is developed by Caio Ricciuti. This privacy policy explains how we handle data when you use Patolake software.

---

## What Patolake does NOT collect

Patolake is a self-hosted application. When you run Patolake on your own infrastructure:

- **No telemetry** is sent to us or any third party
- **No usage data** leaves your server
- **No analytics** are collected
- **No cookies** are set by us (only session cookies for your own login)
- **Your queries, data, and database contents never leave your infrastructure**

## Data stored locally

Patolake stores the following data in a local SQLite database on your server:

- **User sessions** — login tokens for authenticated access
- **Saved queries** — queries you choose to save
- **Dashboard configurations** — layout and panel settings (Pro)
- **Scheduled jobs** — query schedules you create (Pro)
- **Connection settings** — DuckDB connection details (encrypted)
- **License information** — your license key if you activate Pro
- **Application settings** — preferences and configuration

All data is stored in the SQLite file specified by `database_path` in your config (default: `./data/patolake.db`). You have full control over this data.

## Pro license activation

When you activate a Pro license, the license file is stored locally in your database. No information is sent to external servers during activation — the license is validated offline using cryptographic signatures.

## Managed hosting

If you use a managed Patolake hosting offering:

- We may store your account information (email, name) for authentication
- We may store your connection metadata (not your database contents)
- We do not access, read, or store your DuckDB data
- Connections are end-to-end between your server and your browser session

## Third-party services

The self-hosted Patolake binary does not communicate with any third-party services except:

- **Your DuckDB instance** — as configured by you
- **OpenAI API** — only if you configure the Brain AI feature (Pro) with your own API key

## Data deletion

Since all data is stored locally:

- Delete the SQLite database file to remove all application data
- Uninstall the binary to fully remove Patolake

## Contact

For privacy questions: **c.ricciuti@patolake.com**

## Changes

We may update this policy. Changes will be posted in this file and noted in release changelogs.
