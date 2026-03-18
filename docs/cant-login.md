# Can't Login?

Use this guide when Patolake loads but sign-in fails or you are blocked by retry windows.

## Quick Diagnosis

| What you see | Most likely cause | What to do |
|---|---|---|
| `Authentication failed` | Wrong username/password | Retry with correct credentials for the selected connection |
| `Connection unavailable` / `Unreachable` | DuckDB connection is misconfigured or offline | Check configuration, restart Patolake, then retry |
| `Too many login attempts` | Repeated failed attempts triggered temporary lock | Wait retry window; fix setup and restart before retrying |
| No connections configured | Embedded connection was not created/updated correctly | Run setup command below and restart Patolake |

## Local Recovery (Recommended)

1. Open **Can't login?** in Patolake login.
2. Follow the setup guidance.
3. Restart Patolake with one of these commands.

Global install:

```bash
patolake server
```

Local binary:

```bash
./patolake server
```

Then open `http://localhost:3488` and sign in again.

## Docker Recovery

```bash
docker run --rm \
  -p 3488:3488 \
  -v patolake-data:/app/data \
  ghcr.io/caioricciuti/pato-lake:latest
```

## Env And Config Alternatives

Environment variables:

```bash
DATABASE_PATH='./data/patolake.db' patolake server
```

Config file (`server.yaml`):

```yaml
database_path: ./data/patolake.db
```

## Notes

- Local URL setup does **not** require Admin access.
- Admin and multi-connection management are Pro-only features.
- Setup commands intentionally exclude passwords; credentials stay in the Sign in form.
