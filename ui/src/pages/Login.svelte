<script lang="ts">
  import { login, getError } from "../lib/stores/session.svelte";
  import Button from "../lib/components/common/Button.svelte";
  import {
    Terminal,
    Layers,
    Zap,
    ArrowRight,
    AlertTriangle,
  } from "lucide-svelte";
  import logo from "../assets/logo.png";

  let username = $state("");
  let password = $state("");
  let submitting = $state(false);
  let localError = $state<string | null>(null);

  type LoginErrorKind = "auth" | "rateLimit" | "generic";

  type LoginHelp = {
    title: string;
    detail?: string;
  };

  function classifyLoginError(message: string | null): LoginErrorKind | null {
    if (!message) return null;
    const normalized = message.toLowerCase();
    if (normalized.includes("invalid credentials") || normalized.includes("authentication failed")) {
      return "auth";
    }
    if (normalized.includes("too many login attempts") || normalized.includes("retry in")) {
      return "rateLimit";
    }
    return "generic";
  }

  function buildLoginHelp(kind: LoginErrorKind | null): LoginHelp | null {
    if (!kind) return null;
    if (kind === "auth") {
      return {
        title: "Authentication failed",
        detail: "Verify your username and password.",
      };
    }
    if (kind === "rateLimit") {
      return {
        title: "Login temporarily blocked",
        detail: "Wait for the retry window and try again.",
      };
    }
    return { title: "Login failed" };
  }

  async function handleSubmit(e: SubmitEvent) {
    e.preventDefault();
    if (!username) {
      localError = "Username is required";
      return;
    }

    localError = null;
    submitting = true;
    try {
      await login(username, password);
    } catch (e: unknown) {
      localError = (e instanceof Error ? e.message : null) || "Login failed";
    } finally {
      submitting = false;
    }
  }

  const error = $derived(localError || getError());
  const canSubmit = $derived(Boolean(username));
  const errorKind = $derived(classifyLoginError(error));
  const loginHelp = $derived(buildLoginHelp(errorKind));
</script>

<div class="login-root">
  <!-- Left Panel -- Branding -->
  <div class="left-panel">
    <div class="left-panel-noise"></div>
    <div class="left-panel-grid"></div>

    <div class="left-content">
      <div class="logo-block">
        <img src={logo} alt="Patolake" class="logo-img" />
        <div class="logo-text">
          <span class="logo-name">Patolake</span>
          <span class="logo-version">v2.0</span>
        </div>
      </div>

      <h2 class="hero-title">
        Your DuckDB<br />
        <span class="hero-accent">command center.</span>
      </h2>

      <p class="hero-sub">
        Query, explore, and manage your DuckDB databases with a modern
        workspace built for speed.
      </p>

      <div class="features">
        <div class="feature-item">
          <div class="feature-icon">
            <Terminal size={16} />
          </div>
          <div>
            <span class="feature-label">SQL Editor</span>
            <span class="feature-desc"
              >Multi-tab query workspace with autocomplete</span
            >
          </div>
        </div>
        <div class="feature-item">
          <div class="feature-icon">
            <Layers size={16} />
          </div>
          <div>
            <span class="feature-label">Schema Explorer</span>
            <span class="feature-desc"
              >Browse databases, tables, and columns</span
            >
          </div>
        </div>
        <div class="feature-item">
          <div class="feature-icon">
            <Zap size={16} />
          </div>
          <div>
            <span class="feature-label">Performance</span>
            <span class="feature-desc"
              >Real-time metrics and query profiling</span
            >
          </div>
        </div>
      </div>

      <div class="left-footer">
        <span class="left-footer-text">Open Source</span>
        <span class="left-footer-dot"></span>
        <span class="left-footer-text">Self-Hosted</span>
        <span class="left-footer-dot"></span>
        <span class="left-footer-text">Apache License 2.0</span>
      </div>
    </div>
  </div>

  <!-- Right Panel -- Login Form -->
  <div class="right-panel">
    <div class="right-content">
      <div class="form-header">
        <h1 class="form-title">Sign in</h1>
        <p class="form-subtitle">Sign in to your workspace</p>
      </div>

      <form onsubmit={handleSubmit} class="login-form">
        <!-- Username -->
        <div class="field">
          <label class="field-label" for="username">Username</label>
          <input
            id="username"
            type="text"
            bind:value={username}
            placeholder="admin"
            autocomplete="username"
            class="field-input"
          />
        </div>

        <!-- Password -->
        <div class="field">
          <label class="field-label" for="password">Password</label>
          <input
            id="password"
            type="password"
            bind:value={password}
            placeholder="Password"
            autocomplete="current-password"
            class="field-input"
          />
        </div>

        {#if error}
          <div class="error-block">
            <div class="error-header">
              <AlertTriangle size={14} />
              <p class="error-title">{loginHelp?.title ?? "Login failed"}</p>
            </div>
            <p class="error-text">{error}</p>
            {#if loginHelp?.detail}
              <p class="error-help">{loginHelp.detail}</p>
            {/if}
          </div>
        {/if}

        <Button
          type="submit"
          loading={submitting}
          disabled={!canSubmit}
        >
          <span class="btn-inner">
            Sign in
            <ArrowRight size={14} />
          </span>
        </Button>
      </form>

      <div class="right-footer">
        <a
          href="https://github.com/caioricciuti/pato-lake"
          target="_blank"
          rel="noopener"
          class="footer-link"
        >
          GitHub
        </a>
        <span class="footer-sep">/</span>
        <a
          href="https://github.com/caioricciuti/pato-lake#readme"
          target="_blank"
          rel="noopener"
          class="footer-link"
        >
          Docs
        </a>
      </div>
    </div>
  </div>
</div>

<style>
  /* -- Root layout -- */
  .login-root {
    display: flex;
    min-height: 100vh;
    min-height: 100dvh;
    font-family:
      "DM Sans",
      "SF Pro Display",
      -apple-system,
      system-ui,
      sans-serif;
  }

  /* -- Left panel -- */
  .left-panel {
    position: relative;
    flex: 1 1 50%;
    display: none;
    background: linear-gradient(145deg, #0c1220 0%, #0f1a2e 40%, #132240 100%);
    overflow: hidden;
  }

  @media (min-width: 960px) {
    .left-panel {
      display: flex;
      align-items: center;
      justify-content: center;
    }
  }

  .left-panel-noise {
    position: absolute;
    inset: 0;
    background-image: url("data:image/svg+xml,%3Csvg viewBox='0 0 256 256' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='n'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.85' numOctaves='4' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23n)' opacity='0.03'/%3E%3C/svg%3E");
    background-size: 200px 200px;
    pointer-events: none;
    z-index: 1;
  }

  .left-panel-grid {
    position: absolute;
    inset: 0;
    background-image: linear-gradient(
        rgba(255, 255, 255, 0.015) 1px,
        transparent 1px
      ),
      linear-gradient(90deg, rgba(255, 255, 255, 0.015) 1px, transparent 1px);
    background-size: 48px 48px;
    pointer-events: none;
    z-index: 1;
  }

  .left-content {
    position: relative;
    z-index: 2;
    max-width: 420px;
    padding: 3rem 2.5rem;
  }

  /* Logo */
  .logo-block {
    display: flex;
    align-items: center;
    gap: 0.75rem;
    margin-bottom: 2.5rem;
  }

  .logo-img {
    width: 44px;
    height: 44px;
    border-radius: 12px;
    border: 1px solid rgba(255, 255, 255, 0.08);
  }

  .logo-text {
    display: flex;
    align-items: baseline;
    gap: 0.5rem;
  }

  .logo-name {
    font-size: 1.25rem;
    font-weight: 700;
    color: #f0f4f8;
    letter-spacing: -0.02em;
  }

  .logo-version {
    font-size: 0.65rem;
    font-weight: 500;
    color: rgba(255, 255, 255, 0.3);
    background: rgba(255, 255, 255, 0.05);
    padding: 0.15rem 0.4rem;
    border-radius: 4px;
    letter-spacing: 0.04em;
  }

  /* Hero text */
  .hero-title {
    font-size: 2rem;
    font-weight: 700;
    line-height: 1.2;
    color: #e2e8f0;
    letter-spacing: -0.03em;
    margin-bottom: 1rem;
  }

  .hero-accent {
    background: linear-gradient(135deg, #facc15 0%, #f59e0b 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
  }

  .hero-sub {
    font-size: 0.875rem;
    line-height: 1.6;
    color: rgba(226, 232, 240, 0.5);
    margin-bottom: 2.5rem;
    max-width: 340px;
  }

  /* Features list */
  .features {
    display: flex;
    flex-direction: column;
    gap: 1.25rem;
    margin-bottom: 3rem;
  }

  .feature-item {
    display: flex;
    align-items: flex-start;
    gap: 0.75rem;
  }

  .feature-icon {
    flex-shrink: 0;
    width: 32px;
    height: 32px;
    display: flex;
    align-items: center;
    justify-content: center;
    background: rgba(255, 255, 255, 0.04);
    border: 1px solid rgba(255, 255, 255, 0.06);
    border-radius: 8px;
    color: #facc15;
  }

  .feature-label {
    display: block;
    font-size: 0.8rem;
    font-weight: 600;
    color: #e2e8f0;
    letter-spacing: -0.01em;
    margin-bottom: 0.1rem;
  }

  .feature-desc {
    display: block;
    font-size: 0.72rem;
    color: rgba(226, 232, 240, 0.4);
    line-height: 1.4;
  }

  /* Left footer */
  .left-footer {
    display: flex;
    align-items: center;
    gap: 0.6rem;
  }

  .left-footer-text {
    font-size: 0.68rem;
    color: rgba(226, 232, 240, 0.3);
    letter-spacing: 0.03em;
    text-transform: uppercase;
    font-weight: 500;
  }

  .left-footer-dot {
    width: 3px;
    height: 3px;
    border-radius: 50%;
    background: rgba(226, 232, 240, 0.15);
  }

  /* -- Right panel -- */
  .right-panel {
    flex: 1 1 50%;
    display: flex;
    align-items: center;
    justify-content: center;
    background: #fafbfc;
    padding: 2rem 1.5rem;
  }

  :global(.dark) .right-panel {
    background: #0d1117;
  }

  .right-content {
    width: 100%;
    max-width: 380px;
  }

  /* Form header */
  .form-header {
    margin-bottom: 1.75rem;
  }

  .form-title {
    font-size: 1.5rem;
    font-weight: 700;
    color: #111827;
    letter-spacing: -0.03em;
    margin-bottom: 0.35rem;
  }

  :global(.dark) .form-title {
    color: #f0f4f8;
  }

  .form-subtitle {
    font-size: 0.8rem;
    color: #6b7280;
  }

  :global(.dark) .form-subtitle {
    color: #6b7280;
  }

  /* Form */
  .login-form {
    display: flex;
    flex-direction: column;
    gap: 1.25rem;
  }

  .field {
    display: flex;
    flex-direction: column;
  }

  .field-label {
    display: flex;
    align-items: center;
    gap: 0.35rem;
    font-size: 0.72rem;
    font-weight: 600;
    color: #6b7280;
    margin-bottom: 0.4rem;
    text-transform: uppercase;
    letter-spacing: 0.04em;
  }

  :global(.dark) .field-label {
    color: #6b7280;
  }

  .field-input {
    width: 100%;
    background: white;
    border: 1px solid #e5e7eb;
    border-radius: 10px;
    padding: 0.6rem 0.85rem;
    font-size: 0.85rem;
    color: #111827;
    transition:
      border-color 0.15s,
      box-shadow 0.15s;
    outline: none;
    font-family: inherit;
  }

  .field-input::placeholder {
    color: #c9cdd4;
  }

  .field-input:focus {
    border-color: #3b82f6;
    box-shadow: 0 0 0 3px rgba(59, 130, 246, 0.1);
  }

  :global(.dark) .field-input {
    background: rgba(255, 255, 255, 0.03);
    border-color: #2d3748;
    color: #e2e8f0;
  }

  :global(.dark) .field-input::placeholder {
    color: #4a5568;
  }

  :global(.dark) .field-input:focus {
    border-color: #3b82f6;
    box-shadow: 0 0 0 3px rgba(59, 130, 246, 0.15);
  }

  /* Error */
  .error-block {
    background: rgba(239, 68, 68, 0.06);
    border: 1px solid rgba(239, 68, 68, 0.15);
    border-radius: 8px;
    padding: 0.5rem 0.75rem;
  }

  .error-header {
    display: flex;
    align-items: center;
    gap: 0.35rem;
    margin-bottom: 0.3rem;
    color: #dc2626;
  }

  .error-title {
    font-size: 0.75rem;
    font-weight: 700;
    color: inherit;
  }

  .error-text {
    font-size: 0.78rem;
    color: #dc2626;
  }

  :global(.dark) .error-text {
    color: #f87171;
  }

  .error-help {
    margin-top: 0.35rem;
    font-size: 0.72rem;
    color: #b45309;
  }

  :global(.dark) .error-help {
    color: #fde047;
  }

  /* Button inner */
  .btn-inner {
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 0.4rem;
  }

  /* Right footer */
  .right-footer {
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 0.5rem;
    margin-top: 2rem;
    padding-top: 1.25rem;
    border-top: 1px solid #f0f0f0;
  }

  :global(.dark) .right-footer {
    border-top-color: rgba(255, 255, 255, 0.05);
  }

  .footer-link {
    font-size: 0.72rem;
    color: #9ca3af;
    text-decoration: none;
    transition: color 0.15s;
  }

  .footer-link:hover {
    color: #3b82f6;
  }

  .footer-sep {
    font-size: 0.65rem;
    color: #d1d5db;
  }

  :global(.dark) .footer-sep {
    color: #374151;
  }
</style>
