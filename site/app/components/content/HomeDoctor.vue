<script setup lang="ts">
/**
 * The Day-2 AI Diagnostic Copilot showcase (bloodraven-doctor).
 * Highlights the npx skills installation, automated triage, and safe remediation.
 */
const props = defineProps<{
  kicker?: string
  title?: string
  accent?: string
  description?: string
  command?: string
  doctorTo?: string
}>()

const copied = ref(false)
const installCmd = computed(() => props.command || 'npx skills add shipstream/bloodraven')

async function copyCommand() {
  try {
    await navigator.clipboard.writeText(installCmd.value)
    copied.value = true
    setTimeout(() => (copied.value = false), 2000)
  }
  catch {
    // Clipboard permission denied fallback
  }
}
</script>

<template>
  <section class="doctor">
    <div class="br-shell doctor-shell">
      <div class="doctor-copy">
        <p v-if="kicker" class="br-kicker">
          {{ kicker }}
        </p>

        <h2 class="br-display doctor-title">
          {{ title }}
          <span class="br-outline doctor-accent">{{ accent }}</span>
        </h2>

        <p class="doctor-description">
          {{ description }}
        </p>

        <!-- Command Card with Copy Action -->
        <div class="install-card">
          <div class="install-top">
            <span class="install-label">Install for any AI agent</span>
            <span class="install-compat">Cursor · Claude Code · Antigravity · Windsurf · Roo</span>
          </div>
          <div class="install-bar">
            <code class="install-code">
              <span class="prompt">$</span>{{ installCmd }}
            </code>
            <button
              type="button"
              class="copy-btn br-focus"
              :aria-label="copied ? 'Copied' : 'Copy install command'"
              @click="copyCommand"
            >
              <UIcon :name="copied ? 'i-lucide-check' : 'i-lucide-copy'" class="copy-icon" />
              <span>{{ copied ? 'Copied' : 'Copy' }}</span>
            </button>
          </div>
        </div>

        <!-- Capability points -->
        <dl class="capabilities">
          <div class="cap-item">
            <dt>
              <UIcon name="i-lucide-activity" class="cap-icon" />
              <span>60-Second Fast Triage</span>
            </dt>
            <dd>Non-destructive assessment of quorum, active primaries, split-brain status, and replication lag.</dd>
          </div>
          <div class="cap-item">
            <dt>
              <UIcon name="i-lucide-git-branch" class="cap-icon" />
              <span>GTID Divergence Auditor</span>
            </dt>
            <dd>Compares executed transaction sets across sites to detect errant writes and guide safe recloning.</dd>
          </div>
          <div class="cap-item">
            <dt>
              <UIcon name="i-lucide-shield-alert" class="cap-icon" />
              <span>Keyring Escrow Safety</span>
            </dt>
            <dd>Enforces strict safety guards to protect master encryption keys from loss during pod restarts.</dd>
          </div>
          <div class="cap-item">
            <dt>
              <UIcon name="i-lucide-check-circle-2" class="cap-icon" />
              <span>Runbook-Grounded Plans</span>
            </dt>
            <dd>Produces step-by-step remediation commands with clear blast radius and RPO trade-offs.</dd>
          </div>
        </dl>
      </div>

      <!-- Live Terminal Simulation Preview -->
      <div class="terminal-card">
        <div class="terminal-bar">
          <div class="terminal-dots">
            <i class="dot red" /><i class="dot amber" /><i class="dot green" />
          </div>
          <span class="terminal-title">
            <UIcon name="i-lucide-stethoscope" class="term-icon" />
            <code>bloodraven-doctor · orders-production</code>
          </span>
          <span class="terminal-status"><i />active</span>
        </div>

        <div class="terminal-body">
          <div class="line prompt-line">
            <span class="term-prompt">agent&gt;</span>
            <span class="term-user-text">Run bloodraven-doctor on namespace orders</span>
          </div>

          <div class="line output-header">
            <span class="badge-doctor">🩺 Bloodraven Doctor</span>
            <span class="term-dim">Inspecting MySQLFailoverGroup: orders</span>
          </div>

          <div class="output-block">
            <div class="sub-line"><span class="t-key">Status:</span> <span class="t-warn">🟡 DEGRADED</span> (Replication Lag Detected)</div>
            <div class="sub-line"><span class="t-key">Active Primary:</span> <span class="t-cyan">iad</span> (Writable, super_read_only=OFF)</div>
            <div class="sub-line"><span class="t-key">Standby Site:</span>   <span class="t-cyan">pdx</span> (Replica, lag=48s, IO_Thread=ON)</div>
            <div class="sub-line"><span class="t-key">Keyring Status:</span> <span class="t-green">Sealed</span> (Digest: 8f3a... matched escrow)</div>
            <div class="sub-line"><span class="t-key">Dragonfly:</span>      <span class="t-green">Ready</span> (replTakeoverSupported=true)</div>
          </div>

          <div class="line root-cause-block">
            <div class="rc-title"><UIcon name="i-lucide-alert-triangle" class="rc-icon" /> Root Cause Analysis:</div>
            <div class="rc-text">High replication lag on standby `pdx` due to large transaction batch on primary. GTIDs are sequential; no errant transactions detected.</div>
          </div>

          <div class="line remediation-block">
            <div class="rem-title"><UIcon name="i-lucide-wrench" class="rem-icon" /> Recommended Safe Remediation:</div>
            <code class="rem-cmd">1. Monitor catchup: kubectl bloodraven status orders -n orders</code>
            <code class="rem-cmd">2. Preflight failover: kubectl bloodraven promote orders --to=pdx (blocked until lag=0s)</code>
            <div class="rem-risk">Blast Radius: <strong>Zero</strong> (Read-only standby catching up) · Data Loss Risk: <strong>None</strong></div>
          </div>
        </div>
      </div>
    </div>
  </section>
</template>

<style scoped>
.doctor {
  padding: 112px 0;
  border-top: 1px solid var(--br-line-light);
  color: var(--br-text);
  background: var(--br-paper);
}

.doctor-shell {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 56px;
  align-items: center;
}

.doctor-shell > * {
  min-width: 0;
}

.doctor-title {
  margin-top: 18px;
  line-height: 1.08;
}

.doctor-accent {
  display: block;
  color: var(--br-red);
}

.doctor-description {
  max-width: 520px;
  margin: 24px 0 0;
  color: var(--br-text-dim);
  font-size: 15.5px;
  line-height: 1.75;
  text-wrap: pretty;
}

/* Install Card ----------------------------------------------------------- */
.install-card {
  margin-top: 28px;
  padding: 16px;
  border: 1px solid var(--br-line-dark);
  border-radius: 8px;
  background: var(--br-ink-soft);
  color: var(--br-on-ink);
}

.install-top {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 10px;
  font-size: 11px;
}

.install-label {
  color: var(--br-red-bright);
  font-family: var(--br-mono);
  font-weight: 700;
  letter-spacing: 0.08em;
  text-transform: uppercase;
}

.install-compat {
  color: var(--br-on-ink-faint);
  font-family: var(--br-mono);
  font-size: 10.5px;
}

.install-bar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  padding: 8px 12px;
  border: 1px solid rgba(255, 255, 255, 0.08);
  border-radius: 6px;
  background: #070a0f;
}

.install-code {
  color: var(--br-cyan);
  font-family: var(--br-mono);
  font-size: 13.5px;
  font-weight: 600;
  overflow-x: auto;
  white-space: nowrap;
}

.install-code .prompt {
  margin-right: 8px;
  color: var(--br-on-ink-faint);
  user-select: none;
}

.copy-btn {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 6px 12px;
  border: 1px solid rgba(255, 255, 255, 0.14);
  border-radius: 4px;
  color: var(--br-on-ink);
  background: rgba(255, 255, 255, 0.06);
  font-family: var(--br-mono);
  font-size: 11.5px;
  font-weight: 600;
  cursor: pointer;
  transition: all 160ms ease;
  flex-shrink: 0;
}

.copy-btn:hover {
  background: rgba(225, 29, 72, 0.2);
  border-color: var(--br-red);
  color: white;
}

.copy-icon {
  width: 14px;
  height: 14px;
}

/* Capabilities Grid ----------------------------------------------------- */
.capabilities {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 20px 24px;
  margin-top: 32px;
}

.cap-item {
  padding-top: 14px;
  border-top: 1px solid var(--br-line-light);
}

.cap-item dt {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 13px;
  font-weight: 700;
  letter-spacing: -0.01em;
}

.cap-icon {
  width: 15px;
  height: 15px;
  color: var(--br-red);
  flex-shrink: 0;
}

.cap-item dd {
  margin: 6px 0 0;
  color: var(--br-text-dim);
  font-size: 12px;
  line-height: 1.55;
  text-wrap: pretty;
}

/* Terminal Card --------------------------------------------------------- */
.terminal-card {
  border: 1px solid var(--br-line-dark);
  border-radius: 8px;
  background: #090d14;
  color: var(--br-on-ink);
  overflow: hidden;
  box-shadow: 0 16px 40px rgba(0, 0, 0, 0.35);
}

.terminal-bar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 10px 16px;
  background: #0f141e;
  border-bottom: 1px solid rgba(255, 255, 255, 0.08);
}

.terminal-dots {
  display: flex;
  gap: 6px;
}

.terminal-dots .dot {
  width: 10px;
  height: 10px;
  border-radius: 50%;
}

.terminal-dots .dot.red { background: #ef4444; }
.terminal-dots .dot.amber { background: #f59e0b; }
.terminal-dots .dot.green { background: #10b981; }

.terminal-title {
  display: flex;
  align-items: center;
  gap: 6px;
  font-family: var(--br-mono);
  font-size: 11.5px;
  color: var(--br-on-ink-dim);
}

.term-icon {
  width: 13px;
  height: 13px;
  color: var(--br-red-bright);
}

.terminal-status {
  display: flex;
  align-items: center;
  gap: 6px;
  font-family: var(--br-mono);
  font-size: 10px;
  color: var(--br-green);
  text-transform: uppercase;
  letter-spacing: 0.08em;
}

.terminal-status i {
  width: 6px;
  height: 6px;
  border-radius: 50%;
  background: var(--br-green);
  box-shadow: 0 0 8px var(--br-green);
}

.terminal-body {
  padding: 20px;
  font-family: var(--br-mono);
  font-size: 12px;
  line-height: 1.6;
}

.prompt-line {
  margin-bottom: 14px;
  padding-bottom: 10px;
  border-bottom: 1px dashed rgba(255, 255, 255, 0.1);
}

.term-prompt {
  color: var(--br-red-bright);
  font-weight: 700;
  margin-right: 8px;
}

.term-user-text {
  color: #fff;
  font-weight: 500;
}

.output-header {
  display: flex;
  align-items: center;
  gap: 10px;
  margin-bottom: 12px;
}

.badge-doctor {
  padding: 2px 8px;
  border-radius: 4px;
  background: rgba(225, 29, 72, 0.2);
  border: 1px solid var(--br-red);
  color: #fff;
  font-weight: 700;
  font-size: 11px;
}

.term-dim {
  color: var(--br-on-ink-faint);
  font-size: 11.5px;
}

.output-block {
  padding: 10px 14px;
  background: rgba(255, 255, 255, 0.03);
  border-left: 2px solid var(--br-cyan);
  border-radius: 0 6px 6px 0;
  margin-bottom: 14px;
}

.t-key { color: var(--br-on-ink-dim); }
.t-cyan { color: var(--br-cyan); font-weight: 600; }
.t-warn { color: var(--br-amber); font-weight: 600; }
.t-green { color: var(--br-green); font-weight: 600; }

.root-cause-block {
  padding: 10px 12px;
  background: rgba(245, 158, 11, 0.08);
  border: 1px solid rgba(245, 158, 11, 0.2);
  border-radius: 6px;
  margin-bottom: 14px;
}

.rc-title {
  display: flex;
  align-items: center;
  gap: 6px;
  color: var(--br-amber);
  font-weight: 700;
  font-size: 11.5px;
  margin-bottom: 4px;
}

.rc-icon {
  width: 14px;
  height: 14px;
}

.rc-text {
  color: var(--br-on-ink);
  font-size: 11px;
  line-height: 1.5;
}

.remediation-block {
  padding: 10px 12px;
  background: rgba(34, 211, 238, 0.05);
  border: 1px solid rgba(34, 211, 238, 0.15);
  border-radius: 6px;
}

.rem-title {
  display: flex;
  align-items: center;
  gap: 6px;
  color: var(--br-cyan);
  font-weight: 700;
  font-size: 11.5px;
  margin-bottom: 6px;
}

.rem-icon {
  width: 14px;
  height: 14px;
}

.rem-cmd {
  display: block;
  color: #fff;
  font-size: 11px;
  margin-bottom: 4px;
}

.rem-risk {
  margin-top: 8px;
  padding-top: 6px;
  border-top: 1px solid rgba(255, 255, 255, 0.08);
  color: var(--br-on-ink-faint);
  font-size: 10.5px;
}

.rem-risk strong {
  color: var(--br-green);
}

@media (max-width: 960px) {
  .doctor-shell {
    grid-template-columns: 1fr;
    gap: 40px;
  }
}
</style>
