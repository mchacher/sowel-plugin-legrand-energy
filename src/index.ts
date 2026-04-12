/**
 * Sowel Plugin: Legrand Energy
 *
 * Monitors energy consumption and solar production via Legrand NLPC meters.
 * Discovers NLPC modules via homesdata, polls power via homestatus,
 * and fetches 30-min energy windows via getMeasure (bridge-level queries).
 *
 * Production device is configured via `production_device_name` setting.
 */

import * as fs from "node:fs";
import * as path from "node:path";

// ============================================================
// Local type definitions
// ============================================================

interface Logger {
  child(bindings: Record<string, unknown>): Logger;
  info(obj: Record<string, unknown>, msg: string): void;
  info(msg: string): void;
  warn(obj: Record<string, unknown>, msg: string): void;
  warn(msg: string): void;
  error(obj: Record<string, unknown>, msg: string): void;
  error(msg: string): void;
  debug(obj: Record<string, unknown>, msg: string): void;
  debug(msg: string): void;
}

interface EventBus {
  emit(event: unknown): void;
}

interface SettingsManager {
  get(key: string): string | undefined;
  set(key: string, value: string): void;
}

interface DiscoveredDevice {
  friendlyName: string;
  manufacturer?: string;
  model?: string;
  data: { key: string; type: string; category: string; unit?: string }[];
  orders: {
    key: string; type: string; dispatchConfig: Record<string, unknown>;
    min?: number; max?: number; enumValues?: string[]; unit?: string;
  }[];
}

interface DeviceManager {
  upsertFromDiscovery(integrationId: string, source: string, discovered: DiscoveredDevice): void;
  updateDeviceData(integrationId: string, sourceDeviceId: string, payload: Record<string, unknown>, sourceTimestamp?: number): void;
  removeStaleDevices(integrationId: string, activeIds: Set<string>): void;
  migrateIntegrationId(oldIntegrationId: string, newIntegrationId: string, models?: string[]): number;
}

interface Device {
  id: string;
  integrationId: string;
  sourceDeviceId: string;
  name: string;
}

interface PluginDeps {
  logger: Logger;
  eventBus: EventBus;
  settingsManager: SettingsManager;
  deviceManager: DeviceManager;
  pluginDir: string;
}

type IntegrationStatus = "connected" | "disconnected" | "not_configured" | "error";

interface IntegrationSettingDef {
  key: string;
  label: string;
  type: "text" | "password" | "number" | "boolean";
  required: boolean;
  placeholder?: string;
  defaultValue?: string;
}

interface IntegrationPlugin {
  readonly id: string;
  readonly name: string;
  readonly description: string;
  readonly icon: string;
  getStatus(): IntegrationStatus;
  isConfigured(): boolean;
  getSettingsSchema(): IntegrationSettingDef[];
  start(options?: { pollOffset?: number }): Promise<void>;
  stop(): Promise<void>;
  executeOrder(device: Device, dispatchConfig: Record<string, unknown>, value: unknown): Promise<void>;
  refresh?(): Promise<void>;
  getPollingInfo?(): { lastPollAt: string; intervalMs: number } | null;
}

// ============================================================
// Netatmo API types
// ============================================================

interface NetatmoTokenResponse {
  access_token: string;
  refresh_token: string;
  expires_in: number;
}

interface NetatmoHome {
  id: string;
  name: string;
  modules: { id: string; type: string; name: string; bridge?: string }[];
}

interface NetatmoModuleStatus {
  id: string;
  type: string;
  power?: number;
  sum_energy_elec?: number;
  reachable?: boolean;
}

// ============================================================
// Constants
// ============================================================

const INTEGRATION_ID = "legrand_energy";
const LEGACY_INTEGRATION_ID = "netatmo_hc";
const SETTINGS_PREFIX = `integration.${INTEGRATION_ID}.`;
const BASE_URL = "https://api.netatmo.com";
const REQUEST_TIMEOUT_MS = 30_000;
const REFRESH_MARGIN_S = 300;
const DEFAULT_POLL_INTERVAL_MS = 300_000;
const ENERGY_LOOKBACK_S = 12 * 3600;
const HALF_HOUR = 1800;

// ============================================================
// NLPC device mapping
// ============================================================

function mapMeterToDiscovered(mod: { id: string; type: string; name: string; bridge?: string }, homeId: string): DiscoveredDevice {
  return {
    friendlyName: mod.name || mod.id,
    manufacturer: "Legrand",
    model: mod.type,
    data: [
      { key: "power", type: "number", category: "power", unit: "W" },
      { key: "energy", type: "number", category: "energy", unit: "Wh" },
      { key: "autoconso", type: "number", category: "energy", unit: "Wh" },
      { key: "injection", type: "number", category: "energy", unit: "Wh" },
      { key: "demand_30min", type: "number", category: "power", unit: "W" },
    ],
    orders: [],
  };
}

// ============================================================
// OAuth Bridge
// ============================================================

class NetatmoBridge {
  private logger: Logger;
  private clientId: string;
  private clientSecret: string;
  private accessToken: string | null = null;
  private refreshToken: string;
  private tokenExpiresAt = 0;
  private refreshTimer: ReturnType<typeof setTimeout> | null = null;
  private tokenFilePath: string;
  private onRefreshTokenUpdated: ((newToken: string) => void) | null = null;

  constructor(
    clientId: string, clientSecret: string, refreshToken: string,
    logger: Logger, tokenFilePath: string,
    onRefreshTokenUpdated?: (newToken: string) => void,
  ) {
    this.logger = logger;
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.refreshToken = refreshToken;
    this.tokenFilePath = tokenFilePath;
    this.onRefreshTokenUpdated = onRefreshTokenUpdated ?? null;
    this.loadTokensFromFile();
  }

  async authenticate(): Promise<void> {
    await this.doRefreshToken();
    this.scheduleRefresh();
  }

  disconnect(): void {
    if (this.refreshTimer) { clearTimeout(this.refreshTimer); this.refreshTimer = null; }
    this.accessToken = null;
  }

  async getHomesData(): Promise<{ body: { homes: NetatmoHome[] }; status: string }> {
    return this.apiGet("/api/homesdata");
  }

  async getHomeStatus(homeId: string): Promise<{ body: { home: { id: string; modules: NetatmoModuleStatus[] } } }> {
    return this.apiGet(`/api/homestatus?home_id=${encodeURIComponent(homeId)}`);
  }

  async getMeasure(
    deviceId: string, moduleId: string, type: string, scale: string,
    dateBegin?: number, dateEnd?: number,
  ): Promise<{ body: Record<string, (number | null)[]> }> {
    const params = new URLSearchParams({ device_id: deviceId, module_id: moduleId, type, scale, optimize: "false" });
    if (dateBegin !== undefined) params.set("date_begin", String(dateBegin));
    if (dateEnd !== undefined) params.set("date_end", String(dateEnd));
    return this.apiGet(`/api/getmeasure?${params.toString()}`);
  }

  private async doRefreshToken(): Promise<void> {
    const body = new URLSearchParams({
      grant_type: "refresh_token", refresh_token: this.refreshToken,
      client_id: this.clientId, client_secret: this.clientSecret,
    });
    const res = await this.rawFetch(`${BASE_URL}/oauth2/token`, {
      method: "POST", headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: body.toString(),
    });
    if (!res.ok) { const text = await res.text(); throw new Error(`Token refresh failed (${res.status}): ${text}`); }
    const data = (await res.json()) as NetatmoTokenResponse;
    this.accessToken = data.access_token;
    this.refreshToken = data.refresh_token;
    this.tokenExpiresAt = Date.now() + data.expires_in * 1000;
    this.saveTokensToFile();
    if (this.onRefreshTokenUpdated) this.onRefreshTokenUpdated(data.refresh_token);
    this.logger.info({ expiresIn: data.expires_in }, "Access token refreshed");
  }

  private scheduleRefresh(): void {
    if (this.refreshTimer) clearTimeout(this.refreshTimer);
    const msUntilRefresh = Math.max(this.tokenExpiresAt - Date.now() - REFRESH_MARGIN_S * 1000, 60_000);
    this.refreshTimer = setTimeout(async () => {
      try { await this.doRefreshToken(); this.scheduleRefresh(); } catch (err) {
        this.logger.warn({ err } as Record<string, unknown>, "Token refresh failed, retrying in 30s");
        this.refreshTimer = setTimeout(async () => {
          try { await this.doRefreshToken(); this.scheduleRefresh(); } catch (e) {
            this.logger.error({ err: e } as Record<string, unknown>, "Token refresh retry failed");
          }
        }, 30_000);
      }
    }, msUntilRefresh);
  }

  private loadTokensFromFile(): void {
    try {
      if (fs.existsSync(this.tokenFilePath)) {
        const saved = JSON.parse(fs.readFileSync(this.tokenFilePath, "utf-8")) as { refreshToken?: string; accessToken?: string; expiresAt?: number };
        if (saved.refreshToken) this.refreshToken = saved.refreshToken;
        if (saved.accessToken && saved.expiresAt && saved.expiresAt > Date.now()) {
          this.accessToken = saved.accessToken; this.tokenExpiresAt = saved.expiresAt;
        }
      }
    } catch { /* no saved tokens */ }
  }

  private saveTokensToFile(): void {
    try {
      const dir = path.dirname(this.tokenFilePath);
      if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
      fs.writeFileSync(this.tokenFilePath, JSON.stringify({
        refreshToken: this.refreshToken, accessToken: this.accessToken, expiresAt: this.tokenExpiresAt,
      }));
    } catch (err) { this.logger.error({ err } as Record<string, unknown>, "Failed to persist tokens"); }
  }

  private async apiGet<T>(endpoint: string): Promise<T> {
    if (this.accessToken && this.tokenExpiresAt > 0 && Date.now() > this.tokenExpiresAt - 60_000) {
      await this.doRefreshToken();
    }
    const res = await this.rawFetch(`${BASE_URL}${endpoint}`, {
      method: "GET", headers: { Authorization: `Bearer ${this.accessToken}` },
    });
    if (!res.ok) { const text = await res.text(); throw new Error(`API ${endpoint} failed (${res.status}): ${text}`); }
    return (await res.json()) as T;
  }

  private async rawFetch(url: string, init: RequestInit): Promise<Response> {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
    try { return await fetch(url, { ...init, signal: controller.signal }); }
    finally { clearTimeout(timeout); }
  }
}

// ============================================================
// Plugin implementation
// ============================================================

class LegrandEnergyPlugin implements IntegrationPlugin {
  readonly id = INTEGRATION_ID;
  readonly name = "Legrand Energy";
  readonly description = "Legrand energy monitoring — NLPC meters, consumption + solar production";
  readonly icon = "Zap";

  private logger: Logger;
  private eventBus: EventBus;
  private settingsManager: SettingsManager;
  private deviceManager: DeviceManager;
  private bridge: NetatmoBridge | null = null;
  private status: IntegrationStatus = "disconnected";
  private homeId = "";
  private pollIntervalMs = DEFAULT_POLL_INTERVAL_MS;
  private pollInterval: ReturnType<typeof setInterval> | null = null;
  private lastPollAt: string | null = null;
  private polling = false;
  private pollFailed = false;
  private retryTimeout: ReturnType<typeof setTimeout> | null = null;
  private retryCount = 0;
  private migrationDone = false;
  private dataDir: string;

  // Meter tracking
  private moduleNames = new Map<string, string>();
  private bridgeId: string | null = null;
  private mainMeterName: string | null = null;
  private lastEmittedWindowTs = 0;

  constructor(deps: PluginDeps) {
    this.logger = deps.logger;
    this.eventBus = deps.eventBus;
    this.settingsManager = deps.settingsManager;
    this.deviceManager = deps.deviceManager;
    this.dataDir = path.resolve(deps.pluginDir, "..", "..", "data");
  }

  getStatus(): IntegrationStatus {
    if (!this.isConfigured()) return "not_configured";
    if (this.status === "connected" && this.pollFailed) return "error";
    return this.status;
  }

  isConfigured(): boolean {
    return (
      this.getSetting("client_id") !== undefined &&
      this.getSetting("client_secret") !== undefined &&
      this.getSetting("refresh_token") !== undefined
    );
  }

  getSettingsSchema(): IntegrationSettingDef[] {
    return [
      { key: "client_id", label: "Client ID", type: "text", required: true, placeholder: "From dev.netatmo.com" },
      { key: "client_secret", label: "Client Secret", type: "password", required: true },
      { key: "refresh_token", label: "Refresh Token", type: "password", required: true, placeholder: "From Netatmo Token Generator" },
      { key: "polling_interval", label: "Polling interval (seconds)", type: "number", required: false, defaultValue: "300", placeholder: "Min 60, default 300" },
      { key: "production_device_name", label: "Production device name", type: "text", required: false, placeholder: "e.g. Solaire" },
    ];
  }

  async start(options?: { pollOffset?: number }): Promise<void> {
    this.stopPolling();
    if (this.bridge) { this.bridge.disconnect(); this.bridge = null; }

    if (!this.isConfigured()) { this.status = "not_configured"; return; }

    // Migrate NLPC devices BEFORE auth (DB-only)
    if (!this.migrationDone) {
      this.migrateFromLegacy();
      this.migrationDone = true;
    }

    const clientId = this.getSetting("client_id")!;
    const clientSecret = this.getSetting("client_secret")!;
    const refreshToken = this.getSetting("refresh_token")!;
    const pollingIntervalSec = parseInt(this.getSetting("polling_interval") ?? "300", 10);
    this.pollIntervalMs = (isNaN(pollingIntervalSec) ? 300 : Math.max(pollingIntervalSec, 60)) * 1000;

    try {
      this.bridge = new NetatmoBridge(
        clientId, clientSecret, refreshToken, this.logger,
        path.join(this.dataDir, "legrand-energy-tokens.json"),
        (newToken) => { this.settingsManager.set(`${SETTINGS_PREFIX}refresh_token`, newToken); },
      );

      await this.bridge.authenticate();

      // Discover home ID
      const homesData = await this.bridge.getHomesData();
      const homes = homesData.body.homes;
      if (homes.length === 0) throw new Error("No homes found");
      this.homeId = homes[0].id;
      this.logger.info({ homeId: this.homeId, homeName: homes[0].name }, "Using Legrand home");

      // First poll
      await this.poll();

      // Schedule periodic polling
      const offset = options?.pollOffset ?? 0;
      const startInterval = () => {
        this.pollInterval = setInterval(() => this.safePoll(), this.pollIntervalMs);
      };
      if (offset > 0) { setTimeout(startInterval, offset); } else { startInterval(); }

      this.status = "connected";
      this.retryCount = 0;
      this.eventBus.emit({ type: "system.integration.connected", integrationId: this.id });
      this.logger.info({ pollIntervalMs: this.pollIntervalMs }, "Legrand Energy started");
    } catch (err) {
      this.status = "error";
      this.logger.error({ err } as Record<string, unknown>, "Failed to start Legrand Energy");
      this.scheduleRetry();
    }
  }

  async stop(): Promise<void> {
    this.cancelRetry();
    this.stopPolling();
    if (this.bridge) { this.bridge.disconnect(); this.bridge = null; }
    this.status = "disconnected";
    this.eventBus.emit({ type: "system.integration.disconnected", integrationId: this.id });
    this.logger.info("Legrand Energy stopped");
  }

  async executeOrder(_device: Device, _dispatchConfig: Record<string, unknown>, _value: unknown): Promise<void> {
    throw new Error("Legrand Energy does not support orders");
  }

  async refresh(): Promise<void> {
    if (!this.bridge || this.status !== "connected") throw new Error("Not connected");
    await this.poll();
  }

  getPollingInfo(): { lastPollAt: string; intervalMs: number } | null {
    if (!this.lastPollAt) return null;
    return { lastPollAt: this.lastPollAt, intervalMs: this.pollIntervalMs };
  }

  // ============================================================
  // Polling
  // ============================================================

  private async poll(): Promise<void> {
    if (this.polling || !this.bridge) return;
    this.polling = true;

    try {
      this.lastPollAt = new Date().toISOString();

      // Phase 1: discover NLPC meters + capture bridgeId
      await this.discoverMeters();

      // Phase 2: poll status for NLPC meters (power readings)
      await this.pollMeterStatus().catch((err) =>
        this.logger.warn({ err } as Record<string, unknown>, "Meter status poll failed"),
      );

      // Phase 3: poll bridge-level energy data (30-min windows)
      await this.pollEnergy().catch((err) =>
        this.logger.warn({ err } as Record<string, unknown>, "Energy poll failed"),
      );

      this.logger.info({}, "Energy poll complete");

      if (this.pollFailed) {
        this.pollFailed = false;
        this.eventBus.emit({
          type: "system.alarm.resolved",
          alarmId: `poll-fail:${INTEGRATION_ID}`,
          source: "Legrand Energy",
          message: "Communication rétablie",
        });
      }
    } catch (err) {
      this.logger.error({ err } as Record<string, unknown>, "Energy poll cycle failed");
      if (!this.pollFailed) {
        this.pollFailed = true;
        this.eventBus.emit({
          type: "system.alarm.raised",
          alarmId: `poll-fail:${INTEGRATION_ID}`,
          level: "error",
          source: "Legrand Energy",
          message: `Poll en échec : ${err instanceof Error ? err.message : String(err)}`,
        });
      }
    } finally {
      this.polling = false;
    }
  }

  private async discoverMeters(): Promise<void> {
    const homesData = await this.bridge!.getHomesData();
    const home = homesData.body.homes.find((h) => h.id === this.homeId);
    if (!home) throw new Error(`Home ${this.homeId} not found`);

    const modules = home.modules ?? [];
    const activeIds = new Set<string>();

    for (const mod of modules) {
      if (mod.type !== "NLPC") continue;

      // Capture bridge ID from first NLPC with a bridge
      if (mod.bridge && !this.bridgeId) {
        this.bridgeId = mod.bridge;
        this.logger.info({ moduleId: mod.id, bridge: mod.bridge }, "Bridge ID captured");
      }

      // First NLPC with a bridge = main meter (consumption)
      if (!this.mainMeterName && mod.bridge) {
        this.mainMeterName = mod.name || mod.id;
        this.logger.info({ name: this.mainMeterName }, "Main energy meter identified");
      }

      const discovered = mapMeterToDiscovered(mod, this.homeId);
      this.deviceManager.upsertFromDiscovery(INTEGRATION_ID, INTEGRATION_ID, discovered);

      const name = mod.name || mod.id;
      this.moduleNames.set(mod.id, name);
      activeIds.add(name);
    }

    this.deviceManager.removeStaleDevices(INTEGRATION_ID, activeIds);
  }

  private async pollMeterStatus(): Promise<void> {
    const status = await this.bridge!.getHomeStatus(this.homeId);
    const modules = status.body.home.modules;

    for (const mod of modules) {
      const friendlyName = this.moduleNames.get(mod.id);
      if (!friendlyName) continue;

      const payload: Record<string, unknown> = {};
      if (mod.power !== undefined) payload.power = mod.power;
      if (mod.sum_energy_elec !== undefined) payload.sum_energy_elec = mod.sum_energy_elec;
      this.deviceManager.updateDeviceData(INTEGRATION_ID, friendlyName, payload);
    }
  }

  private async pollEnergy(): Promise<void> {
    if (!this.bridgeId || !this.mainMeterName) return;

    try {
      const nowTs = Math.floor(Date.now() / 1000);
      const lookbackStart = Math.floor((nowTs - ENERGY_LOOKBACK_S) / HALF_HOUR) * HALF_HOUR;

      let newBuckets = 0;
      let lastBucketEnergy = 0;
      let lastBucketProduction = 0;

      for (let windowStart = lookbackStart; windowStart < nowTs; windowStart += HALF_HOUR) {
        const windowEnd = windowStart + HALF_HOUR;
        if (windowEnd > nowTs) break;

        const { consumption, production, autoconso, injection } = await this.queryEnergyWindow(windowStart, windowEnd);
        if (consumption <= 0 && production <= 0) continue;

        // Write consumption to main meter
        if (consumption > 0) {
          this.deviceManager.updateDeviceData(
            INTEGRATION_ID, this.mainMeterName!, { energy: consumption }, windowStart,
          );
        }

        // Write production to configured device
        if (production > 0) {
          const prodDeviceName = this.getSetting("production_device_name");
          if (prodDeviceName) {
            this.deviceManager.updateDeviceData(
              INTEGRATION_ID, prodDeviceName, { energy: production, autoconso, injection }, windowStart,
            );
          }
        }

        if (windowStart > this.lastEmittedWindowTs) {
          newBuckets++;
          lastBucketEnergy = consumption;
          lastBucketProduction = production;
          this.lastEmittedWindowTs = windowStart;
        }
      }

      // Write demand_30min (instantaneous power estimate from last bucket)
      if (lastBucketEnergy > 0) {
        this.deviceManager.updateDeviceData(INTEGRATION_ID, this.mainMeterName!, {
          demand_30min: Math.round(lastBucketEnergy * 2), // Wh per 30min → W
        });
      }

      if (newBuckets > 0) {
        this.logger.debug(
          { newBuckets, demand30minW: Math.round(lastBucketEnergy * 2), lastProdWh: lastBucketProduction },
          "Energy poll: new 30-min buckets processed",
        );
      }
    } catch (err) {
      this.logger.warn({ err } as Record<string, unknown>, "Failed to poll bridge-level energy data");
    }
  }

  private async queryEnergyWindow(
    windowStart: number, windowEnd: number,
  ): Promise<{ consumption: number; production: number; autoconso: number; injection: number }> {
    const ENERGY_TYPES = "sum_energy_buy_from_grid$1,sum_energy_buy_from_grid$2,sum_energy_self_consumption,sum_energy_resell_to_grid";

    const res = await this.bridge!.getMeasure(this.bridgeId!, this.bridgeId!, ENERGY_TYPES, "5min", windowStart, windowEnd);
    const timestamps = Object.keys(res.body);
    if (timestamps.length === 0) return { consumption: 0, production: 0, autoconso: 0, injection: 0 };

    let consumption = 0;
    let autoconso = 0;
    let injection = 0;
    for (const ts of timestamps) {
      const values = res.body[ts];
      const hp = values[0] ?? 0;
      const hc = values[1] ?? 0;
      const selfConso = values[2] ?? 0;
      const resellToGrid = values[3] ?? 0;
      consumption += hp + hc + selfConso;
      autoconso += selfConso;
      injection += resellToGrid;
    }
    return { consumption, production: autoconso + injection, autoconso, injection };
  }

  private safePoll(): void {
    this.poll().catch((err) => this.logger.error({ err } as Record<string, unknown>, "Poll failed"));
  }

  // ============================================================
  // Legacy migration (netatmo_hc → legrand_energy)
  // ============================================================

  private migrateFromLegacy(): void {
    try {
      const migrated = this.deviceManager.migrateIntegrationId(
        LEGACY_INTEGRATION_ID, INTEGRATION_ID, ["NLPC"],
      );
      if (migrated > 0) {
        this.logger.info({ migrated }, "Migrated NLPC devices from netatmo_hc to legrand_energy");
      }
    } catch (err) {
      this.logger.warn({ err } as Record<string, unknown>, "Legacy migration failed (non-fatal)");
    }
  }

  // ============================================================
  // Retry + helpers
  // ============================================================

  private scheduleRetry(): void {
    this.cancelRetry();
    this.retryCount++;
    const delaySec = Math.min(30 * Math.pow(2, this.retryCount - 1), 600);
    this.logger.warn({ retryCount: this.retryCount, delaySec }, "Scheduling retry");
    this.retryTimeout = setTimeout(() => {
      this.retryTimeout = null;
      this.start().catch((err) => this.logger.error({ err } as Record<string, unknown>, "Retry failed"));
    }, delaySec * 1000);
  }

  private cancelRetry(): void {
    if (this.retryTimeout) { clearTimeout(this.retryTimeout); this.retryTimeout = null; }
  }

  private stopPolling(): void {
    if (this.pollInterval) { clearInterval(this.pollInterval); this.pollInterval = null; }
  }

  private getSetting(key: string): string | undefined {
    return this.settingsManager.get(`${SETTINGS_PREFIX}${key}`);
  }
}

// ============================================================
// Plugin entry point
// ============================================================

export function createPlugin(deps: PluginDeps): IntegrationPlugin {
  return new LegrandEnergyPlugin(deps);
}
