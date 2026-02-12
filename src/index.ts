import mqtt, { IClientOptions, MqttClient } from "mqtt";
import os from "node:os";

type Env = {
  MQTT_URL: string;                 // e.g. mqtt://emqx.local:1883
  MQTT_USERNAME?: string;
  MQTT_PASSWORD?: string;

  NODE_ID?: string;                 // defaults to hostname
  TOPIC_PREFIX: string;             // e.g. lan/rpi
  HEARTBEAT_INTERVAL_SEC: number;   // e.g. 15
  QOS: 0 | 1 | 2;                   // default 1
  RETAIN_STATUS: boolean;           // default true

  CLIENT_ID_PREFIX: string;         // default "hb"
  KEEPALIVE_SEC: number;            // default 30

  LOG_LEVEL: "debug" | "info" | "warn" | "error";
};

const ENV_DEFAULTS = {
  TOPIC_PREFIX: "lan/rpi",
  HEARTBEAT_INTERVAL_SEC: 15,
  QOS: 1 as const,
  RETAIN_STATUS: true,
  CLIENT_ID_PREFIX: "hb",
  KEEPALIVE_SEC: 30,
  LOG_LEVEL: "info" as const,
};

const readEnv = (): Env => {
  const must = (k: string) => {
    const v = process.env[k];
    if (!v || !v.trim()) throw new Error(`Missing required env var: ${k}`);
    return v.trim();
  };

  const opt = (k: string) => {
    const v = process.env[k];
    return v && v.trim() ? v.trim() : undefined;
  };

  const num = (k: string, def: number) => {
    const v = opt(k);
    if (!v) return def;
    const n = Number(v);
    if (!Number.isFinite(n) || n <= 0) throw new Error(`Invalid ${k}: ${v}`);
    return n;
  };

  const bool = (k: string, def: boolean) => {
    const v = opt(k);
    if (!v) return def;
    if (["1", "true", "yes", "on"].includes(v.toLowerCase())) return true;
    if (["0", "false", "no", "off"].includes(v.toLowerCase())) return false;
    throw new Error(`Invalid boolean ${k}: ${v}`);
  };

  const qos = (k: string, def: 0 | 1 | 2) => {
    const v = opt(k);
    if (!v) return def;
    if (v === "0" || v === "1" || v === "2") return Number(v) as 0 | 1 | 2;
    throw new Error(`Invalid ${k}: ${v} (must be 0|1|2)`);
  };

  const level = (k: string, def: Env["LOG_LEVEL"]) => {
    const v = opt(k);
    if (!v) return def;
    const vv = v.toLowerCase();
    if (vv === "debug" || vv === "info" || vv === "warn" || vv === "error") return vv;
    throw new Error(`Invalid ${k}: ${v}`);
  };

  return {
    MQTT_URL: must("MQTT_URL"),
    MQTT_USERNAME: opt("MQTT_USERNAME"),
    MQTT_PASSWORD: opt("MQTT_PASSWORD"),

    NODE_ID: opt("NODE_ID"),
    TOPIC_PREFIX: opt("TOPIC_PREFIX") ?? ENV_DEFAULTS.TOPIC_PREFIX,
    HEARTBEAT_INTERVAL_SEC: num("HEARTBEAT_INTERVAL_SEC", ENV_DEFAULTS.HEARTBEAT_INTERVAL_SEC),
    QOS: qos("QOS", ENV_DEFAULTS.QOS),
    RETAIN_STATUS: bool("RETAIN_STATUS", ENV_DEFAULTS.RETAIN_STATUS),

    CLIENT_ID_PREFIX: opt("CLIENT_ID_PREFIX") ?? ENV_DEFAULTS.CLIENT_ID_PREFIX,
    KEEPALIVE_SEC: num("KEEPALIVE_SEC", ENV_DEFAULTS.KEEPALIVE_SEC),

    LOG_LEVEL: level("LOG_LEVEL", ENV_DEFAULTS.LOG_LEVEL),
  };
};

const log = (level: Env["LOG_LEVEL"], current: Env["LOG_LEVEL"], msg: string, extra?: unknown) => {
  const order: Record<Env["LOG_LEVEL"], number> = { debug: 10, info: 20, warn: 30, error: 40 };
  if (order[level] < order[current]) return;
  const prefix = `[${new Date().toISOString()}] [${level}]`;
  if (extra !== undefined) console.log(prefix, msg, extra);
  else console.log(prefix, msg);
};

const mkTopics = (prefix: string, nodeId: string) => ({
  status: `${prefix}/${nodeId}/status`,
  heartbeat: `${prefix}/${nodeId}/heartbeat`,
});

const mkStatusPayload = (state: "online" | "offline") => ({
  state,
  ts: new Date().toISOString(),
});

const mkHeartbeatPayload = (nodeId: string, version: string) => ({
  ts: new Date().toISOString(),
  nodeId,
  uptimeSec: Math.floor(os.uptime()),
  loadAvg: os.loadavg(),
  mem: { total: os.totalmem(), free: os.freemem() },
  pid: process.pid,
  version,
});

const safeJson = (v: unknown) => JSON.stringify(v);

const connectClient = (env: Env, nodeId: string, topics: ReturnType<typeof mkTopics>) => {
  const clientId = `${env.CLIENT_ID_PREFIX}-${nodeId}-${Math.random().toString(16).slice(2, 10)}`;

  const willPayload = safeJson(mkStatusPayload("offline"));

  const opts: IClientOptions = {
    clientId,
    username: env.MQTT_USERNAME,
    password: env.MQTT_PASSWORD,
    keepalive: env.KEEPALIVE_SEC,
    reconnectPeriod: 2000, // ms
    connectTimeout: 10_000,
    clean: true,

    will: {
      topic: topics.status,
      payload: willPayload,
      qos: env.QOS,
      retain: env.RETAIN_STATUS,
    },
  };

  log("info", env.LOG_LEVEL, `Connecting to MQTT`, { url: env.MQTT_URL, clientId, nodeId });
  const client = mqtt.connect(env.MQTT_URL, opts);
  return client;
};

const publish = async (client: MqttClient, topic: string, payload: string, qos: 0 | 1 | 2, retain: boolean) =>
  new Promise<void>((resolve, reject) => {
    client.publish(topic, payload, { qos, retain }, (err) => (err ? reject(err) : resolve()));
  });

const main = async () => {
  const env = readEnv();
  const nodeId = env.NODE_ID ?? os.hostname();
  const topics = mkTopics(env.TOPIC_PREFIX, nodeId);
  const version = process.env.npm_package_version ?? "1.0.0";

  const client = connectClient(env, nodeId, topics);

  let interval: NodeJS.Timeout | undefined;
  let shuttingDown = false;

  const startLoop = () => {
    if (interval) return;
    interval = setInterval(async () => {
      if (!client.connected) return;
      const hb = mkHeartbeatPayload(nodeId, version);
      try {
        await publish(client, topics.heartbeat, safeJson(hb), env.QOS, false);
        log("debug", env.LOG_LEVEL, `Heartbeat published`, { topic: topics.heartbeat });
      } catch (e) {
        log("warn", env.LOG_LEVEL, `Heartbeat publish failed`, { error: String(e) });
      }
    }, env.HEARTBEAT_INTERVAL_SEC * 1000);
  };

  const stopLoop = () => {
    if (interval) clearInterval(interval);
    interval = undefined;
  };

  client.on("connect", async () => {
    log("info", env.LOG_LEVEL, `MQTT connected`);
    // announce online retained
    try {
      await publish(client, topics.status, safeJson(mkStatusPayload("online")), env.QOS, env.RETAIN_STATUS);
      log("info", env.LOG_LEVEL, `Status online published`, { topic: topics.status });
    } catch (e) {
      log("warn", env.LOG_LEVEL, `Status online publish failed`, { error: String(e) });
    }
    startLoop();
  });

  client.on("reconnect", () => log("info", env.LOG_LEVEL, `MQTT reconnecting...`));
  client.on("close", () => {
    log("warn", env.LOG_LEVEL, `MQTT connection closed`);
    stopLoop();
  });
  client.on("offline", () => log("warn", env.LOG_LEVEL, `MQTT client offline`));
  client.on("error", (err) => log("error", env.LOG_LEVEL, `MQTT error`, { error: String(err) }));

  const shutdown = async (signal: string) => {
    if (shuttingDown) return;
    shuttingDown = true;
    log("info", env.LOG_LEVEL, `Shutting down`, { signal });

    stopLoop();

    // If connected, publish offline explicitly, then end cleanly.
    // LWT handles unclean exits.
    try {
      if (client.connected) {
        await publish(client, topics.status, safeJson(mkStatusPayload("offline")), env.QOS, env.RETAIN_STATUS);
        log("info", env.LOG_LEVEL, `Status offline published`, { topic: topics.status });
      }
    } catch (e) {
      log("warn", env.LOG_LEVEL, `Status offline publish failed`, { error: String(e) });
    }

    client.end(true, () => {
      process.exit(0);
    });

    // If end callback never fires, force exit
    setTimeout(() => process.exit(0), 3000).unref();
  };

  process.on("SIGINT", () => void shutdown("SIGINT"));
  process.on("SIGTERM", () => void shutdown("SIGTERM"));
};

main().catch((e) => {
  // fail fast; docker will restart
  console.error(`[fatal] ${String(e)}`);
  process.exit(1);
});
