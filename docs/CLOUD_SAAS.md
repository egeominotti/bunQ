# bunqueue Dashboard SaaS — Architettura Completa

## Indice

1. [Vision](#1-vision)
2. [Architettura Generale](#2-architettura-generale)
3. [Phone Home Agent (dentro bunqueue)](#3-phone-home-agent-dentro-bunqueue)
4. [Dashboard Server (backend)](#4-dashboard-server-backend)
5. [Database Schema](#5-database-schema)
6. [API Endpoints](#6-api-endpoints)
7. [Multi-Tenancy](#7-multi-tenancy)
8. [Autenticazione e Sicurezza](#8-autenticazione-e-sicurezza)
9. [Sistema di Alerting](#9-sistema-di-alerting)
10. [Dashboard Frontend](#10-dashboard-frontend)
11. [Billing e Metering](#11-billing-e-metering)
12. [Deployment](#12-deployment)
13. [Onboarding Cliente](#13-onboarding-cliente)
14. [Roadmap Fasi](#14-roadmap-fasi)

---

## 1. Vision

Un servizio SaaS dove chiunque usi bunqueue può monitorare le proprie istanze da una dashboard remota.

**Esperienza utente:**

```
1. Il cliente si registra su dashboard.bunqueue.dev
2. Crea un'organizzazione, riceve una API key
3. Aggiunge 2 variabili d'ambiente alla sua istanza bunqueue:
     PHONE_HOME_URL=https://dashboard.bunqueue.dev
     PHONE_HOME_API_KEY=dk_live_abc123
4. Riavvia bunqueue
5. Dopo 5 secondi, la dashboard mostra l'istanza online con tutti i dati
```

Nessun agent esterno, nessun tunnel, nessuna porta da aprire. Funziona ovunque.

---

## 2. Architettura Generale

```
┌──────────────────────────────────────────────────────────────────────────┐
│                         CLIENTI (on-premise / cloud)                     │
│                                                                          │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐          │
│  │ bunqueue v2.7+  │  │ bunqueue v2.7+  │  │ bunqueue v2.7+  │          │
│  │ Org: Acme Inc   │  │ Org: Acme Inc   │  │ Org: Startup X  │          │
│  │ Instance: prod-1│  │ Instance: prod-2│  │ Instance: main  │          │
│  │                 │  │                 │  │                 │          │
│  │ ┌─────────────┐ │  │ ┌─────────────┐ │  │ ┌─────────────┐ │          │
│  │ │ PhoneHome   │ │  │ │ PhoneHome   │ │  │ │ PhoneHome   │ │          │
│  │ │ Agent       │ │  │ │ Agent       │ │  │ │ Agent       │ │          │
│  │ └──────┬──────┘ │  │ └──────┬──────┘ │  │ └──────┬──────┘ │          │
│  └────────┼────────┘  └────────┼────────┘  └────────┼────────┘          │
│           │ HTTPS              │ HTTPS              │ HTTPS             │
└───────────┼────────────────────┼────────────────────┼────────────────────┘
            │                    │                    │
            ▼                    ▼                    ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                    DASHBOARD SAAS (dashboard.bunqueue.dev)                │
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────┐     │
│  │                        API Gateway / Load Balancer               │     │
│  │                     (Cloudflare / nginx / Caddy)                │     │
│  └───────────┬───────────────────┬─────────────────────────────────┘     │
│              │                   │                                        │
│  ┌───────────▼──────────┐  ┌────▼────────────────────┐                  │
│  │   Ingest Service     │  │   Web App Service        │                  │
│  │                      │  │                          │                  │
│  │ POST /api/v1/ingest  │  │ GET  /app/*              │                  │
│  │ WS   /api/v1/stream  │  │ GET  /api/v1/dashboard/* │                  │
│  │                      │  │ POST /api/v1/auth/*      │                  │
│  │ • Valida API key     │  │ GET  /api/v1/alerts/*    │                  │
│  │ • Verifica HMAC      │  │                          │                  │
│  │ • Scrive su DB       │  │ • Serve frontend React   │                  │
│  │ • Inoltra a WS hub   │  │ • API per dashboard UI   │                  │
│  └───────────┬──────────┘  │ • Gestione utenti/org    │                  │
│              │             └────┬────────────────────┘                  │
│              │                  │                                        │
│  ┌───────────▼──────────────────▼──────────────────────────────────┐    │
│  │                         Database Layer                           │    │
│  │                                                                  │    │
│  │  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────┐  │    │
│  │  │   PostgreSQL     │  │   TimescaleDB    │  │    Redis      │  │    │
│  │  │                  │  │   (extension)    │  │               │  │    │
│  │  │ • organizations  │  │                  │  │ • Sessions    │  │    │
│  │  │ • users          │  │ • snapshots      │  │ • WS pub/sub  │  │    │
│  │  │ • api_keys       │  │ • queue_metrics  │  │ • Rate limits │  │    │
│  │  │ • instances      │  │ • events         │  │ • Cache       │  │    │
│  │  │ • alert_rules    │  │ • alerts_history │  │               │  │    │
│  │  │ • billing        │  │                  │  │               │  │    │
│  │  └──────────────────┘  └──────────────────┘  └──────────────┘  │    │
│  └──────────────────────────────────────────────────────────────────┘    │
│                                                                          │
│  ┌──────────────────────────────────────────────────────────────────┐    │
│  │                      Background Workers                          │    │
│  │                                                                  │    │
│  │  • Instance health checker (ogni 5s)                             │    │
│  │  • Alert evaluator (ogni 10s)                                    │    │
│  │  • Data retention cleaner (ogni 1h)                              │    │
│  │  • Usage metering aggregator (ogni 1min)                         │    │
│  │  • Notification dispatcher (real-time)                           │    │
│  └──────────────────────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Phone Home Agent (dentro bunqueue)

### 3.1 Dove vive nel codice

```
src/infrastructure/phoneHome/
├── phoneHomeAgent.ts       # Orchestratore principale
├── snapshotCollector.ts    # Raccoglie dati da tutti i manager
├── httpSender.ts           # Canale 1: POST snapshot
├── wsSender.ts             # Canale 2: WebSocket eventi
├── buffer.ts               # Buffer locale per offline
├── circuitBreaker.ts       # Circuit breaker per resilienza
├── instanceId.ts           # Gestione ID persistente
└── config.ts               # Parsing configurazione da env
```

### 3.2 Ciclo di vita

```
bunqueue start
  │
  ▼
QueueManager.init()
  │
  ▼
PhoneHomeAgent.init()
  ├── Legge env vars (PHONE_HOME_URL, PHONE_HOME_API_KEY)
  ├── Se PHONE_HOME_URL è vuoto → agent disabilitato, zero overhead
  ├── Carica o genera instanceId (file: data/phone-home-instance-id)
  ├── Avvia httpSender (timer ogni PHONE_HOME_INTERVAL_MS)
  ├── Avvia wsSender (WebSocket persistente verso dashboard)
  └── Si iscrive a EventsManager per eventi real-time
  │
  ▼
[In esecuzione]
  ├── Ogni 5s: raccoglie snapshot → POST /api/v1/ingest
  ├── Real-time: eventi → WS /api/v1/stream
  └── Se dashboard irraggiungibile: buffer + circuit breaker
  │
  ▼
bunqueue shutdown
  ├── Invia snapshot finale con flag shutdown: true
  ├── Chiude WebSocket
  └── Flush buffer rimanente (best effort, timeout 2s)
```

### 3.3 Configurazione (variabili d'ambiente)

```bash
# ─── Obbligatori per attivare ───
PHONE_HOME_URL=https://dashboard.bunqueue.dev    # URL del server dashboard
PHONE_HOME_API_KEY=dk_live_abc123def456           # API key dell'organizzazione

# ─── Opzionali ───
PHONE_HOME_INSTANCE_NAME=prod-eu-1    # Nome leggibile (default: hostname)
PHONE_HOME_INTERVAL_MS=5000           # Intervallo snapshot (default: 5000)
PHONE_HOME_SIGNING_SECRET=            # HMAC secret (fornito dalla dashboard)

# ─── Privacy ───
PHONE_HOME_INCLUDE_JOB_DATA=false     # Inviare i dati dei job (default: false)
PHONE_HOME_REDACT_FIELDS=             # Campi da rimuovere (es: email,password)

# ─── Filtro eventi ───
PHONE_HOME_EVENTS=Failed,Stalled      # Tipi evento da inoltrare (vuoto = tutti)
PHONE_HOME_DASHBOARD_EVENTS=true      # Eventi dashboard: queue:idle, ecc.

# ─── Avanzato ───
PHONE_HOME_BUFFER_SIZE=720            # Max snapshot in buffer offline
PHONE_HOME_CIRCUIT_BREAKER_THRESHOLD=5  # Fallimenti prima di aprire circuito
PHONE_HOME_CIRCUIT_BREAKER_RESET_MS=60000  # Tempo in stato OPEN
```

### 3.4 Snapshot — cosa viene inviato

Ogni 5 secondi, l'agent raccoglie questo payload (~2-10 KB, gzip ~0.5-2 KB):

```typescript
interface PhoneHomeSnapshot {
  // Identità
  instanceId: string;         // UUID persistente su disco
  instanceName: string;       // PHONE_HOME_INSTANCE_NAME o hostname
  version: string;            // Versione bunqueue (es. "2.7.0")
  hostname: string;           // os.hostname()
  pid: number;                // process.pid
  startedAt: number;          // Timestamp avvio server
  timestamp: number;          // Timestamp di questo snapshot
  sequenceId: number;         // Contatore incrementale per detect gap
  shutdown?: boolean;         // true solo nell'ultimo snapshot

  // Stats globali — da statsManager.getStats()
  stats: {
    waiting: number;
    delayed: number;
    active: number;
    dlq: number;
    completed: number;
    totalPushed: string;      // bigint serializzato come stringa
    totalPulled: string;
    totalCompleted: string;
    totalFailed: string;
    uptime: number;
    cronJobs: number;
    cronPending: number;
  };

  // Throughput — da throughputTracker.getRates()
  throughput: {
    pushPerSec: number;
    pullPerSec: number;
    completePerSec: number;
    failPerSec: number;
  };

  // Latenza — da latencyTracker
  latency: {
    averages: {
      pushMs: number;
      pullMs: number;
      ackMs: number;
    };
    percentiles: {
      push: { p50: number; p95: number; p99: number };
      pull: { p50: number; p95: number; p99: number };
      ack:  { p50: number; p95: number; p99: number };
    };
  };

  // Memoria processo — da process.memoryUsage()
  memory: {
    heapUsed: number;       // MB
    heapTotal: number;      // MB
    rss: number;            // MB
    external: number;       // MB
  };

  // Collezioni interne — da queueManager.getMemoryStats()
  collections: {
    jobIndex: number;
    completedJobs: number;
    jobResults: number;
    jobLogs: number;
    customIdMap: number;
    jobLocks: number;
    processingTotal: number;
    queuedTotal: number;
    temporalIndexTotal: number;
    delayedHeapTotal: number;
  };

  // Per-coda — da queueManager iterando le code registrate
  queues: Array<{
    name: string;
    waiting: number;
    delayed: number;
    active: number;
    dlq: number;
    paused: boolean;
  }>;

  // Worker — da workerManager
  workers: {
    total: number;
    active: number;
    totalProcessed: number;
    totalFailed: number;
    activeJobs: number;
    list: Array<{
      id: string;
      name: string;
      queues: string[];
      concurrency: number;
      hostname: string;
      pid: number;
      lastSeen: number;
      activeJobs: number;
      processedJobs: number;
      failedJobs: number;
    }>;
  };

  // Cron — da cronScheduler
  crons: Array<{
    name: string;
    queue: string;
    schedule: string;
    nextRun: number;
    enabled: boolean;
  }>;

  // Storage — da persistence
  storage: {
    sizeBytes: number;
    sizeHuman: string;
    walSizeBytes: number;
    healthy: boolean;
  };

  // Errori background tasks — da backgroundTasks
  taskErrors: Record<string, {
    errors: number;
    lastError: string;
    lastErrorTime: number;
  }>;
}
```

### 3.5 Eventi real-time — cosa viene inoltrrato via WebSocket

```typescript
interface PhoneHomeEvent {
  instanceId: string;
  timestamp: number;

  // Eventi job (dal EventsManager)
  jobEvent?: {
    eventType: string;     // "Failed" | "Stalled" | "Completed" | ...
    queue: string;
    jobId: string;
    error?: string;
    progress?: number;
    data?: unknown;        // Solo se PHONE_HOME_INCLUDE_JOB_DATA=true
  };

  // Eventi dashboard (dal sistema monitoringChecks)
  dashboardEvent?: {
    type: string;          // "queue:idle" | "worker:error" | "dlq:auto-retried" | ...
    data: Record<string, unknown>;
  };
}
```

### 3.6 Costo computazionale sull'istanza bunqueue

| Operazione | Complessità | Frequenza | Costo reale |
|-----------|-------------|-----------|-------------|
| getStats() | O(SHARD_COUNT) | ogni 5s | ~0.01ms |
| getMemoryStats() | O(SHARD_COUNT) | ogni 5s | ~0.01ms |
| getRates() | O(1) | ogni 5s | ~0.001ms |
| getPercentiles() | O(1) | ogni 5s | ~0.001ms |
| workerManager.list() | O(N workers) | ogni 5s | ~0.01ms |
| JSON.stringify() | O(payload size) | ogni 5s | ~0.05ms |
| gzip compress | O(payload size) | ogni 5s | ~0.1ms |
| HTTP POST | O(1) network | ogni 5s | ~5-50ms (async) |
| WS event forward | O(1) | per evento | ~0.01ms |

**Totale: ~0.2ms di CPU ogni 5s + una richiesta HTTP asincrona.**
**Impatto sulle performance di bunqueue: zero misurabile.**

### 3.7 Resilienza dell'agent

**Buffer offline:**
```
Dashboard giù per 30 minuti:
  → 360 snapshot bufferizzati localmente (720 max)
  → Quando la connessione torna, flush batch di 100
  → La dashboard vede il buco riempirsi con dati storici
```

**Circuit breaker:**
```
CLOSED → 5 fallimenti consecutivi → OPEN (smette di provare)
  → Dopo 60s → HALF_OPEN (prova una richiesta)
    → Successo → CLOSED (riprende normalmente)
    → Fallimento → OPEN (aspetta altri 60s)
```

**WebSocket reconnect:**
```
Connessione persa → attende 1s → riconnette
  → Persa di nuovo → attende 2s → riconnette
    → Persa → 4s → 8s → 16s → 30s (max)
  → Connessione OK → reset a 1s
  → Jitter random ±500ms per evitare thundering herd
```

---

## 4. Dashboard Server (backend)

### 4.1 Tech Stack consigliato

| Componente | Tecnologia | Perché |
|-----------|-----------|--------|
| Runtime | Bun | Coerenza con bunqueue, performance |
| Framework HTTP | Hono | Leggero, veloce, TypeScript-native |
| Database relazionale | PostgreSQL 16 | Maturo, affidabile, estendibile |
| Time-series | TimescaleDB (estensione PG) | Un solo database, compressione nativa |
| Cache/pub-sub | Redis 7 | Session store, WS fan-out, rate limiting |
| Frontend | React + Vite | Ecosistema ricco, buon DX |
| Grafici | Recharts o Tremor | Ottimizzati per dashboard |
| Auth | Better Auth oppure Lucia | Leggero, self-hosted |
| Email | Resend | Transazionali (alert, inviti, onboarding) |
| Hosting | Fly.io oppure Railway | Deploy facile, edge locations |

### 4.2 Struttura progetto

```
dashboard/
├── packages/
│   ├── server/                     # Backend API
│   │   ├── src/
│   │   │   ├── routes/
│   │   │   │   ├── auth.ts         # Login, register, API keys
│   │   │   │   ├── ingest.ts       # POST /api/v1/ingest (snapshot)
│   │   │   │   ├── stream.ts       # WS /api/v1/stream (eventi)
│   │   │   │   ├── instances.ts    # CRUD istanze
│   │   │   │   ├── dashboard.ts    # Query per UI
│   │   │   │   ├── alerts.ts       # Regole + storico alert
│   │   │   │   ├── organizations.ts # Gestione org
│   │   │   │   └── billing.ts      # Usage + piani
│   │   │   ├── services/
│   │   │   │   ├── ingestService.ts      # Processa snapshot
│   │   │   │   ├── healthChecker.ts      # Monitora istanze
│   │   │   │   ├── alertEvaluator.ts     # Valuta regole alert
│   │   │   │   ├── retentionCleaner.ts   # Pulizia dati vecchi
│   │   │   │   ├── meteringService.ts    # Conteggio usage
│   │   │   │   └── notificationService.ts # Invio notifiche
│   │   │   ├── db/
│   │   │   │   ├── schema.ts       # Drizzle schema
│   │   │   │   └── migrations/     # SQL migrations
│   │   │   └── middleware/
│   │   │       ├── auth.ts         # JWT / session validation
│   │   │       ├── apiKey.ts       # API key validation (ingest)
│   │   │       └── rateLimit.ts    # Per-org rate limiting
│   │   └── package.json
│   │
│   └── web/                        # Frontend
│       ├── src/
│       │   ├── pages/
│       │   │   ├── Login.tsx
│       │   │   ├── Dashboard.tsx         # Overview multi-istanza
│       │   │   ├── Instance.tsx          # Dettaglio singola istanza
│       │   │   ├── Queues.tsx            # Lista code per istanza
│       │   │   ├── QueueDetail.tsx       # Dettaglio singola coda
│       │   │   ├── Workers.tsx           # Stato worker
│       │   │   ├── Events.tsx            # Feed eventi live
│       │   │   ├── Alerts.tsx            # Regole + storico
│       │   │   ├── Settings.tsx          # Org, API keys, team
│       │   │   └── Billing.tsx           # Piano, usage, fatture
│       │   ├── components/
│       │   │   ├── charts/               # Grafici riutilizzabili
│       │   │   ├── InstanceCard.tsx       # Card con status
│       │   │   ├── MetricPanel.tsx        # Pannello metrica singola
│       │   │   ├── EventFeed.tsx          # Feed real-time
│       │   │   ├── QueueTable.tsx         # Tabella code
│       │   │   └── AlertBanner.tsx        # Banner alert attivo
│       │   ├── hooks/
│       │   │   ├── useInstances.ts        # Query istanze
│       │   │   ├── useMetrics.ts          # Query serie temporali
│       │   │   ├── useWebSocket.ts        # Connessione WS live
│       │   │   └── useAlerts.ts           # Alert attivi
│       │   └── lib/
│       │       ├── api.ts                 # Client HTTP
│       │       └── ws.ts                  # Client WebSocket
│       └── package.json
│
├── docker-compose.yml              # PostgreSQL + Redis per dev
└── package.json                    # Monorepo root
```

### 4.3 Servizi backend nel dettaglio

#### Ingest Service — cuore del sistema

Riceve gli snapshot, li valida, li scrive nel DB.

```
Richiesta in arrivo:
  POST /api/v1/ingest
  Authorization: Bearer dk_live_abc123
  Content-Type: application/json
  Content-Encoding: gzip
  X-Instance-Id: 550e8400-e29b-41d4-a716-446655440000
  X-Timestamp: 1710849600000
  X-Signature: a1b2c3d4e5...   (HMAC-SHA256 del body)

Flusso:
  1. Estrai API key dal header Authorization
  2. Cerca API key nel DB → ottieni org_id
  3. Verifica che l'istanza appartenga a questa org
  4. Se X-Signature presente, verifica HMAC
  5. Decomprimi gzip body
  6. Valida schema JSON (TypeBox o Zod)
  7. Scrivi in batch:
     - UPDATE instances SET last_seen = NOW(), status = 'online'
     - INSERT INTO snapshots (time, instance_id, ...) VALUES (...)
     - INSERT INTO queue_snapshots (time, instance_id, ...) per ogni coda
  8. Pubblica su Redis channel "ingest:{org_id}" per fan-out WS
  9. Incrementa contatore usage per metering
  10. Rispondi 200 OK (o 207 con warning se quota vicina)
```

#### Health Checker — rileva istanze offline

```
Ogni 5 secondi:
  1. SELECT id, last_seen, status FROM instances WHERE status != 'offline'
  2. Per ogni istanza:
     - Se last_seen > 30s fa AND status = 'online' → SET status = 'degraded'
     - Se last_seen > 60s fa AND status != 'offline' → SET status = 'offline'
     - Se status cambiato → genera alert "instance_status_changed"
  3. Per istanze che tornano online (ricevono snapshot dopo offline):
     - SET status = 'online'
     - Genera alert "instance_recovered"
```

#### Alert Evaluator — valuta regole personalizzate

```
Ogni 10 secondi:
  1. SELECT alert_rules WHERE enabled = true (con JOIN su org)
  2. Per ogni regola:
     - Recupera l'ultimo snapshot dell'istanza target
     - Valuta la condizione (es. "dlq > 100", "heap_used_mb > 500")
     - Se condizione vera per >= durata soglia:
       - Crea alert in alerts_history
       - Invia notifica (email, webhook, Slack)
     - Se condizione risolta:
       - Chiudi alert esistente
       - Invia notifica "resolved"
```

#### Retention Cleaner — elimina dati vecchi

```
Ogni 1 ora:
  1. Per ogni organizzazione:
     - Leggi piano (free = 7 giorni, pro = 90 giorni)
     - DELETE FROM snapshots WHERE time < NOW() - retention
     - DELETE FROM queue_snapshots WHERE time < NOW() - retention
     - DELETE FROM events WHERE time < NOW() - INTERVAL '7 days'
  2. Esegui TimescaleDB compression per chunk > 1 giorno
  3. Log: "Cleaned X rows for org Y, freed Z MB"
```

#### Metering Service — conta l'utilizzo

```
Ogni 1 minuto:
  1. Per ogni organizzazione:
     - Conta snapshot ricevuti nell'ultimo minuto
     - Conta eventi ricevuti nell'ultimo minuto
     - Conta istanze attive (status != offline)
  2. INSERT INTO usage_records (org_id, period, snapshots, events, instances)
  3. Se supera quota piano:
     - Imposta flag rate_limited sulla org
     - Ingest Service inizia a droppare snapshot (ma accetta 1 ogni 60s)
     - Invia email "quota exceeded"
```

---

## 5. Database Schema

### 5.1 PostgreSQL — dati relazionali

```sql
-- ═══════════════════════════════════════════════
-- ORGANIZZAZIONI E UTENTI
-- ═══════════════════════════════════════════════

CREATE TABLE organizations (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT NOT NULL,
    slug            TEXT NOT NULL UNIQUE,              -- acme-inc (per URL)
    plan            TEXT NOT NULL DEFAULT 'free',      -- free | pro | enterprise
    plan_started_at TIMESTAMPTZ,
    rate_limited    BOOLEAN NOT NULL DEFAULT false,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE users (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email           TEXT NOT NULL UNIQUE,
    password_hash   TEXT NOT NULL,
    name            TEXT,
    avatar_url      TEXT,
    email_verified  BOOLEAN NOT NULL DEFAULT false,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE org_members (
    org_id          UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    user_id         UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    role            TEXT NOT NULL DEFAULT 'member',    -- owner | admin | member | viewer
    invited_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    accepted_at     TIMESTAMPTZ,
    PRIMARY KEY (org_id, user_id)
);

-- ═══════════════════════════════════════════════
-- API KEYS
-- ═══════════════════════════════════════════════

CREATE TABLE api_keys (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    org_id          UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    -- La key è dk_live_<random> o dk_test_<random>
    -- Si salva solo l'hash, il prefisso in chiaro per lookup
    key_prefix      TEXT NOT NULL,                    -- "dk_live_abc1" (primi 12 char)
    key_hash        TEXT NOT NULL,                    -- SHA-256 della key completa
    name            TEXT NOT NULL DEFAULT 'Default',  -- Nome leggibile
    signing_secret  TEXT,                             -- Per HMAC (opzionale)
    environment     TEXT NOT NULL DEFAULT 'live',     -- live | test
    permissions     TEXT[] NOT NULL DEFAULT '{}',     -- Future: granular permissions
    last_used_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    revoked_at      TIMESTAMPTZ                       -- null = attiva
);

CREATE INDEX idx_api_keys_prefix ON api_keys(key_prefix) WHERE revoked_at IS NULL;
CREATE INDEX idx_api_keys_org ON api_keys(org_id);

-- ═══════════════════════════════════════════════
-- ISTANZE
-- ═══════════════════════════════════════════════

CREATE TABLE instances (
    id              UUID PRIMARY KEY,                 -- instanceId dal Phone Home Agent
    org_id          UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    name            TEXT NOT NULL,                    -- PHONE_HOME_INSTANCE_NAME
    version         TEXT,                             -- Versione bunqueue
    hostname        TEXT,
    pid             INTEGER,
    started_at      TIMESTAMPTZ,
    first_seen      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status          TEXT NOT NULL DEFAULT 'online',   -- online | degraded | offline
    last_sequence_id BIGINT NOT NULL DEFAULT 0,       -- Per detect gap
    metadata        JSONB DEFAULT '{}',               -- Dati extra
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_instances_org ON instances(org_id);
CREATE INDEX idx_instances_status ON instances(status);
CREATE INDEX idx_instances_last_seen ON instances(last_seen);

-- ═══════════════════════════════════════════════
-- ALERT RULES
-- ═══════════════════════════════════════════════

CREATE TABLE alert_rules (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    org_id          UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    name            TEXT NOT NULL,                    -- "DLQ troppo piena"
    description     TEXT,
    enabled         BOOLEAN NOT NULL DEFAULT true,

    -- Target: quale istanza/coda monitorare
    instance_id     UUID REFERENCES instances(id),    -- null = tutte le istanze
    queue_name      TEXT,                             -- null = tutte le code

    -- Condizione
    metric          TEXT NOT NULL,                    -- "stats.dlq" | "memory.heapUsed" | ...
    operator        TEXT NOT NULL,                    -- ">" | "<" | ">=" | "<=" | "=="
    threshold       DOUBLE PRECISION NOT NULL,        -- Valore soglia
    duration_sec    INTEGER NOT NULL DEFAULT 0,       -- Per quanti secondi prima di fire

    -- Notifiche
    notify_channels TEXT[] NOT NULL DEFAULT '{"email"}', -- email | slack | webhook
    notify_webhook_url TEXT,                          -- URL webhook (se canale = webhook)
    cooldown_sec    INTEGER NOT NULL DEFAULT 300,     -- Min tempo tra alert ripetuti

    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_alert_rules_org ON alert_rules(org_id) WHERE enabled = true;

-- ═══════════════════════════════════════════════
-- STORICO ALERT
-- ═══════════════════════════════════════════════

CREATE TABLE alerts_history (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    rule_id         UUID NOT NULL REFERENCES alert_rules(id) ON DELETE CASCADE,
    org_id          UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    instance_id     UUID REFERENCES instances(id),
    queue_name      TEXT,

    status          TEXT NOT NULL DEFAULT 'firing',   -- firing | resolved
    fired_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at     TIMESTAMPTZ,
    metric_value    DOUBLE PRECISION NOT NULL,        -- Valore al momento del fire
    threshold       DOUBLE PRECISION NOT NULL,        -- Soglia configurata
    message         TEXT,                             -- Messaggio generato
    notified        BOOLEAN NOT NULL DEFAULT false,

    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_alerts_history_org ON alerts_history(org_id, fired_at DESC);
CREATE INDEX idx_alerts_history_active ON alerts_history(status) WHERE status = 'firing';

-- ═══════════════════════════════════════════════
-- NOTIFICATION SETTINGS
-- ═══════════════════════════════════════════════

CREATE TABLE notification_channels (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    org_id          UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    type            TEXT NOT NULL,                    -- email | slack | webhook | pagerduty
    name            TEXT NOT NULL,                    -- "Team Slack" | "Oncall webhook"
    config          JSONB NOT NULL,                   -- Configurazione specifica per tipo
    enabled         BOOLEAN NOT NULL DEFAULT true,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ═══════════════════════════════════════════════
-- BILLING / USAGE
-- ═══════════════════════════════════════════════

CREATE TABLE usage_records (
    id              BIGSERIAL PRIMARY KEY,
    org_id          UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    period_start    TIMESTAMPTZ NOT NULL,             -- Inizio periodo (1 minuto)
    snapshots_count INTEGER NOT NULL DEFAULT 0,       -- Snapshot ricevuti
    events_count    INTEGER NOT NULL DEFAULT 0,       -- Eventi ricevuti
    instances_count INTEGER NOT NULL DEFAULT 0,       -- Istanze attive
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_usage_org_period ON usage_records(org_id, period_start DESC);
```

### 5.2 TimescaleDB — serie temporali

```sql
-- ═══════════════════════════════════════════════
-- SNAPSHOT (time-series)
-- ═══════════════════════════════════════════════

CREATE TABLE snapshots (
    time                TIMESTAMPTZ NOT NULL,
    instance_id         UUID NOT NULL,
    org_id              UUID NOT NULL,                -- Denormalizzato per query veloci

    -- Stats
    waiting             INTEGER NOT NULL DEFAULT 0,
    delayed             INTEGER NOT NULL DEFAULT 0,
    active              INTEGER NOT NULL DEFAULT 0,
    dlq                 INTEGER NOT NULL DEFAULT 0,
    completed           INTEGER NOT NULL DEFAULT 0,
    total_pushed        BIGINT NOT NULL DEFAULT 0,
    total_completed     BIGINT NOT NULL DEFAULT 0,
    total_failed        BIGINT NOT NULL DEFAULT 0,

    -- Throughput
    push_per_sec        REAL NOT NULL DEFAULT 0,
    pull_per_sec        REAL NOT NULL DEFAULT 0,
    complete_per_sec    REAL NOT NULL DEFAULT 0,
    fail_per_sec        REAL NOT NULL DEFAULT 0,

    -- Latency
    push_p50            REAL,
    push_p95            REAL,
    push_p99            REAL,
    pull_p50            REAL,
    pull_p95            REAL,
    pull_p99            REAL,
    ack_p50             REAL,
    ack_p95             REAL,
    ack_p99             REAL,

    -- Memory
    heap_used_mb        REAL NOT NULL DEFAULT 0,
    heap_total_mb       REAL NOT NULL DEFAULT 0,
    rss_mb              REAL NOT NULL DEFAULT 0,

    -- Collections
    job_index_size      INTEGER NOT NULL DEFAULT 0,
    completed_jobs_size INTEGER NOT NULL DEFAULT 0,
    processing_total    INTEGER NOT NULL DEFAULT 0,
    queued_total        INTEGER NOT NULL DEFAULT 0,

    -- Workers
    workers_total       INTEGER NOT NULL DEFAULT 0,
    workers_active      INTEGER NOT NULL DEFAULT 0,
    workers_active_jobs INTEGER NOT NULL DEFAULT 0,

    -- Storage
    storage_bytes       BIGINT NOT NULL DEFAULT 0,
    storage_healthy     BOOLEAN NOT NULL DEFAULT true
);

SELECT create_hypertable('snapshots', 'time');
CREATE INDEX idx_snapshots_instance ON snapshots(instance_id, time DESC);
CREATE INDEX idx_snapshots_org ON snapshots(org_id, time DESC);

-- Retention automatica (configurabile per piano)
-- Free: 7 giorni, Pro: 90 giorni, Enterprise: 365 giorni
SELECT add_retention_policy('snapshots', INTERVAL '90 days');

-- Compressione automatica dopo 1 giorno (riduce storage ~90%)
ALTER TABLE snapshots SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'instance_id',
    timescaledb.compress_orderby = 'time DESC'
);
SELECT add_compression_policy('snapshots', INTERVAL '1 day');

-- ═══════════════════════════════════════════════
-- PER-QUEUE METRICS (time-series)
-- ═══════════════════════════════════════════════

CREATE TABLE queue_snapshots (
    time            TIMESTAMPTZ NOT NULL,
    instance_id     UUID NOT NULL,
    org_id          UUID NOT NULL,
    queue_name      TEXT NOT NULL,
    waiting         INTEGER NOT NULL DEFAULT 0,
    delayed         INTEGER NOT NULL DEFAULT 0,
    active          INTEGER NOT NULL DEFAULT 0,
    dlq             INTEGER NOT NULL DEFAULT 0,
    paused          BOOLEAN NOT NULL DEFAULT false
);

SELECT create_hypertable('queue_snapshots', 'time');
CREATE INDEX idx_queue_snapshots_lookup
    ON queue_snapshots(instance_id, queue_name, time DESC);

ALTER TABLE queue_snapshots SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'instance_id, queue_name',
    timescaledb.compress_orderby = 'time DESC'
);
SELECT add_compression_policy('queue_snapshots', INTERVAL '1 day');
SELECT add_retention_policy('queue_snapshots', INTERVAL '90 days');

-- ═══════════════════════════════════════════════
-- EVENTS (time-series)
-- ═══════════════════════════════════════════════

CREATE TABLE events (
    time            TIMESTAMPTZ NOT NULL,
    instance_id     UUID NOT NULL,
    org_id          UUID NOT NULL,
    event_type      TEXT NOT NULL,            -- "Failed" | "Stalled" | "queue:idle" | ...
    queue_name      TEXT,
    job_id          TEXT,
    error           TEXT,
    data            JSONB
);

SELECT create_hypertable('events', 'time');
CREATE INDEX idx_events_lookup ON events(instance_id, time DESC);
CREATE INDEX idx_events_type ON events(org_id, event_type, time DESC);

SELECT add_retention_policy('events', INTERVAL '7 days');

-- ═══════════════════════════════════════════════
-- CONTINUOUS AGGREGATES (pre-calcolati per grafici veloci)
-- ═══════════════════════════════════════════════

-- Aggregato 1-minuto per grafici dettagliati (ultime 24h)
CREATE MATERIALIZED VIEW snapshots_1min
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', time) AS bucket,
    instance_id,
    org_id,
    AVG(waiting)::INT AS avg_waiting,
    AVG(active)::INT AS avg_active,
    AVG(dlq)::INT AS avg_dlq,
    MAX(waiting) AS max_waiting,
    AVG(push_per_sec) AS avg_push_per_sec,
    AVG(complete_per_sec) AS avg_complete_per_sec,
    AVG(fail_per_sec) AS avg_fail_per_sec,
    AVG(push_p50) AS avg_push_p50,
    AVG(push_p99) AS avg_push_p99,
    AVG(heap_used_mb) AS avg_heap_used_mb,
    MAX(heap_used_mb) AS max_heap_used_mb,
    AVG(rss_mb) AS avg_rss_mb,
    MAX(total_pushed) AS total_pushed,
    MAX(total_completed) AS total_completed,
    MAX(total_failed) AS total_failed
FROM snapshots
GROUP BY bucket, instance_id, org_id;

SELECT add_continuous_aggregate_policy('snapshots_1min',
    start_offset => INTERVAL '2 hours',
    end_offset => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute'
);

-- Aggregato 1-ora per grafici a lungo termine (ultimi 30 giorni)
CREATE MATERIALIZED VIEW snapshots_1hour
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS bucket,
    instance_id,
    org_id,
    AVG(waiting)::INT AS avg_waiting,
    AVG(active)::INT AS avg_active,
    MAX(waiting) AS max_waiting,
    MAX(dlq) AS max_dlq,
    AVG(push_per_sec) AS avg_push_per_sec,
    AVG(complete_per_sec) AS avg_complete_per_sec,
    AVG(push_p99) AS avg_push_p99,
    AVG(heap_used_mb) AS avg_heap_used_mb,
    MAX(heap_used_mb) AS max_heap_used_mb,
    MAX(total_pushed) AS total_pushed,
    MAX(total_completed) AS total_completed,
    MAX(total_failed) AS total_failed
FROM snapshots
GROUP BY bucket, instance_id, org_id;

SELECT add_continuous_aggregate_policy('snapshots_1hour',
    start_offset => INTERVAL '3 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour'
);
```

### 5.3 Query tipo per la dashboard

```sql
-- Ultimo stato di ogni istanza per un'org
SELECT DISTINCT ON (i.id)
    i.id, i.name, i.status, i.version, i.last_seen,
    s.waiting, s.active, s.delayed, s.dlq,
    s.push_per_sec, s.complete_per_sec,
    s.heap_used_mb, s.workers_active
FROM instances i
LEFT JOIN snapshots s ON s.instance_id = i.id
WHERE i.org_id = $1
ORDER BY i.id, s.time DESC;

-- Grafico throughput ultime 6 ore (risoluzione 1 min)
SELECT bucket, avg_push_per_sec, avg_complete_per_sec, avg_fail_per_sec
FROM snapshots_1min
WHERE instance_id = $1
  AND bucket > NOW() - INTERVAL '6 hours'
ORDER BY bucket;

-- Grafico latenza ultimi 7 giorni (risoluzione 1 ora)
SELECT bucket, avg_push_p50, avg_push_p99
FROM snapshots_1hour
WHERE instance_id = $1
  AND bucket > NOW() - INTERVAL '7 days'
ORDER BY bucket;

-- Top 10 code per DLQ
SELECT queue_name, MAX(dlq) as max_dlq, AVG(waiting)::INT as avg_waiting
FROM queue_snapshots
WHERE instance_id = $1
  AND time > NOW() - INTERVAL '5 minutes'
GROUP BY queue_name
ORDER BY max_dlq DESC
LIMIT 10;

-- Ultimi 50 eventi di errore
SELECT time, event_type, queue_name, job_id, error
FROM events
WHERE instance_id = $1
  AND event_type IN ('Failed', 'Stalled')
ORDER BY time DESC
LIMIT 50;
```

---

## 6. API Endpoints

### 6.1 Ingest (usati dal Phone Home Agent)

```
POST   /api/v1/ingest                  # Ricevi singolo snapshot
POST   /api/v1/ingest/batch            # Ricevi batch snapshot (dopo reconnect)
WS     /api/v1/stream                  # WebSocket eventi bidirezionale

Headers richiesti:
  Authorization: Bearer dk_live_<key>
  X-Instance-Id: <uuid>
  Content-Type: application/json
  Content-Encoding: gzip              (opzionale)
  X-Timestamp: <unix_ms>
  X-Signature: <hmac_sha256>          (opzionale, se signing_secret configurato)
```

### 6.2 Dashboard API (usati dal frontend)

```
# ─── Auth ───
POST   /api/v1/auth/register           # Registrazione utente
POST   /api/v1/auth/login              # Login → JWT
POST   /api/v1/auth/logout             # Logout
POST   /api/v1/auth/refresh            # Refresh token
POST   /api/v1/auth/forgot-password    # Reset password
POST   /api/v1/auth/verify-email       # Verifica email

# ─── Organizzazioni ───
GET    /api/v1/orgs                     # Lista org dell'utente
POST   /api/v1/orgs                     # Crea nuova org
GET    /api/v1/orgs/:slug              # Dettaglio org
PUT    /api/v1/orgs/:slug              # Aggiorna org
DELETE /api/v1/orgs/:slug              # Elimina org

# ─── Team ───
GET    /api/v1/orgs/:slug/members      # Lista membri
POST   /api/v1/orgs/:slug/members      # Invita membro
PUT    /api/v1/orgs/:slug/members/:id  # Cambia ruolo
DELETE /api/v1/orgs/:slug/members/:id  # Rimuovi membro

# ─── API Keys ───
GET    /api/v1/orgs/:slug/keys         # Lista API keys
POST   /api/v1/orgs/:slug/keys         # Crea API key → mostra UNA volta
DELETE /api/v1/orgs/:slug/keys/:id     # Revoca API key

# ─── Istanze ───
GET    /api/v1/orgs/:slug/instances              # Lista con ultimo stato
GET    /api/v1/orgs/:slug/instances/:id          # Dettaglio istanza
DELETE /api/v1/orgs/:slug/instances/:id          # Rimuovi istanza
GET    /api/v1/orgs/:slug/instances/:id/snapshot # Ultimo snapshot completo

# ─── Metriche (serie temporali) ───
GET    /api/v1/orgs/:slug/instances/:id/metrics
       ?metrics=waiting,active,push_per_sec
       &from=2026-03-18T00:00:00Z
       &to=2026-03-19T00:00:00Z
       &resolution=1m|5m|1h|1d

# ─── Code ───
GET    /api/v1/orgs/:slug/instances/:id/queues              # Lista code
GET    /api/v1/orgs/:slug/instances/:id/queues/:name/metrics
       ?metrics=waiting,dlq
       &from=...&to=...&resolution=...

# ─── Eventi ───
GET    /api/v1/orgs/:slug/instances/:id/events
       ?type=Failed,Stalled
       &queue=emails
       &from=...&to=...
       &limit=100&offset=0

# ─── Alert ───
GET    /api/v1/orgs/:slug/alerts/rules           # Lista regole
POST   /api/v1/orgs/:slug/alerts/rules           # Crea regola
PUT    /api/v1/orgs/:slug/alerts/rules/:id       # Aggiorna regola
DELETE /api/v1/orgs/:slug/alerts/rules/:id       # Elimina regola
GET    /api/v1/orgs/:slug/alerts/history          # Storico alert
GET    /api/v1/orgs/:slug/alerts/active           # Alert attivi ora

# ─── Notifiche ───
GET    /api/v1/orgs/:slug/notifications/channels        # Lista canali
POST   /api/v1/orgs/:slug/notifications/channels        # Crea canale
PUT    /api/v1/orgs/:slug/notifications/channels/:id    # Aggiorna
DELETE /api/v1/orgs/:slug/notifications/channels/:id    # Elimina
POST   /api/v1/orgs/:slug/notifications/channels/:id/test # Invia test

# ─── Billing ───
GET    /api/v1/orgs/:slug/billing/plan           # Piano attuale
GET    /api/v1/orgs/:slug/billing/usage          # Usage corrente
GET    /api/v1/orgs/:slug/billing/usage/history  # Storico usage
POST   /api/v1/orgs/:slug/billing/upgrade        # Upgrade piano

# ─── Real-time (WebSocket frontend) ───
WS     /api/v1/orgs/:slug/live
       # Riceve: eventi real-time + aggiornamenti stato istanze
       # Invia: subscribe/unsubscribe per istanza/coda
```

---

## 7. Multi-Tenancy

### 7.1 Isolamento dati

Ogni query ha **sempre** il filtro `org_id`. Non esiste modo di accedere ai dati di un'altra org.

```
┌─────────────────────────────────────────────────────────┐
│                    Database Layer                         │
│                                                          │
│  Org: Acme Inc (org_id = aaa)                           │
│  ├── Instance: prod-1  ──→ snapshots WHERE org_id = aaa │
│  ├── Instance: prod-2  ──→ snapshots WHERE org_id = aaa │
│  └── Instance: staging ──→ snapshots WHERE org_id = aaa │
│                                                          │
│  Org: Startup X (org_id = bbb)                          │
│  └── Instance: main    ──→ snapshots WHERE org_id = bbb │
│                                                          │
│  MAI: snapshots WHERE instance_id = ... (senza org_id)  │
└─────────────────────────────────────────────────────────┘
```

### 7.2 API Key → Org mapping

```
API Key: dk_live_abc123def456ghi789
                  ↓
           key_prefix: "dk_live_abc1"
                  ↓
           SELECT org_id FROM api_keys
           WHERE key_prefix = 'dk_live_abc1'
           AND key_hash = SHA256('dk_live_abc123def456ghi789')
           AND revoked_at IS NULL
                  ↓
           org_id = 'aaa-bbb-ccc'
```

Lookup in **O(1)** via indice su `key_prefix`. La API key completa non è mai salvata in chiaro — solo l'hash.

### 7.3 Rate limiting per organizzazione

```
Piano Free:
  - Max 1 istanza
  - Max 12 snapshot/min (= 1 ogni 5s)
  - Max 100 eventi/min
  - Retention: 7 giorni

Piano Pro ($29/mese):
  - Max 10 istanze
  - Max 120 snapshot/min (= 12 istanze × 1 ogni 5s, con margine)
  - Max 10,000 eventi/min
  - Retention: 90 giorni
  - Alerting illimitato
  - Canali notifica: email + Slack + webhook

Piano Enterprise ($99/mese):
  - Istanze illimitate
  - Snapshot/eventi illimitati
  - Retention: 365 giorni
  - SSO / SAML
  - Supporto prioritario
  - Custom retention policy
```

Rate limiting implementato in Redis:

```
Per ogni richiesta ingest:
  key = "rl:ingest:{org_id}:{minute_bucket}"
  current = INCR key
  EXPIRE key 120  (2 minuti TTL)
  if current > plan_limit → 429 Too Many Requests
```

---

## 8. Autenticazione e Sicurezza

### 8.1 Due sistemi di auth separati

```
┌─────────────────────────┐     ┌─────────────────────────┐
│   Phone Home Agent      │     │   Dashboard Frontend     │
│   (macchina)            │     │   (browser)              │
│                         │     │                          │
│  Auth: API Key          │     │  Auth: JWT + Session     │
│  Header: Authorization  │     │  Cookie: session_token   │
│  Bearer dk_live_xxx     │     │  Header: Authorization   │
│                         │     │  Bearer eyJhbG...        │
│  → Endpoint: /ingest    │     │  → Endpoint: /api/v1/*   │
│  → Rate limited per org │     │  → RBAC per ruolo        │
└─────────────────────────┘     └─────────────────────────┘
```

### 8.2 HMAC Signature (opzionale ma raccomandato)

Il Phone Home Agent può firmare ogni richiesta:

```
1. Agent serializza il body JSON
2. Calcola: HMAC-SHA256(body, signing_secret)
3. Invia: X-Signature: <hex_digest>

Server:
1. Riceve body + signature
2. Recupera signing_secret dalla API key
3. Calcola: expected = HMAC-SHA256(body, signing_secret)
4. Confronta con timing-safe comparison
5. Se diverso → 401
```

Protegge da:
- Man-in-the-middle (anche se HTTPS è già sufficiente)
- Replay attack (combinato con X-Timestamp, rifiuta se > 5 min)
- Tampering del payload

### 8.3 RBAC (Role-Based Access Control)

```
owner:    Tutto. Può eliminare org, gestire billing.
admin:    Gestisce istanze, alert, API keys, invita membri.
member:   Vede tutto, crea alert, non gestisce team/billing.
viewer:   Solo lettura. Vede dashboard, non configura niente.
```

Implementato come middleware:

```typescript
function requireRole(...roles: Role[]) {
  return async (c: Context, next: Next) => {
    const member = await getOrgMember(c.get('userId'), c.get('orgId'));
    if (!member || !roles.includes(member.role)) {
      return c.json({ error: 'Forbidden' }, 403);
    }
    await next();
  };
}

// Uso
app.delete('/api/v1/orgs/:slug', requireRole('owner'), deleteOrg);
app.post('/api/v1/orgs/:slug/keys', requireRole('owner', 'admin'), createApiKey);
app.get('/api/v1/orgs/:slug/instances', requireRole('owner', 'admin', 'member', 'viewer'), listInstances);
```

---

## 9. Sistema di Alerting

### 9.1 Regole configurabili

L'utente crea regole dalla dashboard:

```typescript
interface AlertRule {
  name: string;              // "DLQ crescente su prod"
  metric: string;            // Cosa monitorare
  operator: '>' | '<' | '>=' | '<=' | '==';
  threshold: number;         // Valore soglia
  duration_sec: number;      // Per quanto tempo prima di fire (0 = immediato)
  instance_id?: string;      // Specifica istanza (null = tutte)
  queue_name?: string;       // Specifica coda (null = tutte)
  notify_channels: string[]; // Dove notificare
  cooldown_sec: number;      // Min tempo tra alert ripetuti
}
```

### 9.2 Metriche monitorabili

```
# ─── Stats ───
stats.waiting              # Job in attesa (totale)
stats.delayed              # Job ritardati
stats.active               # Job attivi
stats.dlq                  # Job nel DLQ
stats.total_failed         # Fallimenti cumulativi (utile con rate of change)

# ─── Throughput ───
throughput.push_per_sec    # Job aggiunti al secondo
throughput.complete_per_sec # Job completati al secondo
throughput.fail_per_sec    # Job falliti al secondo

# ─── Latenza ───
latency.push_p99           # Latenza push P99
latency.pull_p99           # Latenza pull P99
latency.ack_p99            # Latenza ack P99

# ─── Memoria ───
memory.heap_used_mb        # Heap usato
memory.rss_mb              # RSS totale

# ─── Worker ───
workers.active             # Worker attivi
workers.total              # Worker registrati

# ─── Storage ───
storage.size_mb            # Dimensione database

# ─── Per-coda (richiede queue_name) ───
queue.waiting              # Waiting nella coda specifica
queue.delayed              # Delayed nella coda specifica
queue.active               # Active nella coda specifica
queue.dlq                  # DLQ nella coda specifica

# ─── Stato istanza (speciale) ───
instance.status            # online=1, degraded=0.5, offline=0
instance.last_seen_ago_sec # Secondi dall'ultimo heartbeat
```

### 9.3 Esempi di regole comuni

```yaml
# 1. Istanza offline
- name: "Istanza down"
  metric: instance.status
  operator: "=="
  threshold: 0              # offline
  duration_sec: 30
  notify: [email, slack]

# 2. DLQ in crescita
- name: "DLQ > 100 su qualsiasi coda"
  metric: queue.dlq
  operator: ">"
  threshold: 100
  duration_sec: 60          # Deve restare > 100 per 1 minuto
  notify: [slack]

# 3. Memoria alta
- name: "Heap > 1GB"
  metric: memory.heap_used_mb
  operator: ">"
  threshold: 1024
  duration_sec: 120         # 2 minuti sostenuti
  notify: [email]

# 4. Throughput crollato
- name: "Throughput < 10 job/s"
  metric: throughput.complete_per_sec
  operator: "<"
  threshold: 10
  duration_sec: 300         # 5 minuti
  instance_id: "prod-1-uuid"
  notify: [pagerduty]

# 5. Nessun worker attivo
- name: "Zero worker"
  metric: workers.active
  operator: "=="
  threshold: 0
  duration_sec: 30
  notify: [email, slack]
```

### 9.4 Ciclo di vita di un alert

```
1. Alert Evaluator controlla la regola ogni 10s
2. Condizione soddisfatta:
   - Prima volta → registra timestamp in "firing_since" (in-memory)
   - Se firing_since + duration_sec < now → FIRE ALERT
     - Crea record in alerts_history (status: firing)
     - Invia notifica su tutti i canali configurati
     - Registra cooldown (non ri-notificare per cooldown_sec)

3. Condizione non più soddisfatta:
   - Se era in firing → RESOLVE
     - UPDATE alerts_history SET status = 'resolved', resolved_at = NOW()
     - Invia notifica "resolved" (se configurato)
   - Reset firing_since

4. Re-notifica:
   - Se condizione persiste oltre cooldown_sec → ri-notifica
   - Messaggio: "Alert ancora attivo da X minuti"
```

### 9.5 Canali di notifica

```typescript
// Email (via Resend)
{
  type: 'email',
  config: {
    recipients: ['devops@acme.com', 'oncall@acme.com']
  }
}

// Slack (via incoming webhook)
{
  type: 'slack',
  config: {
    webhook_url: 'https://hooks.slack.com/services/T.../B.../xxx',
    channel: '#alerts',         // Override (opzionale)
    mention: '@oncall'          // Mention (opzionale)
  }
}

// Webhook generico
{
  type: 'webhook',
  config: {
    url: 'https://api.acme.com/alerts',
    headers: { 'X-Alert-Source': 'bunqueue' },
    secret: 'webhook-hmac-secret'
  }
}

// PagerDuty
{
  type: 'pagerduty',
  config: {
    routing_key: 'R0123456789ABCDEF',
    severity: 'critical'        // critical | error | warning | info
  }
}
```

---

## 10. Dashboard Frontend

### 10.1 Pagine

#### Home / Overview (`/app/:org`)

```
┌────────────────────────────────────────────────────────────────────┐
│  bunqueue Dashboard                              Acme Inc  │ ⚙  │
├────────────────────────────────────────────────────────────────────┤
│                                                                    │
│  Instances (3)                              🔴 1 alert active     │
│                                                                    │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐ │
│  │ 🟢 prod-eu-1     │  │ 🟢 prod-us-1     │  │ 🟡 staging       │ │
│  │ v2.7.0           │  │ v2.7.0           │  │ v2.7.0           │ │
│  │                  │  │                  │  │                  │ │
│  │ Waiting:    234  │  │ Waiting:    567  │  │ Waiting:     12  │ │
│  │ Active:      50  │  │ Active:     100  │  │ Active:       3  │ │
│  │ DLQ:          2  │  │ DLQ:          0  │  │ DLQ:          0  │ │
│  │                  │  │                  │  │                  │ │
│  │ ████████░░ 1.2k/s│  │ █████████░ 2.4k/s│  │ ██░░░░░░░░ 50/s │ │
│  │ Throughput       │  │ Throughput       │  │ Throughput       │ │
│  │                  │  │                  │  │                  │ │
│  │ Workers: 5/5     │  │ Workers: 10/10   │  │ Workers: 2/2     │ │
│  │ Heap: 245 MB     │  │ Heap: 512 MB     │  │ Heap: 89 MB      │ │
│  │ Last seen: 2s    │  │ Last seen: 1s    │  │ Last seen: 12s   │ │
│  └──────────────────┘  └──────────────────┘  └──────────────────┘ │
│                                                                    │
│  ── Aggregate Throughput (last 6h) ──────────────────────────────  │
│  ┌────────────────────────────────────────────────────────────┐    │
│  │     ╱╲     ╱╲                                              │    │
│  │   ╱╱  ╲╲╱╱  ╲╲         ╱╲                                 │    │
│  │ ╱╱            ╲╲      ╱╱  ╲╲                               │    │
│  │╱                ╲╲╱╱╱╱      ╲╲────────────────             │    │
│  │ push/s ── complete/s ── fail/s                             │    │
│  └────────────────────────────────────────────────────────────┘    │
│                                                                    │
│  ── Recent Events ───────────────────────────────────────────────  │
│  │ 2s ago  │ Failed  │ emails  │ job-abc │ SMTP timeout          │ │
│  │ 5s ago  │ Stalled │ orders  │ job-def │ No heartbeat 30s      │ │
│  │ 12s ago │ Failed  │ emails  │ job-ghi │ Invalid recipient     │ │
│  └─────────┴─────────┴─────────┴─────────┴───────────────────────  │
└────────────────────────────────────────────────────────────────────┘
```

#### Instance Detail (`/app/:org/instances/:id`)

```
┌────────────────────────────────────────────────────────────────────┐
│  ← Back    prod-eu-1                     🟢 Online    v2.7.0     │
├────────────────────────────────────────────────────────────────────┤
│                                                                    │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐   │
│  │ Waiting │ │ Active  │ │ Delayed │ │  DLQ    │ │Complete │   │
│  │   234   │ │   50    │ │   12    │ │    2    │ │  1.2M   │   │
│  │  ↑ +12  │ │  ─ 0   │ │  ↓ -3  │ │  ↑ +1  │ │  ↑ +5k  │   │
│  └─────────┘ └─────────┘ └─────────┘ └─────────┘ └─────────┘   │
│                                                                    │
│  [Throughput] [Latency] [Memory] [Workers] [Queues] [Events]     │
│                                                                    │
│  ── Throughput ──────────────────── Range: [1h] 6h  24h  7d ──   │
│  ┌────────────────────────────────────────────────────────────┐   │
│  │  4k ┤                                                      │   │
│  │     │    ╱╲        ╱╲                                      │   │
│  │  2k ┤  ╱╱  ╲╲    ╱╱  ╲╲       ╱╲                          │   │
│  │     │╱╱      ╲╲╱╱      ╲╲╱╲╱╱╱  ╲╲────                    │   │
│  │  0  ┤─────────────────────────────────                     │   │
│  │     └──────┴──────┴──────┴──────┴───────                   │   │
│  │      14:00  14:15  14:30  14:45  15:00                     │   │
│  └────────────────────────────────────────────────────────────┘   │
│                                                                    │
│  ── Queues (12) ─────────────────────────────────────────────────  │
│  │ Queue      │ Waiting │ Active │ DLQ │ Throughput │ Status    │ │
│  │ emails     │    120  │    25  │   2 │    450/s   │ ▶ Active  │ │
│  │ orders     │     89  │    20  │   0 │    320/s   │ ▶ Active  │ │
│  │ analytics  │     25  │     5  │   0 │     80/s   │ ▶ Active  │ │
│  │ webhooks   │      0  │     0  │   0 │      0/s   │ ⏸ Paused  │ │
│  └────────────┴─────────┴────────┴─────┴────────────┴────────────  │
│                                                                    │
│  ── Workers (5) ─────────────────────────────────────────────────  │
│  │ Worker         │ Queues    │ Active │ Processed │ Failed     │ │
│  │ worker-1 (pid 234) │ emails   │  5/5   │  12,345   │     3     │ │
│  │ worker-2 (pid 235) │ emails   │  5/5   │  11,890   │     1     │ │
│  │ worker-3 (pid 236) │ orders   │ 10/10  │  45,678   │    12     │ │
│  └────────────────┴──────────┴────────┴───────────┴──────────────  │
└────────────────────────────────────────────────────────────────────┘
```

#### Alerts (`/app/:org/alerts`)

```
┌────────────────────────────────────────────────────────────────────┐
│  Alerts                                        [+ New Alert Rule]  │
├────────────────────────────────────────────────────────────────────┤
│                                                                    │
│  ── Active Alerts (1) ────────────────────────────────────────── │
│  │ 🔴 DLQ > 100 su emails                                       │ │
│  │    Instance: prod-eu-1  │  Queue: emails  │  Value: 142       │ │
│  │    Firing since: 5 min ago  │  Notified: Slack, Email         │ │
│  └───────────────────────────────────────────────────────────────  │
│                                                                    │
│  ── Alert Rules (4) ─────────────────────────────────────────────  │
│  │ ✅ Istanza down          │ instance.status == 0     │ 30s     │ │
│  │ ✅ DLQ > 100             │ queue.dlq > 100          │ 60s     │ │
│  │ ✅ Heap > 1GB            │ memory.heap_used_mb > 1024│ 120s   │ │
│  │ ⬚ Zero worker (disabled)│ workers.active == 0       │ 30s     │ │
│  └───────────────────────────────────────────────────────────────  │
│                                                                    │
│  ── Alert History (last 7 days) ─────────────────────────────────  │
│  │ 2h ago   │ 🟢 Resolved │ DLQ > 100       │ prod-eu-1 │ 45min │ │
│  │ 1d ago   │ 🟢 Resolved │ Istanza down    │ staging   │  2min │ │
│  │ 3d ago   │ 🟢 Resolved │ Heap > 1GB      │ prod-eu-1 │ 12min │ │
│  └──────────┴─────────────┴─────────────────┴───────────┴────────  │
└────────────────────────────────────────────────────────────────────┘
```

#### Settings (`/app/:org/settings`)

```
Tabs: [General] [API Keys] [Team] [Notifications] [Billing]

── API Keys ────────────────────────────────────────────────────
│ Name          │ Key Prefix    │ Environment │ Last Used      │
│ Production    │ dk_live_abc1  │ live        │ 2 seconds ago  │ [Revoke]
│ Staging       │ dk_live_def2  │ live        │ 12 seconds ago │ [Revoke]
│ Dev (revoked) │ dk_test_ghi3  │ test        │ 3 days ago     │ Revoked

[+ Create API Key]

── Team Members ────────────────────────────────────────────────
│ Name            │ Email              │ Role   │ Joined        │
│ Marco Rossi     │ marco@acme.com     │ Owner  │ 2026-01-15    │
│ Lisa Chen       │ lisa@acme.com      │ Admin  │ 2026-02-01    │ [Change Role] [Remove]
│ Alex Kim        │ alex@acme.com      │ Member │ 2026-03-10    │ [Change Role] [Remove]

[+ Invite Member]
```

### 10.2 Real-time nel frontend

Il frontend apre un WebSocket verso il backend dashboard:

```typescript
// hooks/useWebSocket.ts
const ws = new WebSocket(`wss://dashboard.bunqueue.dev/api/v1/orgs/${org}/live`);

ws.onopen = () => {
  // Sottoscrivi agli aggiornamenti
  ws.send(JSON.stringify({
    type: 'subscribe',
    instances: ['all'],          // o specifici UUID
    events: ['snapshot', 'event', 'alert', 'status_change']
  }));
};

ws.onmessage = (msg) => {
  const data = JSON.parse(msg.data);

  switch (data.type) {
    case 'snapshot':
      // Aggiorna cards istanza (waiting, active, throughput)
      updateInstanceState(data.instanceId, data.snapshot);
      break;

    case 'event':
      // Aggiungi al feed eventi
      prependEvent(data.event);
      break;

    case 'alert':
      // Mostra banner alert
      showAlertBanner(data.alert);
      break;

    case 'status_change':
      // Cambia colore pallino istanza (verde → giallo → rosso)
      updateInstanceStatus(data.instanceId, data.status);
      break;
  }
};
```

Flusso server-side del WebSocket frontend:

```
Phone Home Agent → POST /api/v1/ingest → Ingest Service
                                              │
                                              ▼
                                    Redis PUBLISH "ingest:{org_id}"
                                              │
                                              ▼
                                    WS Hub ascolta Redis
                                              │
                                              ▼
                                    Fan-out a tutti i browser
                                    connessi per questa org
```

---

## 11. Billing e Metering

### 11.1 Piani

```
┌─────────────┬──────────────┬──────────────┬──────────────────┐
│             │    Free      │     Pro      │   Enterprise     │
├─────────────┼──────────────┼──────────────┼──────────────────┤
│ Prezzo      │ $0/mese      │ $29/mese     │ $99/mese         │
│ Istanze     │ 1            │ 10           │ Illimitate       │
│ Retention   │ 7 giorni     │ 90 giorni    │ 365 giorni       │
│ Snapshot/min│ 12           │ 120          │ Illimitati       │
│ Eventi/min  │ 100          │ 10,000       │ Illimitati       │
│ Alert rules │ 3            │ 25           │ Illimitate       │
│ Team members│ 1            │ 10           │ Illimitati       │
│ Notifiche   │ Solo email   │ Email+Slack  │ Tutti i canali   │
│             │              │ +Webhook     │ +PagerDuty       │
│ Support     │ Community    │ Email        │ Prioritario      │
│ SSO/SAML    │ No           │ No           │ Sì               │
│ API access  │ Read-only    │ Full         │ Full + Admin     │
└─────────────┴──────────────┴──────────────┴──────────────────┘
```

### 11.2 Come si conta l'uso

```
Metering ogni 1 minuto:
  1. Conta snapshot per org nell'ultimo minuto
  2. Conta eventi per org nell'ultimo minuto
  3. Conta istanze attive (status != offline)
  4. Salva in usage_records

Billing mensile:
  1. Al primo del mese: aggrega usage_records del mese precedente
  2. Calcola: max_instances_concurrent, total_snapshots, total_events
  3. Se supera i limiti del piano → sovrapprezzo o upgrade suggerito
  4. Genera fattura (Stripe o LemonSqueezy)
```

### 11.3 Enforcement

```
Quando un'org è rate_limited:
  - Ingest Service accetta 1 snapshot ogni 60s (invece di 5s)
  - Dashboard mostra banner: "Quota exceeded. Upgrade plan for real-time data."
  - I dati non vengono persi — solo la risoluzione è ridotta
  - API key non viene revocata — il servizio continua (degradato)
```

---

## 12. Deployment

### 12.1 Infrastruttura minima per partire

```
┌─────────────────────────────────────────────┐
│              Fly.io / Railway                │
│                                             │
│  ┌──────────────────┐                       │
│  │  App Server       │  ← 1 istanza Bun     │
│  │  (Hono + React)   │     256MB RAM         │
│  │  Port: 3000       │     $5/mese           │
│  └────────┬──────────┘                       │
│           │                                  │
│  ┌────────▼──────────┐                       │
│  │  PostgreSQL 16    │  ← Con TimescaleDB    │
│  │  + TimescaleDB    │     1GB storage       │
│  │                   │     $15/mese          │
│  └───────────────────┘                       │
│                                              │
│  ┌───────────────────┐                       │
│  │  Redis             │  ← Per pub/sub + RL  │
│  │                   │     25MB              │
│  │                   │     $5/mese (Upstash) │
│  └───────────────────┘                       │
│                                              │
│  Costo: ~$25/mese                            │
└─────────────────────────────────────────────┘
```

### 12.2 Scaling

```
Fase 1 (0-100 istanze):
  - Singolo server Bun
  - PostgreSQL singolo con TimescaleDB
  - Redis singolo (Upstash)
  - Costo: ~$25/mese

Fase 2 (100-1000 istanze):
  - 2-3 server Bun dietro load balancer
  - PostgreSQL con read replica
  - Redis cluster
  - CDN per frontend statico
  - Costo: ~$100-200/mese

Fase 3 (1000+ istanze):
  - Ingest separato dal web (microservizi)
  - TimescaleDB dedicato (Timescale Cloud)
  - Redis Cluster
  - Kubernetes
  - Costo: ~$500+/mese
```

### 12.3 Docker Compose per sviluppo locale

```yaml
version: '3.8'
services:
  db:
    image: timescale/timescaledb:latest-pg16
    environment:
      POSTGRES_DB: bunqueue_dashboard
      POSTGRES_USER: bunqueue
      POSTGRES_PASSWORD: localdev
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"

  server:
    build: ./packages/server
    ports:
      - "3000:3000"
    environment:
      DATABASE_URL: postgres://bunqueue:localdev@db:5432/bunqueue_dashboard
      REDIS_URL: redis://redis:6379
      JWT_SECRET: local-dev-secret
    depends_on:
      - db
      - redis

volumes:
  pgdata:
```

---

## 13. Onboarding Cliente

### 13.1 Flusso completo

```
Step 1: Registrazione
  └── dashboard.bunqueue.dev/register
      └── Email + password → verifica email

Step 2: Crea organizzazione
  └── Nome org + slug (URL)
      └── Org creata, piano Free attivato

Step 3: Genera API Key
  └── Click "Create API Key"
      └── Mostra key UNA VOLTA: dk_live_abc123def456ghi789
      └── Mostra signing_secret (opzionale): whsec_xyz789...
      └── Mostra istruzioni di configurazione:

          ┌─────────────────────────────────────────────────────┐
          │  Add these environment variables to your bunqueue:   │
          │                                                     │
          │  PHONE_HOME_URL=https://dashboard.bunqueue.dev      │
          │  PHONE_HOME_API_KEY=dk_live_abc123def456ghi789      │
          │                                                     │
          │  Then restart bunqueue. Your instance will appear    │
          │  on this dashboard within 5 seconds.                │
          │                                                     │
          │  [Copy to clipboard]                                │
          └─────────────────────────────────────────────────────┘

Step 4: Attesa connessione
  └── Dashboard mostra "Waiting for first instance..."
      └── Polling ogni 2s: "Any instances for this org?"
          └── Quando arriva il primo snapshot:
              └── "🟢 Instance connected! Redirecting to dashboard..."

Step 5: Dashboard attiva
  └── L'utente vede la sua istanza con tutti i dati
  └── Suggerimento: "Set up your first alert rule →"
```

### 13.2 Cosa vede il cliente su bunqueue al boot

```
[bunqueue] Starting server v2.7.0
[bunqueue] TCP server listening on 0.0.0.0:6789
[bunqueue] HTTP server listening on 0.0.0.0:6790
[bunqueue] Phone Home: connecting to dashboard.bunqueue.dev...
[bunqueue] Phone Home: connected ✓ (instance: prod-eu-1, id: 550e8400...)
[bunqueue] Phone Home: first snapshot sent (2.3 KB)
[bunqueue] Phone Home: WebSocket stream connected
[bunqueue] Ready.
```

Se la dashboard non è raggiungibile:

```
[bunqueue] Phone Home: connection failed (ECONNREFUSED), retrying in 2s...
[bunqueue] Phone Home: connection failed, retrying in 4s...
[bunqueue] Phone Home: buffering snapshots locally (3 buffered)
[bunqueue] Phone Home: circuit breaker OPEN, pausing for 60s
[bunqueue]
[bunqueue] Note: Phone Home is optional. Your queue server is fully operational.
[bunqueue] Snapshots are buffered and will be sent when the dashboard is reachable.
```

---

## 14. Roadmap Fasi

### Fase 1 — MVP (2-3 settimane)

**Phone Home Agent (dentro bunqueue):**
- [ ] `phoneHomeAgent.ts` — orchestratore con timer
- [ ] `snapshotCollector.ts` — raccoglie dati da manager esistenti
- [ ] `httpSender.ts` — POST snapshot con retry
- [ ] `instanceId.ts` — genera/persisti UUID
- [ ] `config.ts` — parsing env vars
- [ ] Test unitari per ogni componente

**Dashboard Server:**
- [ ] Setup progetto Hono + Drizzle + PostgreSQL
- [ ] Schema DB (organizations, users, api_keys, instances, snapshots)
- [ ] `POST /api/v1/ingest` — ricevi e salva snapshot
- [ ] Auth: registrazione + login + JWT
- [ ] API key: creazione + validazione
- [ ] `GET /api/v1/orgs/:slug/instances` — lista istanze
- [ ] `GET /api/v1/orgs/:slug/instances/:id/metrics` — serie temporali
- [ ] Health checker (rileva offline)

**Dashboard Frontend:**
- [ ] Login/Register
- [ ] Overview con instance cards
- [ ] Instance detail con grafici base (throughput, latency)
- [ ] Settings: API key management

### Fase 2 — Produzione (2-3 settimane)

- [ ] WebSocket canale 2 (eventi real-time)
- [ ] Event feed live nel frontend
- [ ] Alert rules: CRUD + evaluator
- [ ] Notifiche email (Resend)
- [ ] Buffer offline + circuit breaker nell'agent
- [ ] HMAC signature verification
- [ ] Rate limiting per org
- [ ] Retention cleaner
- [ ] TimescaleDB continuous aggregates
- [ ] Queue detail page
- [ ] Worker detail page

### Fase 3 — Monetizzazione (2-3 settimane)

- [ ] Piani Free/Pro/Enterprise
- [ ] Metering service
- [ ] Integrazione Stripe/LemonSqueezy
- [ ] Billing page
- [ ] Team management (inviti, ruoli)
- [ ] Notifiche Slack
- [ ] Notifiche webhook
- [ ] PagerDuty integration
- [ ] SSO/SAML (enterprise)

### Fase 4 — Growth (ongoing)

- [ ] Dashboard pubblica di status (per i clienti dei clienti)
- [ ] Mobile app (React Native)
- [ ] Grafana plugin
- [ ] Terraform provider
- [ ] SDK per dashboard embedded
- [ ] Anomaly detection (ML-based)
- [ ] Compare view (due istanze fianco a fianco)
- [ ] Export CSV/PDF report
