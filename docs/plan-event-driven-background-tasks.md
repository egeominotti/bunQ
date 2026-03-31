# Plan: Event-Driven Background Tasks

## Problema

Il dependency processor usa `setInterval` con polling a 100ms per controllare se ci sono job completati che hanno sbloccato dipendenze. Questo introduce fino a 100ms di latenza su ogni hop della catena di dipendenze.

**Flusso attuale:**
```
ack.ts → onJobCompleted(id) → pendingDepChecks.add(id) → [⏱️ 100ms] → processPendingDependencies()
```

**Flusso nuovo:**
```
ack.ts → onJobCompleted(id) → pendingDepChecks.add(id) → [⚡ microtask] → processPendingDependencies()
```

Latenza: **~100ms → ~0ms** (prossimo microtask)

---

## Architettura

### Pattern: Microtask Coalescing con Dirty Flag

```
onJobCompleted(id1)  ─┐
onJobCompleted(id2)  ─┤  stesso tick sincrono
onJobCompleted(id3)  ─┘
                       │
                       ▼
              depFlushScheduled = true
              queueMicrotask(() => ...)
                       │
                       ▼  (prossimo microtask)
              processPendingDependencies()
              processa id1, id2, id3 in batch
```

### Gestione Reentrancy

```
runDependencyFlush()
  │
  ├─ while (pendingDepChecks.size > 0)
  │    │
  │    ├─ processPendingDependencies()  ← async, await lock
  │    │    │
  │    │    └─ Durante l'await, arriva onJobCompleted(id4)
  │    │       → pendingDepChecks.add(id4)
  │    │       → scheduleDependencyFlush() → depFlushRunning=true, skip
  │    │
  │    └─ Loop ricomincia → processa id4
  │
  └─ depFlushRunning = false
     Se pendingDepChecks non vuoto → ri-schedula
```

### Safety Fallback

Il `setInterval` resta come safety net ma passa da 100ms a 30s. Gestisce edge case come recovery dopo crash.

---

## File da Modificare

### 1. NUOVO: `src/application/taskErrorTracking.ts` (~40 righe)

Estrazione del tracking errori da `backgroundTasks.ts` per riuso in `queueManager.ts`.

```typescript
// Costanti e interfacce
const MAX_CONSECUTIVE_FAILURES = 5;

interface TaskErrorState {
  consecutiveFailures: number;
  lastError?: string;
  lastFailureAt?: number;
}

// Funzioni esportate
export function handleTaskError(taskName: string, err: unknown): void
export function handleTaskSuccess(taskName: string): void
export function getTaskErrorStats(): Record<string, TaskErrorState>
```

**Motivazione:** `queueManager.ts` importa già da `backgroundTasks.ts`. Estrarre il tracking evita dipendenze circolari e rispetta il Single Responsibility Principle.

---

### 2. `src/application/backgroundTasks.ts`

**Rimuovere** (~30 righe):
- `MAX_CONSECUTIVE_FAILURES`, `TaskErrorState`, `taskErrors`
- `handleTaskError()`, `handleTaskSuccess()`, `getTaskErrorStats()`

**Aggiungere** (~3 righe):
```typescript
import { handleTaskError, handleTaskSuccess, getTaskErrorStats } from './taskErrorTracking';
```

**Modificare** il `depCheckInterval` (riga 111-119):
```typescript
// PRIMA: polling aggressivo
const depCheckInterval = setInterval(() => {
  processPendingDependencies(ctx)
    .then(() => handleTaskSuccess('dependency'))
    .catch((err) => handleTaskError('dependency', err));
}, ctx.config.dependencyCheckMs); // 100ms

// DOPO: safety fallback
const depCheckInterval = setInterval(() => {
  if (ctx.pendingDepChecks.size === 0) return; // guard
  processPendingDependencies(ctx)
    .then(() => handleTaskSuccess('dependency'))
    .catch((err) => handleTaskError('dependency', err));
}, ctx.config.dependencyCheckMs); // 30_000ms
```

---

### 3. `src/application/types.ts`

```typescript
// PRIMA
dependencyCheckMs: 100,

// DOPO
dependencyCheckMs: 30_000, // Safety fallback; event-driven gestisce il fast path
```

---

### 4. `src/application/queueManager.ts`

**Nuovi campi privati:**
```typescript
private depFlushScheduled = false;
private depFlushRunning = false;
```

**Nuovi metodi:**
```typescript
/**
 * Schedula dependency flush al prossimo microtask.
 * Coalesce multiple chiamate nello stesso tick.
 */
private scheduleDependencyFlush(): void {
  if (this.depFlushScheduled) return;
  this.depFlushScheduled = true;
  queueMicrotask(() => {
    this.depFlushScheduled = false;
    if (this.depFlushRunning) return;
    this.runDependencyFlush();
  });
}

/**
 * Esegue il flush delle dipendenze con loop per reentrancy.
 * Il while-loop gestisce nuove completions durante l'await.
 */
private async runDependencyFlush(): Promise<void> {
  this.depFlushRunning = true;
  try {
    while (this.pendingDepChecks.size > 0) {
      await processPendingDependencies(
        this.contextFactory.getBackgroundContext()
      );
      handleTaskSuccess('dependency');
    }
  } catch (err) {
    handleTaskError('dependency', err);
  } finally {
    this.depFlushRunning = false;
    if (this.pendingDepChecks.size > 0) {
      this.scheduleDependencyFlush();
    }
  }
}
```

**Metodi modificati:**
```typescript
// PRIMA
private onJobCompleted(completedId: JobId): void {
  this.pendingDepChecks.add(completedId);
}

private onJobsCompleted(completedIds: JobId[]): void {
  for (const id of completedIds) this.pendingDepChecks.add(id);
}

// DOPO
private onJobCompleted(completedId: JobId): void {
  this.pendingDepChecks.add(completedId);
  this.scheduleDependencyFlush();
}

private onJobsCompleted(completedIds: JobId[]): void {
  for (const id of completedIds) this.pendingDepChecks.add(id);
  this.scheduleDependencyFlush();
}
```

---

### 5. NUOVO: `test/event-driven-deps.test.ts` (~80 righe)

| Test | Verifica |
|------|----------|
| Risoluzione immediata | Parent ack → child pullabile nello stesso tick |
| Coalescing | 10 ack paralleli → unica chiamata `processPendingDependencies` |
| Reentrancy | Completions durante flush async non vengono perse |
| Error resilience | Errore nel flush → tracciato, completions successive funzionano |

---

## Riepilogo Quantitativo

| Operazione | File | Righe |
|-----------|------|-------|
| Nuovo file | `taskErrorTracking.ts` | +40 |
| Nuovo file | `test/event-driven-deps.test.ts` | +80 |
| Aggiunte | `queueManager.ts` | +25 |
| Modifiche | `backgroundTasks.ts` | -30 / +5 |
| Modifiche | `types.ts` | 1 |
| **Totale** | **5 file** | **~150 righe nette** |

---

## Rischi e Mitigazioni

| Rischio | Probabilita | Mitigazione |
|---------|------------|-------------|
| Microtask starvation sotto carico estremo | Bassa | `processPendingDependencies` drena tutto il Set in una chiamata; il while-loop gira tipicamente 1 volta |
| Test esistenti che dipendono da `dependencyCheckMs: 100` | Media | Il fallback a 30s non cambia il comportamento dei test che usano l'API (push/ack/pull). I test che impostano `dependencyCheckMs: 50` continueranno a funzionare col fallback |
| Backward compatibility config | Bassa | Chi imposta esplicitamente `dependencyCheckMs` controlla il fallback; il fast path event-driven funziona sempre |

---

## Sviluppi Futuri (non in questo PR)

1. **DLQ Timer-Scheduled**: Sostituire il polling 60s con `setTimeout` basato su `nextRetryAt`
2. **SQLite mmap**: `PRAGMA mmap_size = 268435456` per letture piu veloci
3. **Stall detection event-driven**: Supplementare il polling con heartbeat timeout tracking
