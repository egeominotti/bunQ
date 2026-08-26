type MaintenanceReporter = (subsystem: string, error: unknown) => void;

interface PendingFlight {
  operation: () => Promise<unknown>;
  waiters: Array<() => void>;
}

interface SubsystemFlight {
  pending: PendingFlight | null;
  loop: Promise<void>;
}

/** Serialize maintenance per subsystem and drain every admitted execution on close. */
export class PostgresMaintenanceFlights {
  private readonly flights = new Map<string, SubsystemFlight>();
  private accepting = true;

  constructor(private readonly report: MaintenanceReporter) {}

  run(subsystem: string, operation: () => Promise<unknown>): Promise<void> {
    if (!this.accepting) return Promise.resolve();
    const existing = this.flights.get(subsystem);
    if (existing) return this.replacePending(existing, operation);

    let resolve!: () => void;
    const result = new Promise<void>((accept) => {
      resolve = accept;
    });
    const first: PendingFlight = { operation, waiters: [resolve] };
    const flight = {} as SubsystemFlight;
    flight.pending = first;
    flight.loop = this.runLoop(subsystem, flight);
    this.flights.set(subsystem, flight);
    return result;
  }

  close(): void {
    this.accepting = false;
  }

  async drain(): Promise<void> {
    await Promise.all([...this.flights.values()].map(({ loop }) => loop));
  }

  private replacePending(
    flight: SubsystemFlight,
    operation: () => Promise<unknown>
  ): Promise<void> {
    let resolve!: () => void;
    const result = new Promise<void>((accept) => {
      resolve = accept;
    });
    if (flight.pending) {
      flight.pending.operation = operation;
      flight.pending.waiters.push(resolve);
    } else {
      flight.pending = { operation, waiters: [resolve] };
    }
    return result;
  }

  private async runLoop(subsystem: string, flight: SubsystemFlight): Promise<void> {
    while (flight.pending) {
      const current = flight.pending;
      flight.pending = null;
      try {
        await current.operation();
        this.report(subsystem, null);
      } catch (error) {
        this.report(subsystem, error);
      } finally {
        for (const resolve of current.waiters) resolve();
      }
      await Promise.resolve();
    }
    if (this.flights.get(subsystem) === flight) this.flights.delete(subsystem);
  }
}
