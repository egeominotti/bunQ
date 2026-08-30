export interface JourneyStep {
  detail: string;
  label: string;
  tone: 'default' | 'failure' | 'retry' | 'success';
}

export interface JobJourney {
  label: string;
  steps: JourneyStep[];
}

export const JOB_JOURNEYS = {
  success: {
    label: 'Succeeds first time',
    steps: [
      {
        label: 'Producer',
        detail: 'queue.add() persists the job and returns its ID.',
        tone: 'default',
      },
      {
        label: 'Ready queue',
        detail: 'The job is eligible and waits in scheduling order.',
        tone: 'default',
      },
      {
        label: 'Worker',
        detail: 'One worker claims the job and owns its active attempt.',
        tone: 'default',
      },
      {
        label: 'Completed',
        detail: 'The ACK saves the result and releases the concurrency slot.',
        tone: 'success',
      },
    ],
  },
  retry: {
    label: 'Fails once, then succeeds',
    steps: [
      {
        label: 'Producer',
        detail: 'queue.add() persists the job with attempts and backoff.',
        tone: 'default',
      },
      {
        label: 'Ready queue',
        detail: 'The first attempt becomes eligible for a worker.',
        tone: 'default',
      },
      {
        label: 'Attempt 1',
        detail: 'The worker throws, so bunqueue records a failed attempt.',
        tone: 'failure',
      },
      {
        label: 'Retry delay',
        detail: 'Backoff keeps the job ineligible until its retry time.',
        tone: 'retry',
      },
      {
        label: 'Ready again',
        detail: 'The delayed job is promoted back into scheduling order.',
        tone: 'default',
      },
      {
        label: 'Attempt 2',
        detail: 'A worker claims the next legal attempt.',
        tone: 'default',
      },
      {
        label: 'Completed',
        detail: 'The successful ACK stores the result exactly once.',
        tone: 'success',
      },
    ],
  },
  dlq: {
    label: 'Exhausts every attempt',
    steps: [
      {
        label: 'Producer',
        detail: 'queue.add() persists the job with a finite attempt budget.',
        tone: 'default',
      },
      {
        label: 'Ready queue',
        detail: 'The job becomes eligible for its first attempt.',
        tone: 'default',
      },
      {
        label: 'Attempt 1',
        detail: 'Processing fails and consumes one attempt.',
        tone: 'failure',
      },
      {
        label: 'Retry delay',
        detail: 'Backoff prevents an immediate hot retry.',
        tone: 'retry',
      },
      {
        label: 'Final attempt',
        detail: 'The worker fails after the remaining attempt is claimed.',
        tone: 'failure',
      },
      {
        label: 'Dead letter queue',
        detail: 'The terminal failure stays available for inspection or replay.',
        tone: 'failure',
      },
    ],
  },
} as const satisfies Record<string, JobJourney>;

export function clampJourneyStep(index: number, totalSteps: number): number {
  if (!Number.isFinite(index) || totalSteps <= 0) return 0;
  return Math.max(0, Math.min(Math.trunc(index), totalSteps - 1));
}

export type TopologyId = 'embedded' | 'single-broker' | 'multi-broker';

export interface Topology {
  bestFor: string;
  durability: string;
  id: TopologyId;
  label: string;
  layers: { label: string; nodes: string[] }[];
  links: string[];
  summary: string;
}

export const TOPOLOGIES: Topology[] = [
  {
    id: 'embedded',
    label: 'Embedded',
    summary: 'The producer, queue runtime, and worker share one Bun process.',
    bestFor: 'The smallest deployment, local services, and edge processes.',
    durability: 'Memory or one local SQLite file.',
    layers: [
      { label: 'Bun application', nodes: ['Producer', 'Queue runtime', 'Worker'] },
      { label: 'Optional persistence', nodes: ['SQLite'] },
    ],
    links: ['direct calls'],
  },
  {
    id: 'single-broker',
    label: 'TCP broker',
    summary: 'Independent producers and workers share one bunqueue broker over TCP.',
    bestFor: 'Several processes, several languages, or one central queue service.',
    durability: 'The broker owns memory or one SQLite file.',
    layers: [
      { label: 'Client processes', nodes: ['Producer', 'Worker A', 'Worker B'] },
      { label: 'Queue service', nodes: ['bunqueue broker'] },
      { label: 'Persistence', nodes: ['SQLite'] },
    ],
    links: ['TCP', 'local storage'],
  },
  {
    id: 'multi-broker',
    label: 'PostgreSQL multi-broker',
    summary: 'Clients can use any active broker while PostgreSQL coordinates shared queue state.',
    bestFor: 'Horizontal broker scale, failover, and shared limits across hosts.',
    durability: 'PostgreSQL is authoritative for every broker.',
    layers: [
      { label: 'Client processes', nodes: ['Producers', 'Workers', 'QueueEvents'] },
      { label: 'N active brokers', nodes: ['Broker A', 'Broker B', 'Broker C'] },
      { label: 'Shared persistence', nodes: ['PostgreSQL'] },
    ],
    links: ['TCP through a load balancer', 'transactional coordination'],
  },
];

export function resolveTopology(value: string | undefined): Topology {
  return TOPOLOGIES.find(({ id }) => id === value) ?? TOPOLOGIES[0];
}
