/** Stable background-task import surface. */
export { startBackgroundTasks, stopBackgroundTasks } from './background/lifecycle';
export { recover } from './background/recovery';
export { checkJobTimeouts } from './background/timeouts';
export { processPendingDependencies } from './dependencyProcessor';
export { getTaskErrorStats } from './taskErrorTracking';
export type { BackgroundTaskHandles } from './types/background';
