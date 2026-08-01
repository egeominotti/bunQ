import { QueueCompatibility } from './compatibility';

/** Queue batching shutdown and transport cleanup. */
export class QueueConnection<T> extends QueueCompatibility<T> {
  async disconnect(): Promise<void> {
    if (this.addBatcher) {
      await this.addBatcher.flush();
      await this.addBatcher.waitForInFlight();
      this.addBatcher.stop();
    }
    this.close();
  }

  close(): void {
    this.addBatcher?.stop();
    this.releaseConnection();
  }
}
