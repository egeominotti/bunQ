import { FlowProducer } from '../../../client/flow';
import { getSharedManager } from '../../../client/manager';

export class EmbeddedBackendBase {
  private flowProducer: FlowProducer | null = null;

  protected get manager() {
    return getSharedManager();
  }

  protected getFlowProducer(): FlowProducer {
    this.flowProducer ??= new FlowProducer({ embedded: true });
    return this.flowProducer;
  }

  protected closeFlowProducer(): void {
    this.flowProducer?.close();
  }
}
