import type { FlowJobInput, FlowNodeResult, FlowStepInput, McpBackend } from '../../types/adapter';
import { serializeFlowNode } from '../serializers';
import { TcpServiceBackend } from './services';

export class TcpBackend extends TcpServiceBackend implements McpBackend {
  async addFlow(flow: FlowJobInput): Promise<FlowNodeResult> {
    return serializeFlowNode(await this.getFlowProducer().add(flow));
  }

  async addFlowChain(steps: FlowStepInput[]): Promise<{ jobIds: string[] }> {
    return this.getFlowProducer().addChain(steps);
  }

  async addFlowBulkThen(
    parallel: FlowStepInput[],
    final: FlowStepInput
  ): Promise<{ parallelIds: string[]; finalId: string }> {
    return this.getFlowProducer().addBulkThen(parallel, final);
  }

  async getFlow(
    id: string,
    queueName: string,
    depth?: number,
    maxChildren?: number
  ): Promise<FlowNodeResult | null> {
    const node = await this.getFlowProducer().getFlow({ id, queueName, depth, maxChildren });
    return node ? serializeFlowNode(node) : null;
  }

  shutdown() {
    this.closeBackend();
  }
}
