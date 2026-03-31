import { describe, it, expect } from 'bun:test';
import { FlowProducer } from '../src/client';

describe('FlowProducer EventEmitter (BullMQ v5 compat)', () => {
  it('should support on/off/once/emit', () => {
    const flow = new FlowProducer({ embedded: true });

    // Should have EventEmitter methods
    expect(typeof flow.on).toBe('function');
    expect(typeof flow.off).toBe('function');
    expect(typeof flow.once).toBe('function');
    expect(typeof flow.emit).toBe('function');

    flow.close();
  });

  it('should emit error events', () => {
    const flow = new FlowProducer({ embedded: true });
    const errors: Error[] = [];

    flow.on('error', (err) => {
      errors.push(err);
    });

    flow.emit('error', new Error('test error'));
    expect(errors.length).toBe(1);
    expect(errors[0].message).toBe('test error');

    flow.close();
  });

  it('should support once', () => {
    const flow = new FlowProducer({ embedded: true });
    let count = 0;

    flow.once('test', () => { count++; });
    flow.emit('test', 'e1');
    flow.emit('test', 'e2');

    expect(count).toBe(1);
    flow.close();
  });

  it('should support off to remove listeners', () => {
    const flow = new FlowProducer({ embedded: true });
    let count = 0;
    const listener = () => { count++; };

    flow.on('test', listener);
    flow.emit('test', 'e1');
    flow.off('test', listener);
    flow.emit('test', 'e2');

    expect(count).toBe(1);
    flow.close();
  });

  it('close should return Promise<void>', async () => {
    const flow = new FlowProducer({ embedded: true });
    const result = flow.close();
    expect(result).toBeInstanceOf(Promise);
    await result;
  });

  it('should have closing property', () => {
    const flow = new FlowProducer({ embedded: true });
    expect(flow.closing).toBeDefined();
    flow.close();
  });
});
