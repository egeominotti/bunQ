import { httpUrl, invariant, type BrokerName } from './shared';

const brokers: BrokerName[] = ['a', 'b', 'c'];
export type HttpRequester = (url: string, init?: RequestInit) => Promise<Response>;

export async function runTopologyExample(
  request: HttpRequester = fetch,
  httpTimeoutMs = 5_000
): Promise<void> {
  for (const broker of brokers) {
    const base = httpUrl(broker);
    const get = (path: string, init: RequestInit = {}) =>
      request(`${base}${path}`, { ...init, signal: AbortSignal.timeout(httpTimeoutMs) });
    const [live, ready, deniedMetrics, metrics] = await Promise.all([
      get('/healthz'),
      get('/ready'),
      get('/prometheus'),
      get('/prometheus', {
        headers: { authorization: `Bearer ${Bun.env.BUNQUEUE_TOKEN ?? 'demo-token'}` },
      }),
    ]);

    const [liveBody, , , metricsBody] = await Promise.all([
      live.text(),
      ready.text(),
      deniedMetrics.text(),
      metrics.text(),
    ]);
    invariant(live.status === 200 && liveBody === 'OK', `${broker} is not live`);
    invariant(ready.status === 200, `${broker} is not ready`);
    invariant(deniedMetrics.status === 401, `${broker} exposed metrics without authentication`);
    invariant(metrics.status === 200, `${broker} rejected authenticated metrics`);
    invariant(metricsBody.includes('bunqueue_'), `${broker} returned no bunqueue metrics`);
  }
}
