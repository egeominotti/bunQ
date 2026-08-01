import type { ServerWebSocket } from 'bun';
import { errorResponse } from '../protocol';
import type { WsData } from '../types/ws';
import { VALID_WS_PATTERNS } from './constants';

export function handleWsSubscribe(
  websocket: ServerWebSocket<WsData>,
  command: { events?: string[]; reqId?: string }
): void {
  const events = command.events;
  if (!Array.isArray(events) || events.length === 0) {
    websocket.send(errorResponse('events must be a non-empty array', command.reqId));
    return;
  }
  const invalid = events.filter((event) => !VALID_WS_PATTERNS.has(event));
  if (invalid.length > 0) {
    websocket.send(errorResponse(`Invalid: ${invalid.join(', ')}`, command.reqId));
    return;
  }
  websocket.data.subscriptions ??= new Set();
  for (const event of events) websocket.data.subscriptions.add(event);
  websocket.send(
    JSON.stringify({
      ok: true,
      subscribed: [...websocket.data.subscriptions],
      reqId: command.reqId,
    })
  );
}

export function handleWsUnsubscribe(
  websocket: ServerWebSocket<WsData>,
  command: { events?: string[]; reqId?: string }
): void {
  if (!websocket.data.subscriptions) {
    websocket.send(JSON.stringify({ ok: true, subscribed: [], reqId: command.reqId }));
    return;
  }
  const events = command.events;
  if (!Array.isArray(events) || events.length === 0) websocket.data.subscriptions.clear();
  else for (const event of events) websocket.data.subscriptions.delete(event);
  websocket.send(
    JSON.stringify({
      ok: true,
      subscribed: [...websocket.data.subscriptions],
      reqId: command.reqId,
    })
  );
}
