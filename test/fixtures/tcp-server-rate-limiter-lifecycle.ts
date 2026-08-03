import { QueueManager } from '../../src/application/queueManager';
import { TcpClient } from '../../src/client/tcp';
import { createTcpServer } from '../../src/infrastructure/server/tcp';

const manager = new QueueManager();
const server = createTcpServer(manager, { hostname: '127.0.0.1', port: 0 });

try {
  const client = new TcpClient({
    host: '127.0.0.1',
    port: server.server.port,
    autoReconnect: false,
    pingInterval: 0,
  });
  await client.send({ cmd: 'Ping' });
  client.close();
} finally {
  server.stop();
  manager.shutdown();
}
