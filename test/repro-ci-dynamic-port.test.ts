import { describe, expect, test } from 'bun:test';

const tcpAuditSource = await Bun.file(`${import.meta.dir}/audit-tcp-protocol.test.ts`).text();

describe('CI TCP test port allocation', () => {
  test('the TCP protocol audit delegates port selection to the kernel', () => {
    expect(tcpAuditSource).not.toMatch(/20000\s*\+\s*Math\.floor\(Math\.random\(\)\s*\*\s*20000\)/);
    expect(tcpAuditSource).toContain('port: 0');
    expect(tcpAuditSource).toContain('server.server.port');
  });
});
