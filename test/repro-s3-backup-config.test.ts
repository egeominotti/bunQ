import { afterEach, describe, expect, test } from 'bun:test';
import { resolveBackupConfig } from '../src/config/resolve';
import { backupStartupError } from '../src/infrastructure/server/bootstrap';

const originalSessionToken = Bun.env.S3_SESSION_TOKEN;
const originalVirtualHostedStyle = Bun.env.S3_VIRTUAL_HOSTED_STYLE;

afterEach(() => {
  if (originalSessionToken === undefined) delete Bun.env.S3_SESSION_TOKEN;
  else Bun.env.S3_SESSION_TOKEN = originalSessionToken;
  if (originalVirtualHostedStyle === undefined) delete Bun.env.S3_VIRTUAL_HOSTED_STYLE;
  else Bun.env.S3_VIRTUAL_HOSTED_STYLE = originalVirtualHostedStyle;
});

describe('S3 backup credential and addressing configuration', () => {
  test('server startup rejects enabled backup without persistent SQLite storage', () => {
    expect(
      backupStartupError({
        s3BackupEnabled: true,
        dataPath: undefined,
      })
    ).toContain('BUNQUEUE_DATA_PATH');
    expect(backupStartupError({ s3BackupEnabled: false, dataPath: undefined })).toBeNull();
    expect(backupStartupError({ s3BackupEnabled: true, dataPath: '/data/bunqueue.db' })).toBeNull();
  });

  test('configuration file propagates temporary credentials and virtual-host addressing', () => {
    const resolved = resolveBackupConfig(
      {
        backup: {
          enabled: true,
          sessionToken: 'file-session-token',
          virtualHostedStyle: true,
        },
      } as Parameters<typeof resolveBackupConfig>[0],
      '/tmp/bunqueue.db'
    );

    expect(resolved.sessionToken).toBe('file-session-token');
    expect(resolved.virtualHostedStyle).toBe(true);
  });

  test('environment variables are accepted when the configuration file omits them', () => {
    Bun.env.S3_SESSION_TOKEN = 'env-session-token';
    Bun.env.S3_VIRTUAL_HOSTED_STYLE = 'true';

    const resolved = resolveBackupConfig(null, '/tmp/bunqueue.db');
    expect(resolved.sessionToken).toBe('env-session-token');
    expect(resolved.virtualHostedStyle).toBe(true);
  });
});
