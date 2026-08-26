const DEFAULT_POSTGRES_VERSION_PREFIX = '18.6';

const configuredPrefix =
  Bun.env.BUNQUEUE_TEST_POSTGRES_VERSION_PREFIX ?? DEFAULT_POSTGRES_VERSION_PREFIX;

if (!/^\d+(?:\.\d+)?$/.test(configuredPrefix)) {
  throw new Error(
    `BUNQUEUE_TEST_POSTGRES_VERSION_PREFIX must be a major or major.minor version, received ${configuredPrefix}`
  );
}

/** Version contract for the PostgreSQL instance selected by the test environment. */
export const expectedPostgresVersionPrefix = configuredPrefix;
