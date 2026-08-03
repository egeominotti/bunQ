/** Current TCP wire protocol revision advertised by the server and Bun client. */
export const PROTOCOL_VERSION = 3 as const;

/** Capabilities implemented by protocol revision 3. */
export const PROTOCOL_CAPABILITIES = ['pipelining', 'separate-job-name'] as const;

export type ProtocolCapability = (typeof PROTOCOL_CAPABILITIES)[number];
