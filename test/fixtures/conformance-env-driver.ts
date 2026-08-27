import { createInterface } from 'node:readline';

const lines = createInterface({ input: process.stdin });

lines.on('line', (line) => {
  const request = JSON.parse(line) as { id: number; names?: string[] };
  const brokerOnlyEnvironment = Object.fromEntries(
    (request.names ?? []).flatMap((name) => {
      const value = process.env[name];
      return value === undefined ? [] : [[name, value]];
    })
  );
  console.log(
    JSON.stringify({
      id: request.id,
      ok: true,
      brokerOnlyEnvironment,
    })
  );
});
