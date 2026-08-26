import type { CloudCommandHandler } from '../types/command';

export const INTEGRATION_COMMANDS: Partial<Record<string, CloudCommandHandler>> = {
  'cron:upsert': async (adapter, command) => {
    await adapter.removeCron(command.name ?? '');
    const cron = await adapter.addCron({
      name: command.name ?? '',
      jobName: 'default',
      queue: command.queue ?? '',
      data: command.data ?? {},
      schedule: command.schedule,
    });
    return { name: cron.name, nextRun: cron.nextRun };
  },
  'cron:delete': async (adapter, command) => ({
    deleted: await adapter.removeCron(command.name ?? ''),
  }),
  'webhook:add': (adapter, command) => {
    const webhook = adapter.manager.webhookManager.add(
      command.url ?? '',
      command.events ?? [],
      command.queue ?? undefined,
      command.secret ?? undefined
    );
    return { id: webhook.id, url: webhook.url, events: webhook.events };
  },
  'webhook:remove': (adapter, command) => ({
    removed: adapter.manager.webhookManager.remove(command.webhookId ?? ''),
  }),
  'webhook:set-enabled': (adapter, command) => ({
    updated: adapter.manager.webhookManager.setEnabled(
      command.webhookId ?? '',
      command.enabled ?? true
    ),
  }),
  's3:backup': async (_queueManager, _command, context) => {
    if (!context?.triggerBackup) throw new Error('S3 backup not configured');
    return await context.triggerBackup();
  },
};
