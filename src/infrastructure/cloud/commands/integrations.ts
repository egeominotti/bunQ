import type { CloudCommandHandler } from '../types/command';

export const INTEGRATION_COMMANDS: Partial<Record<string, CloudCommandHandler>> = {
  'cron:upsert': (queueManager, command) => {
    queueManager.removeCron(command.name ?? '');
    const cron = queueManager.addCron({
      name: command.name ?? '',
      queue: command.queue ?? '',
      data: command.data ?? {},
      schedule: command.schedule,
    });
    return { name: cron.name, nextRun: cron.nextRun };
  },
  'cron:delete': (queueManager, command) => ({
    deleted: queueManager.removeCron(command.name ?? ''),
  }),
  'webhook:add': (queueManager, command) => {
    const webhook = queueManager.webhookManager.add(
      command.url ?? '',
      command.events ?? [],
      command.queue ?? undefined,
      command.secret ?? undefined
    );
    return { id: webhook.id, url: webhook.url, events: webhook.events };
  },
  'webhook:remove': (queueManager, command) => ({
    removed: queueManager.webhookManager.remove(command.webhookId ?? ''),
  }),
  'webhook:set-enabled': (queueManager, command) => ({
    updated: queueManager.webhookManager.setEnabled(
      command.webhookId ?? '',
      command.enabled ?? true
    ),
  }),
  's3:backup': async (_queueManager, _command, context) => {
    if (!context?.triggerBackup) throw new Error('S3 backup not configured');
    return await context.triggerBackup();
  },
};
