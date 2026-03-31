#!/usr/bin/env bun
/**
 * Test Webhooks (TCP Mode): Registration, Listing, and Removal
 * Note: We can't test actual webhook delivery without a real HTTP server,
 * so we only test registration/listing/removal via TCP commands.
 */

import { TcpConnectionPool } from '../../src/client/tcpPool';

const TCP_PORT = parseInt(process.env.TCP_PORT ?? '16789');

interface WebhookResponse {
  webhookId: string;
  url: string;
  events: string[];
  queue: string | null;
  createdAt: number;
}

interface ListWebhooksResponse {
  webhooks: Array<{
    id: string;
    url: string;
    events: string[];
    queue: string | null;
    createdAt: number;
    lastTriggered: number | null;
    successCount: number;
    failureCount: number;
    enabled: boolean;
  }>;
  stats: { total: number; enabled: number };
}

async function main() {
  console.log('=== Test Webhooks (TCP) ===\n');

  const tcp = new TcpConnectionPool({ port: TCP_PORT });
  await tcp.connect();

  let passed = 0;
  let failed = 0;

  // Clean up any existing webhooks
  const initialList = await tcp.send({ cmd: 'ListWebhooks' }) as { data: ListWebhooksResponse };
  for (const webhook of initialList.data?.webhooks ?? []) {
    await tcp.send({ cmd: 'RemoveWebhook', webhookId: webhook.id });
  }

  // Test 1: Add webhook with URL and events
  console.log('1. Testing ADD WEBHOOK...');
  try {
    const result = await tcp.send({
      cmd: 'AddWebhook',
      url: 'https://example.com/webhook',
      events: ['job.completed', 'job.failed']
    }) as { data: WebhookResponse };

    if (result.data?.webhookId && result.data?.url === 'https://example.com/webhook' &&
        result.data?.events?.includes('job.completed') && result.data?.events?.includes('job.failed')) {
      console.log(`   PASS Webhook added: ${result.data.webhookId}`);
      passed++;
    } else {
      console.log('   FAIL Webhook not created correctly');
      console.log('   Response:', JSON.stringify(result, null, 2));
      failed++;
    }
  } catch (e) {
    console.log(`   FAIL Add webhook failed: ${e}`);
    failed++;
  }

  // Test 2: List webhooks
  console.log('\n2. Testing LIST WEBHOOKS...');
  try {
    const result = await tcp.send({ cmd: 'ListWebhooks' }) as { data: ListWebhooksResponse };
    const webhooks = result.data?.webhooks ?? [];

    if (webhooks.length >= 1 && webhooks[0].url === 'https://example.com/webhook') {
      console.log(`   PASS Listed ${webhooks.length} webhook(s)`);
      passed++;
    } else {
      console.log(`   FAIL Expected at least 1 webhook, got ${webhooks.length}`);
      failed++;
    }
  } catch (e) {
    console.log(`   FAIL List webhooks failed: ${e}`);
    failed++;
  }

  // Test 3: Remove webhook
  console.log('\n3. Testing REMOVE WEBHOOK...');
  try {
    const listResult = await tcp.send({ cmd: 'ListWebhooks' }) as { data: ListWebhooksResponse };
    const webhookId = listResult.data?.webhooks?.[0]?.id;

    if (!webhookId) {
      console.log('   FAIL No webhook found to remove');
      failed++;
    } else {
      const removeResult = await tcp.send({ cmd: 'RemoveWebhook', webhookId }) as { data: { removed: boolean } };

      if (removeResult.data?.removed) {
        // Verify it's gone
        const verifyResult = await tcp.send({ cmd: 'ListWebhooks' }) as { data: ListWebhooksResponse };
        const stillExists = verifyResult.data?.webhooks?.some(w => w.id === webhookId);

        if (!stillExists) {
          console.log('   PASS Webhook removed successfully');
          passed++;
        } else {
          console.log('   FAIL Webhook still exists after removal');
          failed++;
        }
      } else {
        console.log('   FAIL Remove command did not return success');
        failed++;
      }
    }
  } catch (e) {
    console.log(`   FAIL Remove webhook failed: ${e}`);
    failed++;
  }

  // Test 4: Webhook with specific queue filter
  console.log('\n4. Testing WEBHOOK WITH QUEUE FILTER...');
  try {
    const result = await tcp.send({
      cmd: 'AddWebhook',
      url: 'https://example.com/emails-hook',
      events: ['job.completed'],
      queue: 'emails'
    }) as { data: WebhookResponse };

    if (result.data?.queue === 'emails') {
      console.log(`   PASS Webhook with queue filter: ${result.data.queue}`);
      passed++;
      // Cleanup
      await tcp.send({ cmd: 'RemoveWebhook', webhookId: result.data.webhookId });
    } else {
      console.log(`   FAIL Queue filter not set correctly: ${result.data?.queue}`);
      failed++;
    }
  } catch (e) {
    console.log(`   FAIL Queue filter webhook failed: ${e}`);
    failed++;
  }

  // Test 5: Webhook with secret for signing
  console.log('\n5. Testing WEBHOOK WITH SECRET...');
  try {
    const result = await tcp.send({
      cmd: 'AddWebhook',
      url: 'https://example.com/secure-hook',
      events: ['job.completed'],
      secret: 'my-secret-key-12345'
    }) as { data: WebhookResponse };

    // Note: The secret is not returned in the response for security
    // We just verify the webhook was created successfully
    if (result.data?.webhookId && result.data?.url === 'https://example.com/secure-hook') {
      console.log('   PASS Webhook with secret created');
      passed++;
      // Cleanup
      await tcp.send({ cmd: 'RemoveWebhook', webhookId: result.data.webhookId });
    } else {
      console.log('   FAIL Webhook with secret not created correctly');
      failed++;
    }
  } catch (e) {
    console.log(`   FAIL Secret webhook failed: ${e}`);
    failed++;
  }

  // Test 6: Multiple webhooks for same event
  console.log('\n6. Testing MULTIPLE WEBHOOKS FOR SAME EVENT...');
  try {
    const result1 = await tcp.send({
      cmd: 'AddWebhook',
      url: 'https://example.com/hook1',
      events: ['job.completed']
    }) as { data: WebhookResponse };

    const result2 = await tcp.send({
      cmd: 'AddWebhook',
      url: 'https://example.com/hook2',
      events: ['job.completed']
    }) as { data: WebhookResponse };

    const result3 = await tcp.send({
      cmd: 'AddWebhook',
      url: 'https://example.com/hook3',
      events: ['job.completed']
    }) as { data: WebhookResponse };

    const listResult = await tcp.send({ cmd: 'ListWebhooks' }) as { data: ListWebhooksResponse };
    const webhooks = listResult.data?.webhooks ?? [];
    const completedHooks = webhooks.filter(w => w.events.includes('job.completed'));

    if (completedHooks.length >= 3 &&
        result1.data?.webhookId !== result2.data?.webhookId &&
        result2.data?.webhookId !== result3.data?.webhookId) {
      console.log(`   PASS Multiple webhooks created: ${completedHooks.length} for job.completed`);
      passed++;
    } else {
      console.log(`   FAIL Expected 3+ webhooks for job.completed, got ${completedHooks.length}`);
      failed++;
    }

    // Cleanup
    await tcp.send({ cmd: 'RemoveWebhook', webhookId: result1.data?.webhookId });
    await tcp.send({ cmd: 'RemoveWebhook', webhookId: result2.data?.webhookId });
    await tcp.send({ cmd: 'RemoveWebhook', webhookId: result3.data?.webhookId });
  } catch (e) {
    console.log(`   FAIL Multiple webhooks test failed: ${e}`);
    failed++;
  }

  // Final cleanup
  const finalList = await tcp.send({ cmd: 'ListWebhooks' }) as { data: ListWebhooksResponse };
  for (const webhook of finalList.data?.webhooks ?? []) {
    await tcp.send({ cmd: 'RemoveWebhook', webhookId: webhook.id });
  }

  tcp.close();

  // Summary
  console.log('\n=== Summary ===');
  console.log(`Passed: ${passed}`);
  console.log(`Failed: ${failed}`);

  process.exit(failed > 0 ? 1 : 0);
}

main().catch(console.error);
