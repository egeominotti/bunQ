#!/usr/bin/env bun
/**
 * Test Webhooks (Embedded Mode): Registration, Listing, and Removal
 * Note: We can't test actual webhook delivery without a real HTTP server,
 * so we only test registration/listing/removal via the WebhookManager.
 */

// Force embedded mode BEFORE imports
process.env.BUNQUEUE_EMBEDDED = '1';

import { getSharedManager } from '../../src/client/manager';

async function main() {
  console.log('=== Test Webhooks (Embedded) ===\n');

  const manager = getSharedManager();
  const webhookManager = manager.webhookManager;
  let passed = 0;
  let failed = 0;

  // Clean up any existing webhooks
  for (const webhook of webhookManager.list()) {
    webhookManager.remove(webhook.id);
  }

  // Test 1: Add webhook with URL and events
  console.log('1. Testing ADD WEBHOOK...');
  try {
    const webhook = webhookManager.add(
      'https://example.com/webhook',
      ['job.completed', 'job.failed']
    );

    if (webhook.id && webhook.url === 'https://example.com/webhook' &&
        webhook.events.includes('job.completed') && webhook.events.includes('job.failed') &&
        webhook.enabled === true) {
      console.log(`   PASS Webhook added: ${webhook.id}`);
      passed++;
    } else {
      console.log('   FAIL Webhook not created correctly');
      failed++;
    }
  } catch (e) {
    console.log(`   FAIL Add webhook failed: ${e}`);
    failed++;
  }

  // Test 2: List webhooks
  console.log('\n2. Testing LIST WEBHOOKS...');
  try {
    const webhooks = webhookManager.list();

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
    const webhooks = webhookManager.list();
    const webhookId = webhooks[0].id;
    const removed = webhookManager.remove(webhookId);

    if (removed && webhookManager.get(webhookId) === undefined) {
      console.log('   PASS Webhook removed successfully');
      passed++;
    } else {
      console.log('   FAIL Webhook not removed correctly');
      failed++;
    }
  } catch (e) {
    console.log(`   FAIL Remove webhook failed: ${e}`);
    failed++;
  }

  // Test 4: Webhook with specific queue filter
  console.log('\n4. Testing WEBHOOK WITH QUEUE FILTER...');
  try {
    const webhook = webhookManager.add(
      'https://example.com/emails-hook',
      ['job.completed'],
      'emails'  // Only for 'emails' queue
    );

    if (webhook.queue === 'emails') {
      console.log(`   PASS Webhook with queue filter: ${webhook.queue}`);
      passed++;
      // Cleanup
      webhookManager.remove(webhook.id);
    } else {
      console.log(`   FAIL Queue filter not set correctly: ${webhook.queue}`);
      failed++;
    }
  } catch (e) {
    console.log(`   FAIL Queue filter webhook failed: ${e}`);
    failed++;
  }

  // Test 5: Webhook with secret for signing
  console.log('\n5. Testing WEBHOOK WITH SECRET...');
  try {
    const webhook = webhookManager.add(
      'https://example.com/secure-hook',
      ['job.completed'],
      undefined,  // All queues
      'my-secret-key-12345'
    );

    if (webhook.secret === 'my-secret-key-12345') {
      console.log('   PASS Webhook with secret created');
      passed++;
      // Cleanup
      webhookManager.remove(webhook.id);
    } else {
      console.log(`   FAIL Secret not set correctly`);
      failed++;
    }
  } catch (e) {
    console.log(`   FAIL Secret webhook failed: ${e}`);
    failed++;
  }

  // Test 6: Multiple webhooks for same event
  console.log('\n6. Testing MULTIPLE WEBHOOKS FOR SAME EVENT...');
  try {
    const webhook1 = webhookManager.add(
      'https://example.com/hook1',
      ['job.completed']
    );
    const webhook2 = webhookManager.add(
      'https://example.com/hook2',
      ['job.completed']
    );
    const webhook3 = webhookManager.add(
      'https://example.com/hook3',
      ['job.completed']
    );

    const webhooks = webhookManager.list();
    const completedHooks = webhooks.filter(w => w.events.includes('job.completed'));

    if (completedHooks.length >= 3 &&
        webhook1.id !== webhook2.id && webhook2.id !== webhook3.id) {
      console.log(`   PASS Multiple webhooks created: ${completedHooks.length} for job.completed`);
      passed++;
    } else {
      console.log(`   FAIL Expected 3+ webhooks for job.completed, got ${completedHooks.length}`);
      failed++;
    }

    // Cleanup
    webhookManager.remove(webhook1.id);
    webhookManager.remove(webhook2.id);
    webhookManager.remove(webhook3.id);
  } catch (e) {
    console.log(`   FAIL Multiple webhooks test failed: ${e}`);
    failed++;
  }

  // Final cleanup
  for (const webhook of webhookManager.list()) {
    webhookManager.remove(webhook.id);
  }

  // Summary
  console.log('\n=== Summary ===');
  console.log(`Passed: ${passed}`);
  console.log(`Failed: ${failed}`);

  process.exit(failed > 0 ? 1 : 0);
}

main().catch(console.error);
