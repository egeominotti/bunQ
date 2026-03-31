#!/usr/bin/env bun
/**
 * Flow Children Values Tests (TCP Mode)
 *
 * Verifies that getChildrenValues correctly unwraps the TCP response
 * envelope and returns child job results to the parent processor.
 */

import { Queue, Worker, FlowProducer } from "../../src/client";

const TCP_PORT = parseInt(process.env.TCP_PORT ?? "16789");
const connOpts = { port: TCP_PORT };

let passed = 0;
let failed = 0;

function ok(msg: string) {
	console.log(`   ✅ ${msg}`);
	passed++;
}

function fail(msg: string) {
	console.log(`   ❌ ${msg}`);
	failed++;
}

async function main() {
	console.log("=== Flow Children Values Tests (TCP) ===\n");

	const flow = new FlowProducer({ connection: connOpts, useLocks: false });

	// ─────────────────────────────────────────────────
	// Test 1: Parent reads child results via getChildrenValues
	// ─────────────────────────────────────────────────
	console.log("1. Testing PARENT READS CHILD RESULTS...");
	{
		const queueName = "tcp-flow-children-vals";
		const q = new Queue(queueName, { connection: connOpts });
		q.obliterate();
		await Bun.sleep(100);

		let parentValues: Record<string, unknown> | null = null;

		const worker = new Worker(
			queueName,
			async (job) => {
				const data = job.data as { role: string; value: number };

				if (data.role === "child") {
					return { computed: data.value * 10 };
				}

				if (data.role === "parent") {
					const children = await job.getChildrenValues();
					parentValues = children;
					return { aggregated: true };
				}

				return {};
			},
			{
				concurrency: 5,
				connection: connOpts,
				useLocks: false,
			},
		);

		await flow.add({
			name: "parent",
			queueName,
			data: { role: "parent" },
			children: [
				{ name: "child-a", queueName, data: { role: "child", value: 1 } },
				{ name: "child-b", queueName, data: { role: "child", value: 2 } },
			],
		});

		await Bun.sleep(5000);

		if (parentValues === null) {
			fail("Parent never received children values");
		} else {
			const values = Object.values(parentValues) as Array<{ computed?: number }>;
			const computedResults = values
				.map((v) => v?.computed)
				.filter((v) => v !== undefined)
				.sort();

			if (computedResults.length === 2 && computedResults[0] === 10 && computedResults[1] === 20) {
				ok(`Parent got child results: ${JSON.stringify(computedResults)}`);
			} else {
				fail(`Expected [10, 20], got ${JSON.stringify(parentValues)}`);
			}
		}

		await worker.close();
		q.obliterate();
		q.close();
	}

	// ─────────────────────────────────────────────────
	// Test 2: Queue.getChildrenValues query operation
	// ─────────────────────────────────────────────────
	console.log("\n2. Testing QUEUE getChildrenValues QUERY...");
	{
		const queueName = "tcp-flow-children-query";
		const q = new Queue(queueName, { connection: connOpts });
		q.obliterate();
		await Bun.sleep(100);

		const worker = new Worker(
			queueName,
			async (job) => {
				const data = job.data as { role: string; value: number };
				if (data.role === "child") {
					return { result: data.value * 5 };
				}
				return { done: true };
			},
			{
				concurrency: 5,
				connection: connOpts,
				useLocks: false,
			},
		);

		const tree = await flow.add({
			name: "root",
			queueName,
			data: { role: "parent" },
			children: [
				{ name: "c1", queueName, data: { role: "child", value: 3 } },
				{ name: "c2", queueName, data: { role: "child", value: 7 } },
			],
		});

		await Bun.sleep(5000);

		const childValues = await q.getChildrenValues(tree.job.id);

		if (!childValues || Object.keys(childValues).length === 0) {
			fail(`getChildrenValues returned empty: ${JSON.stringify(childValues)}`);
		} else {
			const results = Object.values(childValues) as Array<{ result?: number }>;
			const nums = results
				.map((v) => v?.result)
				.filter((v) => v !== undefined)
				.sort();

			if (nums.length === 2 && nums[0] === 15 && nums[1] === 35) {
				ok(`Queue.getChildrenValues returned: ${JSON.stringify(nums)}`);
			} else {
				fail(`Expected [15, 35], got ${JSON.stringify(childValues)}`);
			}
		}

		await worker.close();
		q.obliterate();
		q.close();
	}

	flow.close();

	console.log("\n=== Summary ===");
	console.log(`Passed: ${passed}`);
	console.log(`Failed: ${failed}`);

	process.exit(failed > 0 ? 1 : 0);
}

main().catch(console.error);
