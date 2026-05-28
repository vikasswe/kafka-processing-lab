/**
 * ===============================
 * CONSUMER
 * ===============================
 */

import { kafka } from "./config/kafka";

/**
 * Kafka Consumer
 *
 * Consumer reads messages from Kafka topic.
 *
 * Important concepts here:
 * - offsets
 * - heartbeats
 * - rebalancing
 * - manual commits
 */
const consumer = kafka.consumer({

    /**
     * Consumer group name.
     *
     * Kafka tracks offsets per group.
     *
     * Multiple consumers with same groupId
     * share partitions automatically.
     */
    groupId: "order-service",

    /**
     * Maximum time Kafka waits before
     * considering consumer DEAD.
     *
     * If no heartbeat within 15 sec:
     * Kafka removes consumer from group.
     */
    sessionTimeout: 60000, // 🔥 increased for safe processing

    /**
     * Consumer sends heartbeat every 5 sec.
     *
     * Heartbeat tells Kafka:
     * "I am alive and processing."
     */
    heartbeatInterval: 20000, // 🔥 safer for long API calls

    /**
     * Time allowed for partition rebalance.
     *
     * Rebalance happens when:
     * - consumer joins
     * - consumer leaves
     * - crash occurs
     */
    rebalanceTimeout: 60000,
});

async function runConsumer() {

    /**
     * Connect consumer to Kafka broker.
     */
    await consumer.connect();

    /**
     * Subscribe to Kafka topic.
     *
     * fromBeginning=false means:
     *
     * If offset not found,
     * start from latest message.
     *
     * (✔ correct for your requirement: only new data)
     */
    await consumer.subscribe({
        topic: "stress-topic",
        fromBeginning: false,
    });

    console.log("Consumer started");

    await consumer.run({

        /**
         * Disable automatic offset commit.
         *
         * WHY?
         *
         * Auto commit can commit offset
         * BEFORE message processing finishes.
         *
         * If app crashes:
         * message may be LOST forever.
         *
         * Manual commit is safer.
         */
        autoCommit: false,

        /**
         * Disable automatic batch resolution.
         *
         * We manually resolve offsets
         * after successful processing.
         */
        eachBatchAutoResolve: false,

        /**
         * Batch processing mode.
         *
         * Better than eachMessage because:
         *
         * - better performance
         * - heartbeat control
         * - offset control
         * - lower rebalance risk
         */
        eachBatch: async ({
            batch,
            resolveOffset,
            heartbeat,
            commitOffsetsIfNecessary,
            isRunning,
            isStale,
        }) => {

            /**
             * Iterate through messages in batch.
             */
            for (const message of batch.messages) {

                /**
                 * Stop processing if:
                 *
                 * - consumer shutting down
                 * - batch invalid after rebalance
                 */
                if (!isRunning() || isStale()) return;

                try {

                    console.log(
                        `CONSUMED offset=${message.offset} value=${message.value?.toString()}`
                    );

                    /**
                     * VERY IMPORTANT:
                     * Heartbeat BEFORE processing
                     * to ensure consumer is not considered dead
                     */
                    await heartbeat();

                    /**
                     * Simulate long business processing.
                     * (DB / API / payment etc.)
                     */
                    await sleep(5000);

                    /**
                     * Heartbeat AFTER processing too
                     */
                    await heartbeat();

                    /**
                     * Mark offset as processed.
                     */
                    resolveOffset(message.offset);

                } catch (error) {

                    /**
                     * Processing failed.
                     *
                     * Offset NOT committed → Kafka will retry later.
                     */
                    console.error("PROCESS ERROR:", error);
                }
            }

            try {

                /**
                 * Commit processed offsets to Kafka.
                 */
                await commitOffsetsIfNecessary();

                console.log("OFFSET COMMITTED");

            } catch (err) {
                console.error("COMMIT FAILED:", err);
            }
        },
    });
}

/**
 * Utility delay function.
 */
function sleep(ms: number) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Start consumer process.
 */
runConsumer().catch(console.error);