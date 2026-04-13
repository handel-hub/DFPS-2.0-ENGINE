import SlotManager from "./slotManager.mjs"; // adjust path if needed

// Simulate worker IDs
let workerCounter = 0;
const createWorker = () => `worker-${workerCounter++}`;

// Initialize slot manager
const slotManager = new SlotManager({
    workerSlot: 4,   // small number to force contention
    warmSlot: 2
});

// Helper: print stats
function printStats(label) {
    console.log(`\n=== ${label} ===`);
    console.log(slotManager.slotStats());
}

// STEP 1: Fill worker slots
printStats("Initial State");

const activeWorkers = [];

for (let i = 0; i < 6; i++) {
    const workerId = createWorker();
    const success = slotManager.add(workerId, false);

    console.log(`Adding ${workerId} → ${success ? "SUCCESS" : "FAILED"}`);

    if (success) activeWorkers.push(workerId);
}

printStats("After Filling Worker Slots");

// STEP 2: Fill warm slots
const warmWorkers = [];

for (let i = 0; i < 4; i++) {
    const workerId = createWorker();
    const success = slotManager.add(workerId, true);

    console.log(`Adding WARM ${workerId} → ${success ? "SUCCESS" : "FAILED"}`);

    if (success) warmWorkers.push(workerId);
}

printStats("After Filling Warm Slots");

// STEP 3: Free some workers
console.log("\n--- Freeing 2 Worker Slots ---");

slotManager.freeSlots(activeWorkers[0]);
slotManager.freeSlots(activeWorkers[1]);

printStats("After Freeing Some Workers");

// STEP 4: Promote warm → worker manually (simulate scheduler)
console.log("\n--- Promoting Warm Workers ---");

for (let i = 0; i < warmWorkers.length; i++) {
    const workerId = warmWorkers[i];

    const success = slotManager.add(workerId, false);

    console.log(`Promoting ${workerId} → ${success ? "SUCCESS" : "FAILED"}`);
}

printStats("After Promotion Attempt");

// STEP 5: Stress test (random operations)
console.log("\n--- Stress Test Start ---");

for (let i = 0; i < 20; i++) {
    const workerId = createWorker();

    const useWarm = Math.random() > 0.5;

    const added = slotManager.add(workerId, useWarm);

    if (!added) {
        // randomly free something
        const allWorkers = [...activeWorkers, ...warmWorkers];
        const randomWorker = allWorkers[Math.floor(Math.random() * allWorkers.length)];

        if (randomWorker) {
            slotManager.freeSlots(randomWorker);
            console.log(`Freed ${randomWorker}`);
        }
    }
}

printStats("After Stress Test");

// FINAL CHECK
console.log("\n--- FINAL STATE ---");
console.log(slotManager.slotStats());