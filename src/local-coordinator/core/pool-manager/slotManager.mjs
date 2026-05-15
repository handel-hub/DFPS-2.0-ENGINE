import os from "node:os";

class SlotManager {

    // ===== SLOT STORAGE =====
    #workerSlots; // Map<slotId, { workerId, pluginId }>
    #warmSlots;   // Map<slotId, { workerId, pluginId }>

    // ===== FREE SLOT TRACKING =====
    #workerFreeSet; // Set of free worker slot IDs
    #warmFreeSet;   // Set of free warm slot IDs

    // ===== REVERSE INDEX (CRITICAL) =====
    #workerIndex; // Map<workerId, { type: "worker" | "warm", slotId }>

    // ===== WORKER METADATA =====
    #workers; // Map<workerId, { pluginId, state, startedAt, lastUsedAt }>

    // ===== CAPACITY =====
    #workerSlotCount;
    #warmSlotCount;

    constructor(config = {}) {

        // Determine worker slot count safely
        this.#workerSlotCount = config.workerSlot ?? Math.max(1, os.cpus().length - 1);

        // Determine warm slot count safely
        this.#warmSlotCount = config.warmSlot ?? Math.floor(this.#workerSlotCount / 2);

        // Initialize slot maps
        this.#workerSlots = new Map();
        this.#warmSlots = new Map();

        // Initialize free slot sets
        this.#workerFreeSet = new Set();
        this.#warmFreeSet = new Set();

        // Initialize reverse index
        this.#workerIndex = new Map();

        // Initialize worker metadata store
        this.#workers = new Map();

        // Public counters
        this.freeWorkerSlot = this.#workerSlotCount;
        this.usedWorkerSlot = 0;

        this.freeWarmSlot = this.#warmSlotCount;
        this.usedWarmSlot = 0;

        // Populate worker slots
        for (let i = 0; i < this.#workerSlotCount; i++) {
            this.#workerSlots.set(i, null);
            this.#workerFreeSet.add(i);
        }

        // Populate warm slots
        for (let i = 0; i < this.#warmSlotCount; i++) {
            this.#warmSlots.set(i, null);
            this.#warmFreeSet.add(i);
        }
    }

    // ===== INTERNAL SAFETY CHECK =====
    #assertInvariants() {
        if (this.usedWorkerSlot > this.#workerSlotCount) {
            throw new Error("Worker slot overflow");
        }
        if (this.usedWarmSlot > this.#warmSlotCount) {
            throw new Error("Warm slot overflow");
        }
    }

    // ===== ADD WORKER =====
    add(workerId, pluginId, isWarm = false) {

        if (!workerId || !pluginId) return null;
        if (this.#workerIndex.has(workerId)) return null;

        const now = Date.now(); // Current timestamp

        // Register worker metadata
        this.#workers.set(workerId, {
            pluginId,
            state: isWarm ? "WARM" : "RUNNING",
            startedAt: now,
            lastUsedAt: now
        });

        // ===== WARM SLOT =====
        if (isWarm) {

            if (this.#warmFreeSet.size === 0) {
                this.#workers.delete(workerId);
                return null;
            }

            const slotId = this.#warmFreeSet.values().next().value;

            this.#warmSlots.set(slotId, { workerId, pluginId });

            // Update tracking
            this.#warmFreeSet.delete(slotId);
            this.#workerIndex.set(workerId, { type: "warm", slotId });

            this.freeWarmSlot--;
            this.usedWarmSlot++;

            this.#assertInvariants();

            return slotId;
        }

        // ===== WORKER SLOT =====
        if (this.#workerFreeSet.size === 0) {
            this.#workers.delete(workerId);
            return null;
        }

        const slotId = this.#workerFreeSet.values().next().value;
        this.#workerSlots.set(slotId, { workerId, pluginId });
        this.#workerFreeSet.delete(slotId);
        this.#workerIndex.set(workerId, { type: "worker", slotId });

        this.freeWorkerSlot--;
        this.usedWorkerSlot++;

        this.#assertInvariants();

        return slotId;
    }

    // ===== FREE WORKER =====
    freeSlots(workerId) {

        const record = this.#workerIndex.get(workerId);
        if (!record) return false;

        const { type, slotId } = record;

        // Remove from slot
        if (type === "worker") {
            this.#workerSlots.set(slotId, null);
            this.#workerFreeSet.add(slotId);

            this.freeWorkerSlot++;
            this.usedWorkerSlot--;
        } else {
            this.#warmSlots.set(slotId, null);
            this.#warmFreeSet.add(slotId);

            this.freeWarmSlot++;
            this.usedWarmSlot--;
        }

        // Remove from index + metadata
        this.#workerIndex.delete(workerId);
        this.#workers.delete(workerId);

        this.#assertInvariants();

        return true;
    }

    /**
     * Free a slot by numeric slotId. Convenience wrapper.
     */
    freeSlotById(slotId) {
        if (this.#workerSlots.has(slotId)) {
            const slot = this.#workerSlots.get(slotId);
            if (!slot) return false;
            return this.freeSlots(slot.workerId);
        }
        if (this.#warmSlots.has(slotId)) {
            const slot = this.#warmSlots.get(slotId);
            if (!slot) return false;
            return this.freeSlots(slot.workerId);
        }
        return false;
    }
    // ===== PROMOTE WARM → WORKER =====
    promote(workerId) {

        const record = this.#workerIndex.get(workerId);
        if (!record || record.type !== "warm") return false;

        // Check if worker slot available
        if (this.#workerFreeSet.size === 0) return false;

        const warmSlotId = record.slotId;

        const slotData = this.#warmSlots.get(warmSlotId);
        if (!slotData) return false;
        const pluginId = slotData.pluginId;

        // Remove from warm slot
        this.#warmSlots.set(warmSlotId, null);
        this.#warmFreeSet.add(warmSlotId);

        this.freeWarmSlot++;
        this.usedWarmSlot--;

        // Assign to worker slot
        const workerSlotId = this.#workerFreeSet.values().next().value;

        this.#workerSlots.set(workerSlotId, { workerId, pluginId });
        this.#workerFreeSet.delete(workerSlotId);

        this.freeWorkerSlot--;
        this.usedWorkerSlot++;

        // Update index
        this.#workerIndex.set(workerId, {
            type: "worker",
            slotId: workerSlotId
        });

        // Update metadata
        const meta = this.#workers.get(workerId);
        meta.state = "RUNNING";
        meta.lastUsedAt = Date.now();

        this.#assertInvariants();

        return true;
    }

    // ===== GET WORKER INFO =====
    getWorker(workerId) {
        return this.#workers.get(workerId) || null;
    }

    /**
   * Return the numeric slotId for a given workerId (or tempKey).
   * If the workerId is not present in the index, returns null.
   */
    getSlotIdForWorker(workerId) {
        if (!workerId) return null;
        const rec = this.#workerIndex.get(workerId);
        return rec ? rec.slotId : null;
    }


    // ===== STATS =====
    slotStats() {
        return {
            worker: {
                total: this.#workerSlotCount,
                free: this.freeWorkerSlot,
                used: this.usedWorkerSlot
            },
            warm: {
                total: this.#warmSlotCount,
                free: this.freeWarmSlot,
                used: this.usedWarmSlot
            }
        };
    }

    // ===== DEBUG =====
    debug() {
        return {
            workerSlots: [...this.#workerSlots.entries()],
            warmSlots: [...this.#warmSlots.entries()],
            workers: [...this.#workers.entries()],
            index: [...this.#workerIndex.entries()]
        };
    }
}

export default SlotManager;