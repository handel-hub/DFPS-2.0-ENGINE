// slotManager.mjs
import os from "node:os";

class SlotManager {
    // ===== SLOT STORAGE =====
    #workerSlots; // Map<slotId, { workerId, pluginId, meta }>
    #warmSlots;   // Map<slotId, { workerId, pluginId, meta }>

    // ===== FREE SLOT TRACKING =====
    #workerFreeSet; // Set of free worker slot IDs
    #warmFreeSet;   // Set of free warm slot IDs

    // ===== REVERSE INDEX (CRITICAL) =====
    #workerIndex; // Map<workerIdOrTempKey, { type: "worker" | "warm", slotId }>

    // ===== WORKER METADATA =====
    #workers; // Map<workerId, { pluginId, state, startedAt, lastUsedAt }>

    // ===== CAPACITY =====
    #workerSlotCount;
    #warmSlotCount;

    constructor(config = {}) {
        this.#workerSlotCount = Number(config.workerSlot ?? Math.max(1, os.cpus().length - 1));
        this.#warmSlotCount = Number(config.warmSlot ?? Math.floor(this.#workerSlotCount / 2));

        this.#workerSlots = new Map();
        this.#warmSlots = new Map();

        this.#workerFreeSet = new Set();
        this.#warmFreeSet = new Set();

        this.#workerIndex = new Map();
        this.#workers = new Map();

        this.freeWorkerSlot = this.#workerSlotCount;
        this.usedWorkerSlot = 0;

        this.freeWarmSlot = this.#warmSlotCount;
        this.usedWarmSlot = 0;

        for (let i = 0; i < this.#workerSlotCount; i++) {
            this.#workerSlots.set(i, null);
            this.#workerFreeSet.add(i);
        }

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

    /**
     * add(workerId, pluginId, isWarm = false)
     * - occupant key may be a tempKey or a real workerId
     * - returns numeric slotId on success or null on failure
     */
    add(workerId, pluginId, isWarm = false) {
        if (!workerId || !pluginId) return null;
        if (this.#workerIndex.has(workerId)) return null;

        const now = Date.now();
        this.#workers.set(workerId, {
            pluginId,
            state: isWarm ? "WARM" : "RUNNING",
            startedAt: now,
            lastUsedAt: now
        });

        if (isWarm) {
            if (this.#warmFreeSet.size === 0) {
                this.#workers.delete(workerId);
                return null;
            }
            const slotId = this.#warmFreeSet.values().next().value;
            this.#warmSlots.set(slotId, { workerId, pluginId, meta: { state: "WARM" } });
            this.#warmFreeSet.delete(slotId);
            this.#workerIndex.set(workerId, { type: "warm", slotId });
            this.freeWarmSlot--;
            this.usedWarmSlot++;
            this.#assertInvariants();
            return slotId;
        }

        if (this.#workerFreeSet.size === 0) {
            this.#workers.delete(workerId);
            return null;
        }
        const slotId = this.#workerFreeSet.values().next().value;
        this.#workerSlots.set(slotId, { workerId, pluginId, meta: { state: "RUNNING" } });
        this.#workerFreeSet.delete(slotId);
        this.#workerIndex.set(workerId, { type: "worker", slotId });
        this.freeWorkerSlot--;
        this.usedWorkerSlot++;
        this.#assertInvariants();
        return slotId;
    }

    /**
     * freeSlots(occupantKey)
     * - frees slot by occupant key (workerId or tempKey)
     */
    freeSlots(occupantKey) {
        const record = this.#workerIndex.get(occupantKey);
        if (!record) return false;
        const { type, slotId } = record;

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

        this.#workerIndex.delete(occupantKey);
        this.#workers.delete(occupantKey);
        this.#assertInvariants();
        return true;
    }

    /**
     * freeSlotById(slotId)
     * - convenience wrapper to free by numeric id
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

    /**
     * promote(workerId)
     * - move a warm occupant into a worker slot (if available)
     */
    promote(workerId) {
        const record = this.#workerIndex.get(workerId);
        if (!record || record.type !== "warm") return false;
        if (this.#workerFreeSet.size === 0) return false;

        const warmSlotId = record.slotId;
        const slotData = this.#warmSlots.get(warmSlotId);
        if (!slotData) return false;
        const pluginId = slotData.pluginId;

        this.#warmSlots.set(warmSlotId, null);
        this.#warmFreeSet.add(warmSlotId);
        this.freeWarmSlot++;
        this.usedWarmSlot--;

        const workerSlotId = this.#workerFreeSet.values().next().value;
        this.#workerSlots.set(workerSlotId, { workerId, pluginId, meta: { state: "RUNNING" } });
        this.#workerFreeSet.delete(workerSlotId);
        this.freeWorkerSlot--;
        this.usedWorkerSlot++;

        this.#workerIndex.set(workerId, { type: "worker", slotId: workerSlotId });

        const meta = this.#workers.get(workerId);
        if (meta) {
            meta.state = "RUNNING";
            meta.lastUsedAt = Date.now();
        }

        this.#assertInvariants();
        return true;
    }

    /**
     * getWorker(workerId)
     * - returns worker metadata (pluginId, state, startedAt, lastUsedAt) or null
     */
    getWorker(workerId) {
        return this.#workers.get(workerId) || null;
    }

    /**
     * getSlotIdForWorker(workerId)
     * - returns numeric slotId or null
     */
    getSlotIdForWorker(workerId) {
        if (!workerId) return null;
        const rec = this.#workerIndex.get(workerId);
        return rec ? rec.slotId : null;
    }

    /**
     * replaceOccupant(slotId, expectedKey, newWorkerId)
     * - Atomically replace occupant if expectedKey matches current occupant.
     * - Returns true on success, false on mismatch/invalid slot.
     */
    replaceOccupant(slotId, expectedKey, newWorkerId) {
        if (slotId === null || slotId === undefined) return false;

        // --- WORKER SLOT BRANCH ---
        if (this.#workerSlots.has(slotId)) {
            const slot = this.#workerSlots.get(slotId);
            const current = slot?.workerId ?? null;
            if (current !== expectedKey) return false;

            // Clean up OLD key from ALL tracking maps
            if (this.#workerIndex.has(expectedKey)) this.#workerIndex.delete(expectedKey);
            if (this.#workers.has(expectedKey)) this.#workers.delete(expectedKey); // <-- THE FIX

            // Assign NEW key
            slot.workerId = newWorkerId;
            slot.meta = slot.meta ?? {};
            this.#workerIndex.set(newWorkerId, { type: "worker", slotId });

            const meta = this.#workers.get(newWorkerId) ?? { pluginId: slot.pluginId, state: "RUNNING", startedAt: Date.now(), lastUsedAt: Date.now() };
            meta.pluginId = slot.pluginId;
            meta.state = "RUNNING";
            this.#workers.set(newWorkerId, meta);
            return true;
        }

        // --- WARM SLOT BRANCH ---
        if (this.#warmSlots.has(slotId)) {
            const slot = this.#warmSlots.get(slotId);
            const current = slot?.workerId ?? null;
            if (current !== expectedKey) return false;

            // Clean up OLD key from ALL tracking maps
            if (this.#workerIndex.has(expectedKey)) this.#workerIndex.delete(expectedKey);
            if (this.#workers.has(expectedKey)) this.#workers.delete(expectedKey); // <-- THE FIX

            // Assign NEW key
            slot.workerId = newWorkerId;
            slot.meta = slot.meta ?? {};
            this.#workerIndex.set(newWorkerId, { type: "warm", slotId });
            
            const meta = this.#workers.get(newWorkerId) ?? { pluginId: slot.pluginId, state: "WARM", startedAt: Date.now(), lastUsedAt: Date.now() };
            meta.pluginId = slot.pluginId;
            meta.state = "WARM";
            this.#workers.set(newWorkerId, meta);
            return true;
        }

        return false;
    }

    /**
     * slotStats()
     * - returns summary of worker and warm slots
     */
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

    /**
     * debug()
     * - returns internal structures for debugging
     */
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
