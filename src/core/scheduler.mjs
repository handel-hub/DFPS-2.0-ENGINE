/**
 * DFPS 2.0 Contextual Dispatcher
 * Implements Greedy Constrained Bipartite Assignment with Diminishing Marginal Utility
 */
class Dispatcher {
    #EPSILON_MAX = 0.05;
    #EPSILON_MIN = 0.005;
    #PRESSURE_K = 2.0;       // Tuning constant for pressure decay
    #SOFTMAX_TEMP = 0.1;     // Low temperature for sharp softmax exploitation

    constructor(config = {}) {
        if (config.epsilonMax) this.#EPSILON_MAX = config.epsilonMax;
        if (config.epsilonMin) this.#EPSILON_MIN = config.epsilonMin;
        if (config.pressureK) this.#PRESSURE_K = config.pressureK;
        if (config.softmaxTemp) this.#SOFTMAX_TEMP = config.softmaxTemp;
    }

    /**
     * Executes a single routing tick.
     * @param {Array} sortedJobs - Top-K jobs sorted by priority.
     * @param {Map} nodesMetrics - The visibleData map from AdaptiveMetricsEngine.
     * @param {Object} affinityEngine - Instance of the Affinity class.
     * @returns {Array} Array of assignment tuples: [jobId, nodeId]
     */
    runAssignmentTick(sortedJobs, nodesMetrics, affinityEngine) {
        const assignments = [];
        const numJobs = sortedJobs.length;
        const numNodes = nodesMetrics.size;

        if (numJobs === 0 || numNodes === 0) return assignments;

        // 1. Snapshot Simulated State (O(N))
        // We isolate state mutation to prevent cross-tick contamination
        const simulatedState = new Map();
        let totalLoad = 0;

        for (const [nodeId, data] of nodesMetrics.entries()) {
            const metrics = data.metrics;
            // Calculate pseudo-load based on current queue and execution speed
            const load = (metrics.queue_ema * (metrics.avg_job_time_sec || 1.0)) / 1800.0; // Assuming W_max = 1800s
            totalLoad += Math.min(1.0, load);

            simulatedState.set(nodeId, {
                mem_sim: metrics.available_memory || 0,
                q_sim: metrics.queue_ema || 0,
                baseScore: data.score // M_n from Metrics Engine
            });
        }

        // 2. Cluster Pressure Scalar (P)
        const clusterPressure = totalLoad / numNodes;
        const f_P = Math.exp(-this.#PRESSURE_K * clusterPressure);

        // 3. Assignment Loop (O(J x N))
        for (let j = 0; j < numJobs; j++) {
            const jobTuple = sortedJobs[j];
            const jobId = jobTuple[0];
            const job = jobTuple[1];
            const jobSize = job.size_bytes / (1024 * 1024); // Convert to MB

            // Step A: Capacity Filter
            const validNodes = [];
            let totalAvailableMem = 0;

            for (const [nodeId, state] of simulatedState.entries()) {
                if (state.mem_sim >= jobSize) {
                    validNodes.push(nodeId);
                }
                totalAvailableMem += state.mem_sim;
            }

            // Early Stop Condition: If the cluster is completely out of RAM for this job size
            if (validNodes.length === 0) {
                // If even the smallest job can't fit, we break the entire batch to save CPU.
                // Assuming sortedJobs might have smaller jobs later, we could just `continue`, 
                // but a hard stop protects V8 under extreme saturation.
                if (totalAvailableMem < jobSize) break; 
                continue; 
            }

            const numValidNodes = validNodes.length;

            // Step B: Contextual Bids
            // Fetch precise affinity scores for these specific valid nodes in O(V)
            const affinities = affinityEngine.getAffinity(job.type, jobSize, validNodes);
            const bids = new Array(numValidNodes);
            let maxBid = -1;
            let bestNodeId = null;

            for (let i = 0; i < numValidNodes; i++) {
                const nodeId = validNodes[i];
                const state = simulatedState.get(nodeId);
                const affinityScore = affinities[i].score;
                
                // M_n * A_n
                const bid = state.baseScore * affinityScore;
                bids[i] = { nodeId, bid };

                if (bid > maxBid) {
                    maxBid = bid;
                    bestNodeId = nodeId;
                }
            }

            // Step C: Dynamic Epsilon Decision
            const f_C = 1.0 / (1.0 + (job.priority || 1));
            const f_S = numValidNodes / numNodes;
            const epsilon_j = Math.max(
                this.#EPSILON_MIN,
                this.#EPSILON_MAX * f_P * f_C * f_S
            );

            let selectedNodeId = bestNodeId;

            if (Math.random() < epsilon_j) {
                // Exploration Mode: Low-Temperature Softmax
                selectedNodeId = this.#selectSoftmax(bids);
            }

            // Step D: Commit Assignment & Optimistic Mutation
            assignments.push([jobId, selectedNodeId]);

            const targetState = simulatedState.get(selectedNodeId);
            targetState.q_sim += 1;
            targetState.mem_sim -= jobSize;

            // Dynamically penalize the base score (M_n) to prevent dogpiling in the same tick
            // λ = 0.05 penalty per assigned job
            targetState.baseScore = Math.max(0.01, targetState.baseScore - 0.05); 
        }

        return assignments;
    }

    /**
     * Helper: Selects a node probabilistically using Softmax
     */
    #selectSoftmax(bids) {
        let sumExp = 0;
        const exps = new Array(bids.length);

        // Calculate exponents
        for (let i = 0; i < bids.length; i++) {
            // Divide by temperature to sharpen differences (low temp = closer to argmax)
            const val = Math.exp(bids[i].bid / this.#SOFTMAX_TEMP);
            exps[i] = val;
            sumExp += val;
        }

        // Roulette wheel selection
        let random = Math.random() * sumExp;
        for (let i = 0; i < bids.length; i++) {
            random -= exps[i];
            if (random <= 0) {
                return bids[i].nodeId;
            }
        }
        
        return bids[bids.length - 1].nodeId; // Fallback
    }
}

export default Dispatcher;


/* 
for this assignment of jobs to a node i have decided to use
a Greedy Constrained Bipartite Assignment with Diminishing 
marginal utility.
*/