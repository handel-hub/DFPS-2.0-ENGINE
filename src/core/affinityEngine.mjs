class Affinity {

    #numSegment;
    #segmentRange;

    #variance_penalty_lambda;
    #ema_alpha;
    #min_sample_threshold;
    #temporal_decay_tau_seconds;

    #prior_alpha;
    #prior_beta;
    #adaptation_rate_rho;

    #dataStructure;

    constructor(sizeSegmentation = {}, policyConfig = {}) {

        this.#numSegment = sizeSegmentation.numSegment;
        this.#segmentRange = sizeSegmentation.segmentRange;

        this.#variance_penalty_lambda = policyConfig.variance_penalty_lambda || 0.75;
        this.#ema_alpha = policyConfig.ema_alpha || 0.2;

        this.#min_sample_threshold = policyConfig.min_sample_threshold || 50;
        this.#temporal_decay_tau_seconds = policyConfig.temporal_decay_tau_seconds || 86400;

        this.#prior_alpha = policyConfig.prior_alpha || 2;
        this.#prior_beta = policyConfig.prior_beta || 2;

        this.#adaptation_rate_rho = policyConfig.adaptation_rate_rho || 0.02;

        this.#dataStructure = new Map();

        this.#initialize();
    }

    #initialize() {
        for (let index = 0; index < this.#numSegment; index++) {
            const range = this.#segmentRange[index];
            const rangeName = Object.keys(range)[0];
            this.#dataStructure.set(rangeName, new Map());
        }
    }

    #variables() {
        return {
            rawStats: {
                totalS: 0,          // lifetime success count
                totalN: 0,          // lifetime total count
                timestamp: 0        // last update time
            },
            derived: {
                emaTime: 0,         // μ_k
                deviation: 0,       // σ_k

                smoothSuccess: 0,   // S̃_k
                smoothTotal: 0      // Ñ_k
            },
            result: 0
        };
    }

    #bucketSegment(size) {
        let bucket = '';
        this.#segmentRange.forEach(elem => {
            const key = Object.keys(elem)[0];
            const min = elem[key].min;
            const max = elem[key].max;

            if (size >= min && size <= max) {
                bucket = key;
            }
        });
        return bucket;
    }

    #updateAffinity(id, value, pipeline) {

        const { time, size, successIndicator, timestamp } = value;

        const bucket = this.#bucketSegment(size);
        const pipelineSegment = this.#dataStructure.get(bucket);

        if (!pipelineSegment.has(pipeline)) {
            pipelineSegment.set(pipeline, new Map());
        }

        const lcSegment = pipelineSegment.get(pipeline);

        if (!lcSegment.has(id)) {
            lcSegment.set(id, this.#variables());
        }

        const segment = lcSegment.get(id);

        // ----------------------------
        // PERFORMANCE (FAST TIMESCALE)
        // ----------------------------

        const execution = time / size; // x_k

        const prevMu = segment.derived.emaTime;
        const newMu =
            (this.#ema_alpha * execution) +
            ((1 - this.#ema_alpha) * prevMu); // μ_k

        const deviationInput = Math.abs(execution - newMu);

        const prevSigma = segment.derived.deviation;
        const newSigma =
            (this.#ema_alpha * deviationInput) +
            ((1 - this.#ema_alpha) * prevSigma); // σ_k

        segment.derived.emaTime = newMu;
        segment.derived.deviation = newSigma;

        // ----------------------------
        // HYBRID RELIABILITY (MEDIUM)
        // ----------------------------

        segment.derived.smoothSuccess =
            this.#adaptation_rate_rho * successIndicator +
            (1 - this.#adaptation_rate_rho) * segment.derived.smoothSuccess; // S̃_k

        segment.derived.smoothTotal =
            this.#adaptation_rate_rho +
            (1 - this.#adaptation_rate_rho) * segment.derived.smoothTotal; // Ñ_k

        // ----------------------------
        // LIFETIME STRUCTURAL STATS
        // ----------------------------

        if (successIndicator === 1) {
            segment.rawStats.totalS += 1;
        }

        segment.rawStats.totalN += 1;
        segment.rawStats.timestamp = timestamp;
    }

    runUpdateAffinty(value) {
        const id = value.id;

        value.data.forEach(element => {
            const pipeline = element.pipeline;
            const val = element.value;
            this.#updateAffinity(id, val, pipeline);
        });
    }

    // ----------------------------
    // COMPONENTS
    // ----------------------------

    #performance(mu, sigma) {
        return 1 / (mu + (this.#variance_penalty_lambda * sigma) + 1e-9);
    }

    #reliability(smoothS, smoothN) {
        return (
            (smoothS + this.#prior_alpha) /
            (smoothN + this.#prior_alpha + this.#prior_beta)
        );
    }

    #structuralConfidence(totalN, timestamp) {

        const structural =
            totalN / (totalN + this.#min_sample_threshold);

        const deltaSeconds =
            (Date.now() - timestamp) / 1000;

        const temporal =
            Math.exp(-deltaSeconds / this.#temporal_decay_tau_seconds);

        return structural * temporal;
    }

    #calculateResult(segment) {

        const P = this.#performance(
            segment.derived.emaTime,
            segment.derived.deviation
        );

        const R = this.#reliability(
            segment.derived.smoothSuccess,
            segment.derived.smoothTotal
        );

        const C = this.#structuralConfidence(
            segment.rawStats.totalN,
            segment.rawStats.timestamp
        );

        return P * R * C;
    }
    prunning(){

    }
    getAffinity(pipeline, size, validNodeIds = []) {
        const length = validNodeIds.length;

        const results = new Array(length);
        
        const bucket = this.#bucketSegment(size);
        const pipelineSegment = this.#dataStructure.get(bucket);

        if (!pipelineSegment || !pipelineSegment.has(pipeline)) {
            const fallbackScore = this.#calculateFallback();
            for (let i = 0; i < length; i++) {
                results[i] = { id: validNodeIds[i], score: fallbackScore };
            }
            return results;
        }

        const lcSegment = pipelineSegment.get(pipeline);

        for (let i = 0; i < length; i++) {
            const nodeId = validNodeIds[i];
            const segment = lcSegment.get(nodeId);

            let finalScore;
            if (segment) {

                finalScore = this.#calculateResult(segment);
            } else {

                finalScore = this.#calculateFallback();
            }

            results[i] = { id: nodeId, score: finalScore };
        }

        results.sort((a, b) => b.score - a.score);

        return results;
    }


    #calculateFallback() {
        return 0.1; 
    }
}
export default Affinity;







/* 
reanlyzing to restucture rare file buckets

to instatiate with one more bucket if the file type sees it fit that will be complexity
class for a more granular segmentation and information this wiggle room for this 
robustness will be done on later days.

variance penalty affected by lamda


room for improvemnts properly modelling O(n log(n) and O(n^2) more of
constant overhead processing + linear processingpresent system models workloads 
that scales linearly. example of scenarior heavily compressed file and raw file.
i reccomend segmenting the complexity bucket by file type, compression type and
modality, regresional analysis could be done.

system model profers a solution that helps reduce the effect of the issues above
without formaly addressing the model, by usingsegmentation by file size this is 
default aim is to reduce the variance score without applying regression.
i was quite cormfortable with this system. if during your testing and and result are 
indesirable  you could consider the previous reccomendation above.
another reason for not going the regresion route is such a solution will be undesirable
in node js  perhaps python, c++, rust, java, go will produce more desirable result

explicit temporal decay
two layers of application is at structural confidence and reliability estimate.
structural confidence is an epistemic certinty which is decayed explicitly this is tunable 
at the policy config.
reliability decay can be applied too but should be at a much more slower rate as the default
confidence temporal decay (i.e success rate decays temporally)
performance already decays implictly temporally through EMA

system does not incorporate reliability decay

desired Redemption Latency for a node:
redemption of nodes that begin to show positive signs of improvemnts after series of bad performance
is set to be slow and this was done by default because the system is assmed to be conservatve.

affnity does not factor latency of any sort 'infrastrutucre'(disk failures or shared resource contention)
affnity is merely behavioural infrastructure degradation affect the system it does not include any model 
to mitigate such issues of any sort. node redemption latency becomes the major draw back. but this is
tolerable as the system improves with time 


affinity incorporate full processing pipeline because that is desirable but stage level affinity becomes
necesssary if pipeline crosses over differnt hardware path (cpu and gpu), stages are independently scheduled
and many more scenerios not listed.



*/

