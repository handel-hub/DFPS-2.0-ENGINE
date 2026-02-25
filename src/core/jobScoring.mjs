class AdaptiveJobScoringEngine {
    #fileSnapshots;
    #EPSILON;
    #T_AGING;
    #MAX_AGING
    #N_MIN;
    #W_MAX;
    #W_MIN;
    #snapshotFunction
    constructor(config={},polcyConfig={},snapshotFunction) {
        this.#MAX_AGING=config.MAX_AGING ||3.0;
        this.#T_AGING=polcyConfig.T_AGING ||3600000;
        this.#W_MAX=polcyConfig.W_MAX||1.5;
        this.#W_MIN=polcyConfig.W_MIN ||0.1;
        this.#EPSILON=polcyConfig.EPSILON || 2;
        this.#N_MIN=polcyConfig.N_MIN || 50
        this.#fileSnapshots=new Map()

        //connects to jobweight table
        this.#snapshotFunction=snapshotFunction
    }

    #refreshFileSnapshot(){
    if (typeof this.#snapshotFunction !== 'function'){}

    const snapshot=this.#snapshotFunction()
        for (const [key,data] of snapshot) {
            const weight=this.#calculateTypeWeight(data)
            this.#fileSnapshots.set(key,weight)
        }   
    }
    
    #calculateTypeWeight(stats) {
        const { successCount, failCount, expectedTime, actualTimeEMA, totalSamples } = stats;

        //Bayesian Yield
        const yieldK = (successCount + this.#EPSILON) / 
                    (successCount + failCount + (2 * this.#EPSILON));

        //Raw Measured Weight
        const wRaw = (expectedTime / Math.max(1, actualTimeEMA)) * yieldK;

        //Credibility Blending
        const confidence = Math.min(1.0, totalSamples / this.#N_MIN);
        const wBlended = (confidence * wRaw) + ((1 - confidence) * 1.0);

        //Bounded Final Weight
        return Math.max(this.#W_MIN, Math.min(this.#W_MAX, wBlended));
    }

    #scoreJob(job) {
        const { priority, created_at , size_bytes, type} = job;
        //uptime
        const uptime=Date.now()-created_at
        // Capped Aging Multiplier
        const agingMultiplier = Math.min(
        this.#MAX_AGING, 
        1.0 + (uptime / this.#T_AGING)
        );

        // Sublinear Size Penalty: 1 / sqrt(size)
        const sizePenalty = Math.sqrt(Math.max(0.1, size_bytes));

        // Score = Priority * Aging * (Health / Size)
        return priority * agingMultiplier * ((this.#fileSnapshots.get(type)|| 1) / sizePenalty);
    }


    
    scoreJobs(pendingJobs = []) {
        this.#refreshFileSnapshot();

        const length = pendingJobs.length;
        const shadowArray = new Array(length);

        for (let i = 0; i < length; i++) {
            const jobTuple = pendingJobs[i];
            const score = this.#scoreJob(jobTuple[1]);

            shadowArray[i] = [jobTuple, score];
        }

        shadowArray.sort((a, b) => b[1] - a[1]);

        const sortedJobs = new Array(length);
        
        for (let i = 0; i < length; i++) {
            const originalTuple = shadowArray[i][0];
            const calculatedScore = shadowArray[i][1];

            originalTuple[1].calculatedScore = calculatedScore;
            
            sortedJobs[i] = originalTuple;
        }

        return sortedJobs;
    }
}
export default AdaptiveJobScoringEngine
//add job type field to the job or file table