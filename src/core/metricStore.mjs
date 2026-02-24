class MetricTable {
    constructor(fetchfunction,alpha,beta) {
        this.fetch=fetchfunction
        this.metricTable=new Map()
        this.deletedQueue=new Set()
        this.totalCoordinator=0
        this.alpha=alpha
        this.beta=beta
        this.strategies = {
                cpu_ema: (input, current) => {
                    return this.alpha * input + (1 - this.alpha) * (current || 0);
                },
                memory_ema: (input, current) => {
                    return this.alpha * input + (1 - this.alpha) * (current || 0);
                },
                queue_len_ema: (input, current) => {
                    return this.alpha * input + (1 - this.alpha) * (current || 0);
                },
                avg_job_time: (duration, current) => {
                if (!duration) return current;
                return (this.beta * duration) + ((1 - this.beta) * (current || 0));
                },
                last_heartbeat: (timestamp) => timestamp,
                alive: () => true,
                success_count:(newVal, oldVal) => (oldVal || 0) + 1,
                throughput: (newVal, oldVal) => (oldVal || 0) + 1,
                error_count:(newVal, oldVal) => (oldVal || 0) + 1,
                updated_at:()=>Date.now(),
                };
    }
    async fetchMetric(){
        const jobs=await this.fetch()
        await this.#addToQueue(jobs)
    }
    scaleUp(){

    }
    sclaleDown(){

    }
    async #addToQueue(metric){
        metric.forEach(element => {
            const id =element.id
            if (!this.metricTable.has(id)) {
                this.metricTable.set(id, element);
                this.totalCoordinator++;
            }
            
        });
    }
    updates(id,updateData){
        if(!this.metricTable.has(id))return false
        const metric=this.metricTable.get(id)
        for (const key of Object.keys(updateData)) {
                const newValue=updateData[key]
                const currentValue=metric[key]
                const strategy=this.strategies[key]

                if (strategy) {
                    metric[key] = strategy(newValue, currentValue);
                } else {
                    metric[key] = newValue;
                }
            }
        
    }
    
    deleteCoordinator(id){
        if(!this.metricTable.has(id))return false
        this.metricTable.delete(id)

        this.deletedQueue.add(id)
    }
    async write(){

    }
    async dbDelete(){

    }
}

export default MetricTable