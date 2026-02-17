class GlobalJobQueue {
    constructor(writeFunction) {
        this.globalJobQueue= new Map(),
        this.structureChange=false,
        this.jobScheduleQueue=[],
        this.writeDbQueue=new Map()
        this.deleteWriteJobs=false
        this.writeFunction=writeFunction
    }
    async fetch(fetchFunction,coordinatorId,limit){
        const jobs=await fetchFunction(coordinatorId,limit)
        this.#addToQueue(jobs)
    }
    #addToQueue(jobs){
        const data=jobs
        data.forEach(element => {
            const id =element.id
            this.globalJobQueue.set(id,element)
        });
    }
    update(id,updateData){
        if(this.globalJobQueue.has(id))return false
        const job=this.globalJobQueue.get(id);
        for (const key of Object.keys(updateData)) {
            const value=updateData[key]
            job[key]=value
        }
        this.#updateWriteQueue(id,updateData)
    }
    #updateWriteQueue(id,updateData){
        if (this.writeDbQueue.has(id)) {
            const job=this.writeDbQueue.get(id);
            for (const key of Object.keys(updateData)) {
                const value=updateData[key]
                job[key]=value
            }
        } else {
            this.writeDbQueue.set(id,updateData)
        }
    }
    getStatusAndCount(){
        const result=[]
        this.writeDbQueue.forEach((value,key)=>{
            const data={
                count:value.count,
                status:value.status
            }

            result.push({[key]:data})
        })
        return result
    }
    async write(del=false){
        // to be when write function is completed
        await this.writeFunction
        let writeAck// jus mock for now write returns ack to ensure successful write before final deletion
        if (del===true&writeAck) {
            this.deleteWriteJobs=true
        }
    }
    async deleteJobs(jobIds,del=false){
        const jobs=jobIds
        for (const job of jobs) {
            this.globalJobQueue.delete(job)
        }
        await this.write(del)
        if (del===true&&writeAck) {
            for (const job of jobs) {
                this.writeDbQueue.delete(job)
            }
        }
    }
    scheduleQueue(lastId,size,fromstart=false){
        this.jobScheduleQueue.length=0
        let count=0
        const jobSize=size
        let flag=false
        if (fromstart===true) {
            for (const [jobId,jobData] of this.globalJobQueue) {
                if (count >= jobSize) break;
                this.jobScheduleQueue.push(jobData);
                count++;

            }
            return this.jobScheduleQueue
        }
        for (const [jobId,jobData] of this.globalJobQueue) {
            if (flag) {
                if (count >= jobSize) break;
                this.jobScheduleQueue.push(jobData);
                count++;
            }
            if (jobId===lastId) {
                flag=true
            }
        }
        return this.jobScheduleQueue
    }
}