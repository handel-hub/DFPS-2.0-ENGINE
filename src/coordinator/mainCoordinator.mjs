import { fetchPendingJobs,getCoordinatorMetrics } from "../infrastructure/db.mjs";
import { schedule } from "../core/scheduler.mjs";

export async function runOnce(){
    const jobs=await getCoordinatorMetrics()
    const selected=schedule(jobs)
    if(!selected){
        console.log('NO pending jobs')
        return
    }

    console.log(jobs)
    
}