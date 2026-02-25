export function schedule(jobs){
    if(!jobs||jobs.length===0){
        return null
    }
    return jobs[0]
}
