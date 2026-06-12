import { StateInterface } from "../core/state/index.mjs";
import { ExternalJobQueue } from "../infrastructure/index.mjs";
import { DAGBuilder } from "../core/dag-builder/index.mjs";
import { ProcessPoolOrchestrator } from "../core/pool-manager/index.mjs";
import { extract } from "../utils/index.mjs";
import { EventEmitter } from "node:events";
import { 
    Wal,
    WorkerBatcher 
} from "../infrastructure/index.mjs";

class RuntimeOchestrator extends EventEmitter {
	#maxDag
	#errors
	#State;
	#extract;
	#Queue;
	#Pool;
	#Dag
	#Wal
	#Batcher
	#dag
	#dagList
	#toExecDag
	#execDagMap

	#sequence
	#dagPoint
	constructor(config = {}) {

		this.schedulingNum = config.schedulingNum ?? 10

		this.#maxDag = config.maxDag ?? 1000; //value is a guesse one refinement will be done after tests 
		this.#State = new StateInterface();
		this.#extract = extract;
		this.#Queue = new ExternalJobQueue();
		this.#Dag = new DAGBuilder();
		this.#Pool = new ProcessPoolOrchestrator();
		
		this.#Wal = new Wal({ walDir: './error', workerId })
		this.#Batcher = opts.Batcher || new WorkerBatcher(this.#Wal, this.fetchBatchFn, {
				workerId: workerId,
				storageMode: 'both',
				// ... grpc hooks go here
			});	

		
		this.#errorLog = [];
		this.#dagList = [];
		this.#toExecDag = []
		this.#sequence = 0;
		this.#dagPoint = 0

		this.#execDagMap = new Map()
		this.failed = []
		this.#execDagMap.set('queue',Array(Number(config.workerSlot ?? Math.max(1, os.cpus().length - 1))))

		this.on('jobsAvailable',(data)=>{
			#handleJob(data)
		})
	}
	    // -------------------------
    // Internal helpers
    // -------------------------
    #now() { return Date.now(); }
    #nextSeq() { this.#sequence += 1; return this.#sequence; }

    #appendChange(type, jobId, taskId = null, payload = {}) {
        const ev = {
            type,
            jobId,
            error,
            timestamp: this.#now(),
            sequenceId: this.#nextSeq()
        };
        this.errorLog.push(ev);
        return ev;
    }

	#handleJob(data){
		const extractedData = this.#extract(data)
		
		//pushing malformed jobs upward to handle them centrally  
		this.#handleErrors(extractedData.failed)
		
		//initializing jobs processing that passed 
		const {normalizedJob,context,dag} = extractedData.passed
		const completeContext = this.#State.completeContext(context)
		this.#State.registerJobs(normalizedJob)
		const dagData = this.#Dag.buildBatch(dag,completeContext)	
		
		this.#dagList.push(
			{
				dagNumber:this.#dagPoint++,
				dag: dagData
			}
		)
	}

	#handleErrors(errors = []){
		if (!Array.isArray(errors) || errors.length === 0) return;

		for (const err of errors) {
			const {jobId,phase,errors} = err;
			this.#appendChange(phase,jobId,null,errors);
		}
	}

	fetchBatchFn(fromSeq = 0, {maxEvents=200, maxBytes = 256 * 1024 * 1024, coalesce = true, coalesceWindowMs = 500} = {}){
		if (!Number.isFinite(fromSeq) || fromSeq < 0) fromSeq = 0;
        const events = this.#errorLog.filter(e => e.sequenceId > fromSeq);
        if (!events.length) return { fromSeq, toSeq: fromSeq, events: [], meta: { count: 0, bytes: 0 } };

		const batch = [];
        let bytes = 0;
        for (const e of events) {
            const s = JSON.stringify(e);
            const len = Buffer.byteLength(s, 'utf8');
            if (batch.length >= maxEvents) break;
            if (bytes + len > maxBytes) break;
            batch.push(e);
            bytes += len;
        }
        const toSeq = batch.length ? batch[batch.length - 1].sequenceId : fromSeq;
        return { fromSeq, toSeq, events: batch, meta: { count: batch.length, bytes } };
    
	}

	////////MAIN OCHESTRATION//////////////
	//////////////////////////////////////

	handleSchedule(){
		const addExec = this.schedulingNum - this.#execDagMap.size

		if (addExec > 0) {
			const newTask = this.#toExecDag.slice(-addExec)
			const filteredTask = this.scan(newTask)
			filteredTask.forEach((task)=>{
				this.#execDagMap.set(task.taskId,task)
			})

			const newSetNum = this.#toExecDag.length - addExec
			const newSetDag = this.#toExecDag.slice(0,newSetNum)
			this.#toExecDag = newSetDag
			
		}

	}

	scan(tasks = []) {
		return tasks.filter(task => !this.failed.includes(task.jobId));
	}


}