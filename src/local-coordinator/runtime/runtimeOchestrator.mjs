import { StateInterface } from "../core/state/index.mjs";
import { ExternalJobQueue } from "../infrastructure/index.mjs";
import { DAGBuilder } from "../core/dag-builder/index.mjs";
import { ProcessPoolOrchestrator } from "../core/pool-manager/index.mjs";
import { extract } from "../utils/index.mjs";
import { EventEmitter } from "node:events";
import os from "node:os";
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
	#errorLog

	constructor(config = {}) {
		super();
		const workerId = config.workerId || 'local-coordinator';

		this.schedulingNum = config.schedulingNum ?? 10

		this.#maxDag = config.maxDag ?? 1000; //value is a guesse one refinement will be done after tests 
		this.#State = config.State || new StateInterface();
		this.#extract = extract;
		this.#Queue = new ExternalJobQueue();
		this.#Dag = new DAGBuilder();
		this.#Pool = new ProcessPoolOrchestrator({}, () => {});
		
		this.#Wal = new Wal({ walDir: './error', workerId })
		this.#Batcher = config.Batcher || new WorkerBatcher(this.#Wal, this.fetchBatchFn.bind(this), {
				workerId: workerId,
				storageMode: 'both',
                grpcSendFn: config.grpcSendFn || (async () => ({ acceptedUpTo: Date.now() }))
			});	

		
		this.#errorLog = [];
		this.#dagList = [];
		this.#toExecDag = []
		this.#sequence = 0;
		this.#dagPoint = 0

		this.#execDagMap = new Map()
		this.failed = []

		this.on('jobsAvailable',(data)=>{
			this.#handleJob(data)
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
            error: payload,
            timestamp: this.#now(),
            sequenceId: this.#nextSeq()
        };
        this.#errorLog.push(ev);
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

	handleSchedule() {
		const addExec = this.schedulingNum - this.#execDagMap.size;

		if (addExec > 0) {
			const newTask = this.#toExecDag.slice(0, addExec);
			
			const filteredTask = this.scan(newTask);
			filteredTask.forEach((task) => {
				const id = task.taskId || task.task_id;
				if (id) {
					this.#execDagMap.set(id, task);
				}
			});

			this.#toExecDag = this.#toExecDag.slice(addExec);
		}
		
	}




	scan(tasks = []) {
		return tasks.filter(task => !this.failed.includes(task.jobId));
	}

	requestTasks(needed) {
		const tasks = [];
		const keys = Array.from(this.#execDagMap.keys()).slice(0, needed);
		for (const key of keys) {
			const rawTask = this.#execDagMap.get(key);
			tasks.push({
				taskId: rawTask.task_id,
				pluginId: rawTask.plugin_id,
				filePath: rawTask.file_path, // Could be extracted if needed
				payload: rawTask,
				ignoreMemoryCheck: rawTask.ignoreMemoryCheck || rawTask.ignore_memory_check || false
			});
			this.#execDagMap.delete(key);
		}
		return tasks;
	}
}

export { RuntimeOchestrator };