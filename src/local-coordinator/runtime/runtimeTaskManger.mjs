import LocalNodeDispatcher from "./taskDispatcher.mjs"
import StateInterface from "../core/state/stateInterface.mjs";

//const dispatcher 

class name {
	#state = new Map()
	
	constructor(fetchFunction,hashfunction,dispatcheinstance) {

		this.dispatcher = new LocalNodeDispatcher();
		this.StateInterface = new StateInterface()
		

	}
	update(event){

	}

	#schema(data){
		const {jobId,taskId} = data
		return {
			jobId,
			taskId,
			currentState : 'PENDING',
			startedAt : Date.now(),
			updatedAt : null,
			workerId : null,
			retries : null,
			outputHash : null,
			metadata : data
		}
	}

	registerTask(task){
		const schema = this.#schema(task)
		this.#state.set(schema.taskId,schema)
		


	}

	analyzEvent(event){

		const {
			type,
			pluginId, 
			workerId, 
			taskId, 
			slotId, 
			data, 
			err 
		} = event

		switch (type) {
			case value:
				
				break;
		
			default:
				break;
		}

	}



























	 // ===================================================================
    // EVENT HANDLERS (Lower Tier Translation)
    // ===================================================================

    #handleCapacity(nodeId, payload, isAvailable) {
        const node = this.#nodes.get(nodeId);
        if (!node) return;

        if (node.status === "DRAINING" || node.status === "OFFLINE") return;

        node.availableCapacity = isAvailable ? (payload.idleCount || 1) : 0;
        
        if (isAvailable) {
            this.#routeTraffic();
        }
    }
	#handleTaskAccepted(nodeId, payload) {
        const { taskId } = payload;
        const task = this.#activeTasks.get(taskId);
        if (task && task.status === "DISPATCHING") {
            task.status = "RUNNING";
            this.emit("TASK_RUNNING", { taskId, nodeId });
        }
    }
	#handleTaskFailed(nodeId, payload) {
        const { taskId, reason } = payload;
        const task = this.#activeTasks.get(taskId);
        const node = this.#nodes.get(nodeId);

        if (node) node.assignedTasks.delete(taskId);

        if (!task) return;

        console.warn(`[RTM] Task ${taskId} failed on ${nodeId}. Reason: ${reason}`);

        if (task.attempts >= this.config.maxRetries) {
            task.status = "DEAD_LETTER";
            task.failedAt = Date.now();
            task.finalReason = reason;
            this.#deadLetterQueue.set(taskId, task);
            this.#activeTasks.delete(taskId);
            this.emit("TASK_DEAD_LETTER", { taskId, attempts: task.attempts, reason });
        } else {
            task.status = "QUEUED";
            task.assignedNode = null;
            task.dispatchedAt = null;
            // Place at the front of the queue to prioritize retries
            this.#globalQueue.unshift(taskId); 
            this.emit("TASK_REQUEUED", { taskId, nextAttempt: task.attempts + 1 });
            this.#routeTraffic();
        }
    }
	#handleNodeFailure(nodeId, payload) {
        const node = this.#nodes.get(nodeId);
        if (!node || node.status === "OFFLINE") return;

        console.error(`[CRITICAL] Evacuating Node ${nodeId} due to critical error:`, payload);
        node.status = "OFFLINE";
        node.availableCapacity = 0;

        // Reclaim all tasks assigned to this node
        for (const taskId of node.assignedTasks) {
            this.#handleTaskFailed(nodeId, { taskId, reason: "NODE_EVACUATION" });
        }

        node.assignedTasks.clear();
        this.emit("NODE_OFFLINE", { nodeId, reason: payload });
    }

}

