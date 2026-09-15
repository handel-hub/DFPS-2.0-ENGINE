import { EventEmitter } from "node:events";

export class LifecycleMonitor extends EventEmitter {
    constructor(taskDispatcher) {
        super();
        this.dispatcher = taskDispatcher;
        
        if (this.dispatcher) {
            this.dispatcher.on("TASK_UPDATE", this.#handleTaskUpdate.bind(this));
            this.dispatcher.on("TASK_FAILED", this.#handleTaskFailed.bind(this));
        }
    }

    #handleTaskUpdate(event) {
        // Expected shape from Dispatcher: { taskId, workerId, payload }
        // where payload is the JSONL object
        const { taskId, payload } = event;
        if (!payload) return;

        if (payload.type === "lifecycle") {
            this.emit("LIFECYCLE_PHASE", {
                taskId: taskId || payload.job_id,
                phase: payload.event
            });
        } else if (payload.type === "result") {
            this.emit("LIFECYCLE_RESULT", {
                taskId: taskId || payload.job_id,
                status: payload.status,
                metrics: payload.execution_metrics,
                artifacts: payload.artifacts,
                error: payload.error_logs
            });
        }
    }

    #handleTaskFailed(event) {
        // OS-level crash emitted by the dispatcher
        const { taskId, error } = event;
        this.emit("LIFECYCLE_CRASH", {
            taskId,
            error: error || "Worker process crashed abruptly"
        });
    }
}
