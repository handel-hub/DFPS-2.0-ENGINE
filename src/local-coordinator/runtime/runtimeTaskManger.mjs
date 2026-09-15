import LocalNodeDispatcher from "./taskDispatcher.mjs";
import { LifecycleMonitor } from "./lifecycleMonitor.mjs";
import StateInterface from "../core/state/stateInterface.mjs";

export class RuntimeTaskManager {
    constructor(apiFetchFunction, maxSlots = 10, sharedState = null) {
        this.api = apiFetchFunction;
        this.maxSlots = maxSlots;
        this.dispatcher = new LocalNodeDispatcher();
        this.StateInterface = sharedState || new StateInterface();
        this.lifecycleMonitor = new LifecycleMonitor(this.dispatcher);

        this.#initListeners();
    }

    #initListeners() {
        this.lifecycleMonitor.on("LIFECYCLE_PHASE", ({ taskId, phase }) => {
            this.StateInterface.updateRuntimePhase(taskId, phase, {});
        });

        this.lifecycleMonitor.on("LIFECYCLE_RESULT", ({ taskId, status, metrics, artifacts, error }) => {
            if (status === "SUCCESS") {
                this.StateInterface.markTaskCompleted(taskId, { metrics, artifacts });
                if (metrics) {
                    this.StateInterface.updateProfilesSynchronous(taskId, metrics);
                }
            } else {
                this.StateInterface.markTaskFailed(taskId, error);
            }
            this.requestWork();
        });

        this.lifecycleMonitor.on("LIFECYCLE_CRASH", ({ taskId, error }) => {
            this.StateInterface.markTaskFailed(taskId, error);
            this.requestWork();
        });
    }

    availableSlots() {
        const activeCount = this.StateInterface.getActiveTaskCount();
        return Math.max(0, this.maxSlots - activeCount);
    }

    requestWork() {
        if (typeof this.api !== 'function') return;
        const needed = this.availableSlots();
        if (needed > 0) {
            const tasks = this.api(needed);
            if (tasks && Array.isArray(tasks) && tasks.length > 0) {
                console.log("[DEBUG] Tasks received:", tasks);
                this.accept(tasks);
            }
        }
    }

    accept(tasks = []) {
        for (const task of tasks) {
            this.StateInterface.markTaskDispatched(task.taskId, task.pluginId);
            const jobPayload = {
                taskId: task.taskId,
                pluginId: task.pluginId,
                filePath: task.filePath || task.payload?.filePath,
                parameters: task.parameters || task.payload?.parameters,
                ignoreMemoryCheck: task.ignoreMemoryCheck || task.payload?.ignoreMemoryCheck
            };
            this.dispatcher.dispatchJob(jobPayload);
        }
    }
}
