import { RuntimeOchestrator } from "./runtime/runtimeOchestrator.mjs";
import { RuntimeTaskManager } from "./runtime/runtimeTaskManger.mjs";
import StateInterface from "./core/state/stateInterface.mjs";

class LocalCoordinator {
    constructor(config = {}) {
        const sharedState = new StateInterface();
        config.State = sharedState;

        this.orchestrator = new RuntimeOchestrator(config);
        
        // Expose a fetch function for the task manager to request tasks from the orchestrator
        const apiFetch = (needed) => {
            return typeof this.orchestrator.requestTasks === 'function' ? this.orchestrator.requestTasks(needed) : [];
        };

        this.taskManager = new RuntimeTaskManager(apiFetch, config.maxSlots || 10, sharedState);
        this.interval = null;
    }

    start() {
        console.log("[Local Coordinator] Starting...");
        
        // Start the orchestrator schedule loop
        this.interval = setInterval(() => {
            if (typeof this.orchestrator.handleSchedule === 'function') {
                this.orchestrator.handleSchedule();
            }
            
            // Trigger task manager to request work if it has slots
            this.taskManager.requestWork();
        }, 1000);

        console.log("[Local Coordinator] System is running.");
    }

    stop() {
        if (this.interval) {
            clearInterval(this.interval);
        }
        console.log("[Local Coordinator] System stopped.");
    }
}

export { LocalCoordinator };

// If run directly:
if (process.argv[1] && process.argv[1].endsWith('index.mjs')) {
    const coordinator = new LocalCoordinator();
    coordinator.start();

    process.on('SIGINT', () => {
        coordinator.stop();
        process.exit(0);
    });
}
