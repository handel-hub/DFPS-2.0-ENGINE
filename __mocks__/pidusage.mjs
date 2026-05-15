// Simple deterministic pidusage mock
export default async function pidusage(pids) {
    const out = {};
    for (const pid of Array.isArray(pids) ? pids : Object.keys(pids)) {
        out[pid] = {
            cpu: 0.5,
            memory: 20 * 1024 * 1024,
            elapsed: 1000,
            timestamp: Date.now()
        };
    }
    return out;
}

export function clear() {
    // no-op
}

export function unmonitor(pid) {
    // no-op
}
