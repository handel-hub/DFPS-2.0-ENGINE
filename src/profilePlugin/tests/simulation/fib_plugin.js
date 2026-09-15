import fs from 'fs/promises';
import path from 'path';
import readline from 'readline';

// The deliberately slow Fibonacci algorithm to spike CPU
function fibonacci(n) {
    if (n <= 1) return n;
    return fibonacci(n - 1) + fibonacci(n - 2);
}

function sendEvent(data) {
    console.log(JSON.stringify(data));
}

async function processTask(task) {
    const { taskId, filePath } = task;
    
    // Lifecycle: Input Read
    sendEvent({ type: "lifecycle", event: "Input Read Started", timestamp: new Date().toISOString() });
    
    let numberToProcess = 30; // fallback
    try {
        const content = await fs.readFile(filePath, 'utf8');
        numberToProcess = parseInt(content.trim(), 10) || 30;
    } catch (e) {
        // Ignored
    }
    
    sendEvent({ type: "lifecycle", event: "Input Read Completed", timestamp: new Date().toISOString() });
    
    // Lifecycle: Processing
    sendEvent({ type: "lifecycle", event: "Processing Started", timestamp: new Date().toISOString() });
    
    const result = fibonacci(numberToProcess);
    
    sendEvent({ type: "lifecycle", event: "Processing Completed", timestamp: new Date().toISOString() });
    
    // Lifecycle: Output Write
    sendEvent({ type: "lifecycle", event: "Output Write Started", timestamp: new Date().toISOString() });
    
    try {
        const outDir = path.dirname(filePath) + "_out";
        await fs.mkdir(outDir, { recursive: true });
        const outPath = path.join(outDir, `${path.basename(filePath, path.extname(filePath))}.out`);
        await fs.writeFile(outPath, `Fibonacci(${numberToProcess}) = ${result}\n`);
    } catch (e) {
        // Ignored
    }
    
    sendEvent({ type: "lifecycle", event: "Output Write Completed", timestamp: new Date().toISOString() });
    
    // Final Result
    sendEvent({
        type: "result",
        status: "COMPLETED",
        execution_metrics: { fibonacci_n: numberToProcess, fibonacci_result: result },
        artifacts: { output: `${path.basename(filePath, path.extname(filePath))}.out` }
    });
}

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
    terminal: false
});

rl.on('line', (line) => {
    if (!line.trim()) return;
    try {
        const payload = JSON.parse(line);
        if (payload.action === 'PROCESS_FILE') {
            processTask(payload).catch(err => {
                console.error(JSON.stringify({ error: err.message }));
                sendEvent({
                    type: "result",
                    status: "FAILED",
                    execution_metrics: {},
                    artifacts: {}
                });
            });
        }
    } catch (e) {
        console.error(JSON.stringify({ error: "Invalid JSON input", details: e.message }));
    }
});
