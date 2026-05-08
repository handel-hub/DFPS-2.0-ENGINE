This is the comprehensive "Master Map" of fault points, edge cases, and architectural traps for your Node.js Medical Image Admission System. I have aggregated them into a logical lifecycle.
## Phase 1: Admission & Spawning (The "Budget" Phase)

* The Ghost Process (ENOENT/EACCES): The OS refuses to create the process (invalid path or permissions).
* Action: Catch via child.on('error'); immediately refund the RAM budget.
* The Spawn Tax (Interpreter Overhead): Python/Node runtimes eat 30-50MB before your code even runs.
* Action: Your 7.5GB simulation must include a "Fixed Runtime Overhead" per process.
* The Copy-on-Write (CoW) Illusion: Shared memory makes RAM look "free" until the worker actually starts modifying image pixels.
* Action: Always reserve the Peak RSS estimate immediately; never rely on real-time OS reporting for admission.
* Path/Environment Length: Windows 260-character limits or missing PATH variables.
* Action: Sanitize paths and use absolute paths in your NDJSON payloads.

## Phase 2: The Handshake (The "I'm Alive" Phase)

* The Immediate Silent Death (DOA): Process gets a PID but crashes (segfault/syntax error) before communicating.
* Action: Monitor spawn event vs. exit event timing.
* The Startup Timeout: The process hangs during initialization (e.g., waiting for a license or a heavy library to load).
* Action: If a {"status": "READY"} NDJSON is not received within $X$ seconds, SIGKILL and reclaim RAM.
* Zombie Console Windows: On Windows, failing to use windowsHide: true causes UI flicker and potential user interference.

## Phase 3: Active Processing (The "Execution" Phase)

* The Zombified Worker (Logic Hang): The worker is stuck in an infinite loop but still "alive."
* Action: Implement an NDJSON Heartbeat. No heartbeat = SIGTERM.
* Uninterruptible Sleep (Status D): The worker is stuck waiting for a failing disk/network drive. It cannot be killed by the OS.
* Action: Mark RAM as "Hostage" in your budget. You cannot reuse this space until the process physically disappears from the PID list.
* Pipe Saturation & Deadlock: Both parent and child fill their 64KB buffers and wait for each other.
* Action: Use non-blocking stream reads; never use writeSync for IPC.
* Parser Poisoning (Dirty Stdout): A stray printf in C++ breaks your JSON.parse in Node.
* Action: Wrap parsing in try-catch; treat invalid lines as "unstructured logs" rather than system failures.

## Phase 4: Termination & Cleanup (The "Reclamation" Phase)

* The OOM-Killer Ambiguity: The OS kills your process via SIGKILL without your command.
* Action: If exitCode is null and signal is SIGKILL, assume a system-wide OOM; halt the admission of new jobs.
* The Resource Leak (Partial Completion): A crash leaves .tmp files or shared memory segments behind.
* Action: Each DAG node must have a cleanup() hook that triggers on any non-zero exit code.
* Stale PID Collision: The OS reuses a PID before your coordinator realizes the old process died.
* Action: Always use the Node.js ChildProcess Object, never just the raw PID number for kill() operations.
* The Zombie File Lock: A dead process still holds a lock on a DICOM file, blocking the next DAG node.
* Action: Implement a "Lock-Check/Retry" logic before spawning a dependent node.
* The Non-Zero Success: Binary returns exit code 1 despite finishing the work.
* Action: Treat your "Success" NDJSON as the Source of Truth, not the numeric exit code.

## Phase 5: Management (The "Maintenance" Phase)

* Ref/Unref Logic: Accidentally exiting the coordinator while workers are mid-image.
* Action: Keep workers ref()'d; only exit the coordinator when your "Active Processes" counter is zero.
* Signal Race Conditions: Sending SIGTERM at the exact millisecond the worker finishes.
* Action: Use a 50ms "Cooldown" window where you ignore exit signals if a "DONE" message was just received.

This covers the full spectrum. By implementing a "State Record" for every process that tracks these specific failure points, your admission system will be mathematically sound.
Would you like a JSON schema for that "Process Record" to help your state engine track these transitions?

