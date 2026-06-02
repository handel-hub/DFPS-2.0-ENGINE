import { Piscina } from 'piscina';
import path from 'path';
import fs from 'fs/promises';
import os from 'os';
import { fileURLToPath } from 'url';
import { computeHashDirect } from './hasher.worker.js';

export class FileHashManager {
	static #instance = null;
	#pool = null;
	#isServerless = false;
	#isShuttingDown = false;

	constructor(config = {}) {

		this.#isServerless = !!(
			process.env.AWS_LAMBDA_FUNCTION_NAME ||
			process.env.VERCEL ||
			process.env.NETLIFY ||
			process.env.FUNCTION_NAME // Google Cloud Functions
		);

		if (this.#isServerless) {

			return;
		}

		const realCores = this.#detectAvailableCores();
		const maxThreads = config.maxThreads ?? Math.max(2, realCores - 1);

		const libuvSize = parseInt(process.env.UV_THREADPOOL_SIZE || '4', 10);
		if (maxThreads > libuvSize && process.env.NODE_ENV !== 'production') {
			console.warn(`[Env Check] Resource warning: maxThreads (${maxThreads}) > UV_THREADPOOL_SIZE (${libuvSize}).`);
		}

		// 4. BUNDLER-RESILIENT PATH RESOLUTION
		// Fallback order: Config Explicit Path -> Env Variable (for Docker/Bundlers) -> Local Dir
		let workerFile = config.workerPath || process.env.HASH_WORKER_PATH;
		if (!workerFile) {
			try {
				const __dirname = path.dirname(fileURLToPath(import.meta.url));
				workerFile = path.join(__dirname, 'hasher.worker.js');
			} catch {
				// Absolute fallback for environments where import.meta.url gets stripped by compilers
				workerFile = path.resolve('./hasher.worker.js');
			}
		}

		this.#pool = new Piscina({
			filename: workerFile,
			minThreads: config.minThreads ?? Math.max(1, Math.floor(realCores / 4)),
			maxThreads: maxThreads,
			maxQueue: config.maxQueue ?? 5000,
			idleTimeout: config.idleTimeout ?? 10000,
			});
	}

	static getInstance(config) {
		if (!FileHashManager.#instance) {
			FileHashManager.#instance = new FileHashManager(config);
		}
		return FileHashManager.#instance;
	}

	/**
	 * Environment-Agnostic verification strategy
	 */
	async verifyHash(targetPath, expectedHash, taskId, workerId, algorithm = 'sha256') {
		if (this.#isShuttingDown) throw new Error('System is tearing down.');
		if (!expectedHash || typeof expectedHash !== 'string') throw new TypeError('Invalid comparison hash format.');

		const cleanExpectedHash = expectedHash.trim().toLowerCase();
		
		const absolutePath = path.normalize(path.resolve(targetPath));

		try {
			const stats = await fs.stat(absolutePath);
			if (!stats.isFile()) throw new Error(`Not a regular file: ${absolutePath}`);
			await fs.access(absolutePath, fs.constants.R_OK);
		} catch (err) {
			if (err.code === 'ENOENT') throw new Error(`File not found: ${absolutePath}`);
			throw err;
		}

		if (this.#isServerless) {
			const directHash = await computeHashDirect({ filePath: absolutePath, algorithm });
			return directHash === cleanExpectedHash;
		}

		try {
			const { hash, durationMs } = await this.#pool.run(
				{ filePath: absolutePath, taskId, workerId, algorithm}
			);
			return { integrity : hash === cleanExpectedHash , durationMs };
		} catch (workerError) {
			const err = new Error(`Execution error: ${workerError.message}`);
			err.code = workerError.code;
			throw err;
		}
	}

	/**
   * Safely reads Linux Control Groups (cgroups) to identify real container limitations
   */
	#detectAvailableCores() {
		const logicalCPUs = os.cpus().length;
		
		if (process.platform !== 'linux') return logicalCPUs;

		try {
			// Read Linux cgroups v2 quota boundaries if present
			const cgroupQuota = fs.readFileSync('/sys/fs/cgroup/cpu.max', 'utf8').trim().split(' ');
			if (cgroupQuota.length === 2 && cgroupQuota[0] !== 'max') {
				const quotaLimit = Math.ceil(parseInt(cgroupQuota[0], 10) / parseInt(cgroupQuota[1], 10));
				if (quotaLimit > 0) return Math.min(logicalCPUs, quotaLimit);
			}
		} catch {
		}
		return logicalCPUs;
	}

	async shutdown() {
		this.#isShuttingDown = true;
		if (this.#pool) {
			await this.#pool.destroy();
		}
	}
}