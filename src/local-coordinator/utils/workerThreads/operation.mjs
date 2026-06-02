import { isMainThread } from 'worker_threads';
import fs from 'fs';
import crypto from 'crypto';

if (isMainThread) {
	throw new Error('Worker script cannot be executed directly on the main thread.');
}

/**
 * Streams a file from disk and computes its cryptographic hash.
 * * @param {Object} params
 * @param {string} params.filePath - Must be an absolute path.
 * @param {string} params.algorithm - e.g., 'sha256', 'md5', 'sha512'
 * @returns {Promise<string>} The calculated hex digest.
 */

export async function computeHashDirect({ filePath, taskId, workerId, algorithm, }) {
	return new Promise((resolve, reject) => {
		let stream;
		try {
			const hash = crypto.createHash(algorithm);
			
			stream = fs.createReadStream(filePath, { highWaterMark: 64 * 1024 });

			stream.on('data', (chunk) => hash.update(chunk));
			stream.on('end', () => {
				
				const durationMs = parseFloat((performance.now() - startTime).toFixed(2));

				resolve({
				hash: hash.digest('hex'),
				durationMs,
				workerId,
				taskId
        		});
			})	
			
			stream.on('error', (err) => {
				if (stream) stream.destroy();

				if (err.code === 'EBUSY' || err.code === 'EPERM') {
					err.message = `File is locked or inaccessible by OS: ${err.message}`;
				}
				err.durationMs = parseFloat((performance.now() - startTime).toFixed(2));
				err.taskId;
				err.workerId;
				reject(err);
			});
		} catch (err) {
			if (stream) stream.destroy();
			err.durationMs = parseFloat((performance.now() - startTime).toFixed(2));
			err.taskId;
			err.workerId;
			reject(err);
		}
	});
}

if (!isMainThread) {
	const workerTask = async (data) => computeHashDirect(data);
	export default workerTask;
}