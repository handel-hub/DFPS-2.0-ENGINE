import pkg from "pg";
import dotenv from "dotenv";
import path from "path";
import { fileURLToPath } from "url";

const { Pool } = pkg;


const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);


dotenv.config({ path: path.resolve(__dirname, "../../.env") });

if (!process.env.DATABASE_URL) {
    throw new Error("DATABASE_URL is not defined in .env");
}

export const pool = new Pool({
    connectionString: process.env.DATABASE_URL
});

export async function fetchPendingJobs(coordinatorId, limit = 10) {
    const { rows } = await pool.query(`
        WITH next_jobs AS (
            SELECT j.id
            FROM jobs j
            JOIN job_retries r ON r.job_id = j.id
            WHERE
                (
                    j.status = 0  
                )
                OR
                (
                    j.status = 1  
                    AND j.lease_expires_at < now()
                    AND (r.retry_count IS NULL OR r.retry_count < r.max_retries)
                )
            ORDER BY j.priority DESC, j.id
            FOR UPDATE SKIP LOCKED
            LIMIT $2
        ),
        claimed_jobs AS (
            UPDATE jobs j
            SET status = 1, 
                assigned_coordinator = $1,
                lease_expires_at = now() + interval '30 seconds',
                last_updated = now(),
                version = j.version + 1
            FROM next_jobs nj
            WHERE j.id = nj.id
            RETURNING j.id,
                    j.status,
                    j.priority,
                    j.assigned_coordinator,
                    j.assigned_worker,
                    j.file_id
        )
        SELECT
            cj.id,
            cj.status,
            cj.priority,
            cj.assigned_coordinator,
            cj.assigned_worker,

            f.filepath,
            f.size_bytes,

            r.retry_count,
            r.max_retries,
            r.last_attempt,

            COALESCE(
                json_agg(
                    json_build_object(
                        'id', jt.id,
                        'type', jt.transformation_type,
                        'status', jt.status
                    )
                ) FILTER (WHERE jt.id IS NOT NULL),
                '[]'
            ) AS transformations

        FROM claimed_jobs cj
        JOIN files f ON f.id = cj.file_id
        LEFT JOIN job_retries r ON r.job_id = cj.id
        LEFT JOIN job_transformations jt ON jt.job_id = cj.id
        GROUP BY
            cj.id,
            cj.status,
            cj.priority,
            cj.assigned_coordinator,
            cj.assigned_worker,
            cj.file_id,
            f.filepath,
            f.size_bytes,
            r.retry_count,
            r.max_retries,
            r.last_attempt;
    `, [coordinatorId, limit]);

    return rows;
}

export async function acknowledgeJobs(jobIds,coordinatorId) {
    await pool.query(`
        UPDATE jobs
        SET status = 2,
            acknowledge_at = now(),
            last_updated = now(),
            version = version + 1
        WHERE id = ANY($1)
        AND status = 1
        AND assigned_coordinator = $2
        `,[jobIds,coordinatorId])
}

export async function getCoordinatorMetrics() {
    const { rows } = await pool.query(`
        SELECT 
            coordinator_id,
            cpu_ema,
            memory_ema,
            queue_len_ema,
            success_count,
            error_count,
            avg_job_time,
            throughput,
            network_latency,
            alive,
            last_heartbeat,
            updated_at
        FROM coordinator_metrics;
    `);
    return rows;
}
