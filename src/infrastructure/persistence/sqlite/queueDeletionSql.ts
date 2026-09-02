export const DELETE_QUEUE_JOB_RESULTS_FROM_JOBS_SQL = `DELETE FROM job_results
  WHERE job_id IN (
    SELECT target.id FROM jobs AS target
    WHERE target.queue = ?
      AND NOT EXISTS (
        SELECT 1 FROM dlq AS other_dlq
        WHERE other_dlq.job_id = target.id AND other_dlq.queue <> ?
      )
  )`;

export const DELETE_QUEUE_JOB_RESULTS_FROM_DLQ_SQL = `DELETE FROM job_results
  WHERE job_id IN (
    SELECT dlq.job_id FROM dlq
    WHERE dlq.queue = ?
      AND NOT EXISTS (
        SELECT 1 FROM jobs
        WHERE jobs.id = dlq.job_id AND jobs.queue <> ?
      )
      AND NOT EXISTS (
        SELECT 1 FROM dlq AS other_dlq
        WHERE other_dlq.job_id = dlq.job_id AND other_dlq.queue <> ?
      )
  )`;

export const DELETE_QUEUE_FLOW_FAILURES_SQL = `DELETE FROM flow_failures
  WHERE child_queue = ?
     OR parent_id IN (
       SELECT target.id FROM jobs AS target
       WHERE target.queue = ?
         AND NOT EXISTS (
           SELECT 1 FROM dlq AS other_dlq
           WHERE other_dlq.job_id = target.id AND other_dlq.queue <> ?
         )
     )
     OR child_id IN (
       SELECT target.id FROM jobs AS target
       WHERE target.queue = ?
         AND NOT EXISTS (
           SELECT 1 FROM dlq AS other_dlq
           WHERE other_dlq.job_id = target.id AND other_dlq.queue <> ?
         )
     )
     OR EXISTS (
       SELECT 1 FROM dlq
       WHERE dlq.queue = ?
         AND (dlq.job_id = flow_failures.parent_id
              OR dlq.job_id = flow_failures.child_id)
         AND NOT EXISTS (
           SELECT 1 FROM jobs
           WHERE jobs.id = dlq.job_id AND jobs.queue <> ?
         )
         AND NOT EXISTS (
           SELECT 1 FROM dlq AS other_dlq
           WHERE other_dlq.job_id = dlq.job_id AND other_dlq.queue <> ?
         )
     )`;
