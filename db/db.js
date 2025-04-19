import format from "pg-format";
import { pool, executeWithRetry } from "./connection.js";

export async function insertItems(config_code, items, job_id, source) {
  return executeWithRetry(async (client) => {
    try {
      await client.query("BEGIN");
      const batchSize = process.env.BATCH_SIZE || 100;
      const numBatches = Math.ceil(items.length / batchSize);
      const now = new Date();

      // Field validation functions
      const validateFields = (item) => {
        return {
          config_code: config_code,
          created: now,
          discount: item.discount || null,
          link: item.link ? item.link.substring(0, 255) : null,
          modified: now,
          oldprice: item.oldprice || null,
          price: item.priceraw,
          price_string: item.price ? item.price.substring(0, 60) : null,
          title: item.title ? item.title.substring(0, 255) : null,
          job_id: job_id,
        };
      };

      for (let i = 0; i < numBatches; i++) {
        const start = i * batchSize;
        const end = Math.min((i + 1) * batchSize, items.length);
        const batchItems = items.slice(start, end);

        const insertValues = batchItems.map((item) => {
          const validatedItem = validateFields(item);
          return [
            validatedItem.config_code,
            validatedItem.created,
            validatedItem.discount,
            validatedItem.link,
            validatedItem.modified,
            validatedItem.oldprice,
            validatedItem.price,
            validatedItem.price_string,
            validatedItem.title,
            validatedItem.job_id,
          ];
        });

        if (insertValues.length === 0) {
          console.log("No items to insert");
          continue;
        }

        console.log(
          "inserting data to db, start:",
          start,
          ", end:",
          end,
          ", full size:",
          items.length
        );
        await client.query(
          format(
            "INSERT INTO crawler_raw (config_code, created, discount, link, modified, oldprice, price, price_string, title, job_id) VALUES %L",
            insertValues
          )
        );
        console.log(
          "inserted data to db, start:",
          start,
          ", end:",
          end,
          ", full size:",
          items.length
        );
      }
      await client.query("COMMIT");
    } catch (err) {
      await client.query("ROLLBACK");

      // Log the error details for debugging
      console.error("Insert error details:", {
        error: err.message,
        code: err.code,
        detail: err.detail,
      });

      // Update job status to Failed
      await failJob(job_id);

      // Insert error into job_error
      await insertJobError(job_id, source, config_code, "CRAWL", err);

      throw err;
    }
  });
}

export async function insertJobError(jobId, source, category, jobType, err) {
  return executeWithRetry(async (client) => {
    try {
      await client.query("BEGIN");
      const errorDetails = [
        source,
        category,
        jobType,
        err.message,
        new Date(),
        jobId,
      ];
      console.log("Inserting error details into database:", errorDetails);
      await client.query(
        "INSERT INTO job_error (source, category, job_type, error, created, job_id) VALUES ($1, $2, $3, $4, $5, $6)",
        errorDetails
      );
      await client.query("COMMIT");
    } catch (err) {
      await client.query("ROLLBACK");
      console.error("Error inserting error details into database:", err);
      throw err;
    }
  });
}

export async function processCrawlJobs() {
  return executeWithRetry(async (client) => {
    try {
      // Only look for CRAWL type jobs
      // Only exclude websites that are currently running (1 job per domain)
      const awaitingJobsQuery = await client.query(
        `SELECT * FROM job
                 WHERE status = $1 
                 AND job_type = 'CRAWL'
                 AND NOT EXISTS (
                     SELECT 1 
                     FROM job running
                     WHERE running.website_code = job.website_code 
                     AND running.status = $2
                     AND running.job_type = 'CRAWL'
                 )
                 ORDER BY id ASC 
                 LIMIT 1`,
        ["Created", "Running"]
      );

      if (awaitingJobsQuery.rowCount > 0) {
        const job = awaitingJobsQuery.rows[0];
        console.log(`Processing CRAWL job:`, job);

        const crawlerConfigQuery = await client.query(
          "SELECT * FROM crawler_config WHERE code = $1",
          [job.config_code]
        );

        if (crawlerConfigQuery.rowCount > 0) {
          const crawlerConfig = crawlerConfigQuery.rows[0];
          crawlerConfig.job_id = job.id;
          crawlerConfig.test_run = job.test_run;
          crawlerConfig.website_code = job.website_code;
          crawlerConfig.job_type = job.job_type;
          return crawlerConfig;
        }
      }
      return null;
    } catch (err) {
      console.error(`Error processing CRAWL jobs:`, err);
      return null;
    }
  });
}

export async function startJob(job) {
  return executeWithRetry(async (client) => {
    await client.query(
      `UPDATE job SET status = $1, started_at = NOW(), modified = NOW(), modified_by = $3 WHERE id = $2`,
      ["Running", job.job_id || job.id, "SYSTEM"]
    );
    job.status = "Running";
    return job;
  });
}

export async function finishJob(id) {
  return executeWithRetry(async (client) => {
    await client.query(
      "UPDATE job SET status = $1, finished_at = NOW(), modified = NOW(), modified_by = $3 WHERE id = $2",
      ["Finished", id, "SYSTEM"]
    );
  });
}

export async function failJob(id) {
  return executeWithRetry(async (client) => {
    await client.query(
      "UPDATE job SET status = $1, finished_at = NOW(), modified = NOW(), modified_by = $3 WHERE id = $2",
      ["Failed", id, "SYSTEM"]
    );
  });
}

// Aliases for backward compatibility
export const startCrawlJob = startJob;
export const finishCrawlJob = finishJob;
export const failCrawlJob = failJob;
export const insertCrawlError = (jobId, website, category, err) =>
  insertJobError(jobId, website, category, "CRAWL", err);
