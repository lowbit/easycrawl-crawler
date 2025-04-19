import { crawlWebsite } from "./crawlers/crawler.js";
import {
  finishJob,
  insertItems,
  processCrawlJobs,
  startJob,
  failJob,
  insertJobError,
} from "./db/db.js";
import { testDatabaseConnection } from "./db/connection.js";
import dotenv from "dotenv";

// Load environment variables
dotenv.config();

// Track running jobs by website
const runningWebsites = new Set();
const CHECK_INTERVAL = parseInt(process.env.CHECK_INTERVAL || "5000");
const MAX_CONCURRENT_JOBS = parseInt(process.env.MAX_CONCURRENT_JOBS || 5);

// Process crawler job
async function processCrawlerJob(jobConfig) {
  try {
    console.log(
      `Starting crawler job ${jobConfig.job_id} for website ${jobConfig.website_code}`
    );
    runningWebsites.add(jobConfig.website_code);

    if (jobConfig.test_run) {
      jobConfig.max_pages = 2;
    }

    const items = await crawlWebsite(jobConfig);
    if (items != null && items.length > 0) {
      await insertItems(
        jobConfig.code,
        items,
        jobConfig.job_id,
        jobConfig.website_code
      );
      await finishJob(jobConfig.job_id);
      console.log(`Successfully completed crawler job ${jobConfig.job_id}`);
    }
  } catch (error) {
    console.error(`Error processing crawler job ${jobConfig.job_id}:`, error);
    await failJob(jobConfig.job_id);
  } finally {
    runningWebsites.delete(jobConfig.website_code);
  }
}

async function checkAndProcessJobs() {
  try {
    if (runningWebsites.size < MAX_CONCURRENT_JOBS) {
      // Only get CRAWL jobs
      const crawlerJob = await processCrawlJobs();
      if (crawlerJob) {
        const startedJob = await startJob(crawlerJob);
        processCrawlerJob(startedJob);
      }
    }
  } catch (error) {
    console.error("Error in checkAndProcessJobs:", error);
    // If there's a database error, wait a bit longer before the next check
    await new Promise((resolve) => setTimeout(resolve, 5000));
  }
}

// Main loop with connection verification
async function main() {
  console.log(`Starting crawler with ${CHECK_INTERVAL}ms check interval`);
  console.log(`Maximum concurrent jobs: ${MAX_CONCURRENT_JOBS}`);

  // Initial database connection test
  const isConnected = await testDatabaseConnection();
  if (!isConnected) {
    console.error(
      "Failed to establish initial database connection. Exiting..."
    );
    process.exit(1);
  }

  while (true) {
    try {
      await checkAndProcessJobs();
      await new Promise((resolve) => setTimeout(resolve, CHECK_INTERVAL));
    } catch (error) {
      console.error("Error in main loop:", error);
      // Wait a bit longer if there's an error
      await new Promise((resolve) => setTimeout(resolve, CHECK_INTERVAL * 2));
    }
  }
}

// Handle process termination
process.on("SIGINT", async () => {
  console.log("Gracefully shutting down...");
  // Wait for any running jobs to complete
  if (runningWebsites.size > 0) {
    console.log("Waiting for running jobs to complete...");
    await new Promise((resolve) => setTimeout(resolve, 5000));
  }
  process.exit(0);
});

// Start the application
main().catch((error) => {
  console.error("Fatal error:", error);
  process.exit(1);
});
