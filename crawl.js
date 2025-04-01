import { crawlWebsite } from "./crawlers/crawler.js";
import {
  finishCrawlJob,
  insertItems,
  processCrawlJobs,
  startCrawlJob,
  failCrawlJob,
  insertCrawlError,
} from "./db/db.js";
import { testDatabaseConnection } from "./db/connection.js";
import dotenv from "dotenv";

// Load environment variables
dotenv.config();

// Track running jobs by website
const runningWebsites = new Set();
const CHECK_INTERVAL = parseInt(process.env.CHECK_INTERVAL || "5000");
const MAX_CONCURRENT_JOBS = parseInt(process.env.MAX_CONCURRENT_JOBS || 5);

async function processJob(crawlerConfig) {
  try {
    console.log(
      `Starting job ${crawlerConfig.job_id} for website ${crawlerConfig.website_code}`
    );
    runningWebsites.add(crawlerConfig.website_code);

    if (crawlerConfig.test_run) {
      crawlerConfig.max_pages = 2;
    }

    const items = await crawlWebsite(crawlerConfig);
    if (items != null && items.length > 0) {
      await insertItems(
        crawlerConfig.code,
        items,
        crawlerConfig.job_id,
        crawlerConfig.website_code
      );
      await finishCrawlJob(crawlerConfig.job_id);
    }
  } catch (error) {
    console.error(`Error processing job ${crawlerConfig.job_id}:`, error);
    await failCrawlJob(crawlerConfig.job_id);
  } finally {
    runningWebsites.delete(crawlerConfig.website_code);
  }
}

async function checkAndProcessJobs() {
  try {
    if (runningWebsites.size < MAX_CONCURRENT_JOBS) {
      const availableJobs = [];
      let websiteCrawlerConfig = await processCrawlJobs();
      if (websiteCrawlerConfig !=null) {
        websiteCrawlerConfig = await startCrawlJob(websiteCrawlerConfig);
        availableJobs.push(websiteCrawlerConfig);
        availableJobs.map((config) => processJob(config));
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
