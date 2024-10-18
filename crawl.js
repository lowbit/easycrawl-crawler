import { crawlWebsite } from './crawlers/crawler.js';
import { finishCrawlJob, insertItems, processCrawlJobs } from './db.js';

(async () => {
  const websiteCrawlerConfig = await processCrawlJobs();
  if(websiteCrawlerConfig != null){
      const items = await crawlWebsite(websiteCrawlerConfig);
      if(items!=null && items.length>0){
        await insertItems(websiteCrawlerConfig.code, items);
        await finishCrawlJob(websiteCrawlerConfig.job_id);
      }
  } else {
    console.log('Did not find website crawler config');
  }
})();