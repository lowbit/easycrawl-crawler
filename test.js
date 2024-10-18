import { crawlWebsite } from './crawlers/crawler.js';
import { processCrawlJobs, finishCrawlJob } from './db.js';

(async () => {
    const websiteCrawlerConfig = await processCrawlJobs();
    if(websiteCrawlerConfig != null){
        const items = await crawlWebsite(websiteCrawlerConfig);
        //await insertItems(websiteCrawlerConfig.code, items);
        //await finishCrawlJob(websiteCrawlerConfig.job_id);
        items.forEach(item => {
            console.log(
                item.title, '|', item.priceraw, '|', item.price, '|', item.link
            );
        });
    }
})();
