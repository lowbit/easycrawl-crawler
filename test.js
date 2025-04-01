import { crawlWebsite } from './crawlers/crawler.js';
import { processCrawlJobs } from './db/db.js';

(async () => {
    const websiteCrawlerConfig = await processCrawlJobs();
    if(websiteCrawlerConfig != null){
        const items = await crawlWebsite(websiteCrawlerConfig);
        items.forEach(item => {
            console.log(
                item.title, '|', item.priceraw, '|', item.price, '|', item.link
            );
        });
    }
})();
