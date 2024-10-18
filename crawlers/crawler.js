import puppeteer from 'puppeteer';
import { setTimeout } from "node:timers/promises";
import dotenv from 'dotenv';
import { failCrawlJob, insertCrawlError } from '../db.js';

export async function crawlWebsite(crawlConfig) {
  dotenv.config();
  const browser = await puppeteer.launch({ headless: true });
  const page = await browser.newPage();
  // page.on('console', msg => console.log('PAGE LOG:', msg.text()));

  let currentPage = 1;
  let hasNextPage = true;
  const allItems = [];
  const pageLimit = crawlConfig.max_pages === undefined ? Number(process.env.PAGE_LIMIT) || 20 : crawlConfig.max_pages;

  try {
    while (currentPage <= pageLimit && hasNextPage) {
      let currentPageUrl = `${crawlConfig.start_url}`;
      // always at start go to first page
      if (currentPage == 1) {
        await page.goto(currentPageUrl, { waitUntil: 'domcontentloaded', timeout: 30000 });
      } // otherwise only if not clicking to next page
      else if (!crawlConfig.use_next_page_button) {
        currentPageUrl += `?page=${currentPage}`;
        await page.goto(currentPageUrl, { waitUntil: 'domcontentloaded', timeout: 30000 });
      }

      const itemsOnPage = await page.evaluate((crawlConfig, currentPage) => {
        const items = [];
        const productElements = document.querySelectorAll(crawlConfig.all_items_sel);
        console.log('productElements', JSON.stringify(productElements));
        if (currentPage == 1 && (productElements == null || productElements.length == 0)) {
          throw new Error('All items selector has not found any items on 1st page');
        }
        for (var i = 0; i < productElements.length; i++) {
          var element = productElements[i];
          const title = element.querySelector(crawlConfig.title_sel)?.textContent?.trim();
          if (title === null || title === undefined || title === '') {
            throw new Error(`Title not found for item: ${i + 1} on page ${currentPage}`);
          }
          const link = element.querySelector(crawlConfig.link_sel)?.href;
          if (link === null || link === undefined || link === '') {
            throw new Error(`Link not found for item: ${i + 1} on page ${currentPage}`);
          }
          const price = element.querySelector(crawlConfig.price_sel)?.textContent?.trim();

          if (price === null || price === undefined || price === '') {
            throw new Error(`Price not found for item: ${i + 1} on page ${currentPage}`);
          }
          const priceraw = cleanAndParsePrice(price);

          if (priceraw === null || priceraw === undefined || priceraw === '' || priceraw === 0) {
            throw new Error(`Priceraw not parsed for item: ${i + 1} on page ${currentPage}, price string: ${price}`);
          }
          items.push({ title, link, price, priceraw });
        }

        function cleanAndParsePrice(priceString) {
          // Remove non-numeric characters except dots and commas
          const cleanedPriceString = priceString.replace(/[^\d.,]/g, '');
          const priceWithDots = cleanedPriceString.replaceAll(',', '.');
          const priceWithDot = removeDotsExceptLast(priceWithDots);
          const price = parseFloat(priceWithDot);
          if (isNaN(price)) {
            return null;
          }
          return price;
        }

        function removeDotsExceptLast(str) {
          const lastDotIndex = str.lastIndexOf('.');
          if (lastDotIndex === -1) return str;
          // Remove all dots before the last one
          const withoutDots = str.slice(0, lastDotIndex).replace(/\./g, '');
          // Combine the part without dots and the part from the last dot onwards
          return withoutDots + str.slice(lastDotIndex);
        }

        return items;
      }, crawlConfig, currentPage);

      if (itemsOnPage.length === 0) {
        hasNextPage = false;
      } else {
        allItems.push(...itemsOnPage);
        currentPage++;
        if (crawlConfig.use_next_page_button) {
          //update current page for logging purposes
          currentPageUrl = page.url();
          const nextPageButton = await page.$(crawlConfig.next_page_button_sel);
          if (!nextPageButton) {
            hasNextPage = false;
          } else {
            await nextPageButton.click();
          }
        }
        // Add a random pause between 3000 and 6000 milliseconds between page loads
        const randomTimeout = Math.floor(Math.random() * (6000 - 3000 + 1)) + 3000;
        console.log('Crawling page ' + currentPageUrl);
        console.log('Crawled ' + itemsOnPage.length + ' items');
        console.log('Sleeping for ' + randomTimeout + ' ms');
        await setTimeout(randomTimeout);
      }
    }
    return allItems;
  } catch (err) {
    console.error('Error during crawling:', err);
    await failCrawlJob(crawlConfig.job_id);
    await insertCrawlError(crawlConfig.job_id, crawlConfig.start_url, crawlConfig.category_code, err)
    throw err;
  } finally {
    await page.close();
    await browser.close();
  }
}
