import puppeteer from "puppeteer";
import { setTimeout } from "node:timers/promises";
import dotenv from "dotenv";
import { failCrawlJob, insertCrawlError } from "../db/db.js";

export async function crawlWebsite(crawlConfig) {
  dotenv.config();
  const browser = await puppeteer.launch({ headless: true });
  const page = await browser.newPage();
  // page.on('console', msg => console.log('PAGE LOG:', msg.text()));

  let currentPage = 1;
  let hasNextPage = true;
  const allItems = [];
  const pageLimit =
    crawlConfig.max_pages === undefined
      ? Number(process.env.PAGE_LIMIT) || 20
      : crawlConfig.max_pages;

  try {
    // Initial page load
    await page.goto(crawlConfig.start_url, {
      waitUntil: "domcontentloaded",
      timeout: 30000,
    });

    while (currentPage <= pageLimit && hasNextPage) {
      let currentPageUrl = page.url();

      if (crawlConfig.infinite_scroll) {
        // For infinite scroll, we'll scroll and wait for new content
        const previousHeight = await page.evaluate(
          "document.documentElement.scrollHeight"
        );
        await page.evaluate(
          "window.scrollTo(0, document.documentElement.scrollHeight)"
        );

        // Wait for potential new content to load
        try {
          await page.waitForFunction(
            `document.documentElement.scrollHeight > ${previousHeight}`,
            { timeout: 5000 }
          );
          // Additional wait for content to render
          await setTimeout(1000);
        } catch (e) {
          // If no height change after scroll, assume we've reached the end
          hasNextPage = false;
        }
      } else if (crawlConfig.use_next_page_button) {
        // Existing next page button logic
        const nextPageButton = await page.$(crawlConfig.next_page_button_sel);
        if (!nextPageButton) {
          hasNextPage = false;
        } else {
          await nextPageButton.click();
          await page.waitForNavigation({
            waitUntil: "domcontentloaded",
            timeout: 30000,
          });
        }
      } else {
        // Existing URL-based pagination logic
        currentPageUrl = `${crawlConfig.start_url}?page=${currentPage + 1}`;
        await page.goto(currentPageUrl, {
          waitUntil: "domcontentloaded",
          timeout: 30000,
        });
      }

      // Get all items currently on the page
      const itemsOnPage = await page.evaluate(
        (crawlConfig, currentPage) => {
          const items = [];
          const productElements = document.querySelectorAll(
            crawlConfig.all_items_sel
          );

          if (
            currentPage == 1 &&
            (productElements == null || productElements.length == 0)
          ) {
            throw new Error(
              "All items selector has not found any items on 1st page"
            );
          }

          for (var i = 0; i < productElements.length; i++) {
            var element = productElements[i];
            const title = element
              .querySelector(crawlConfig.title_sel)
              ?.textContent?.trim();
            if (title === null || title === undefined || title === "") {
              throw new Error(
                `Title not found for item: ${i + 1} on page ${currentPage}`
              );
            }

            const link = element.querySelector(crawlConfig.link_sel)?.href;
            if (link === null || link === undefined || link === "") {
              throw new Error(
                `Link not found for item: ${i + 1} on page ${currentPage}`
              );
            }

            const price = element
              .querySelector(crawlConfig.price_sel)
              ?.textContent?.trim();
            if (price === null || price === undefined || price === "") {
              throw new Error(
                `Price not found for item: ${i + 1} on page ${currentPage}`
              );
            }

            const priceraw = cleanAndParsePrice(price);
            if (
              priceraw === null ||
              priceraw === undefined ||
              priceraw === "" ||
              priceraw === 0
            ) {
              throw new Error(
                `Priceraw not parsed for item: ${
                  i + 1
                } on page ${currentPage}, price string: ${price}`
              );
            }

            // Only add items we haven't seen before (checking by link)
            if (!items.some((item) => item.link === link)) {
              items.push({ title, link, price, priceraw });
            }
          }

          function cleanAndParsePrice(priceString) {
            const cleanedPriceString = priceString.replace(/[^\d.,]/g, "");
            const priceWithDots = cleanedPriceString.replaceAll(",", ".");
            const priceWithDot = removeDotsExceptLast(priceWithDots);
            const price = parseFloat(priceWithDot);
            return isNaN(price) ? null : price;
          }

          function removeDotsExceptLast(str) {
            const lastDotIndex = str.lastIndexOf(".");
            if (lastDotIndex === -1) return str;
            const withoutDots = str.slice(0, lastDotIndex).replace(/\./g, "");
            return withoutDots + str.slice(lastDotIndex);
          }

          return items;
        },
        crawlConfig,
        currentPage
      );

      // Check if we found any new items
      const newItems = itemsOnPage.filter(
        (newItem) =>
          !allItems.some((existingItem) => existingItem.link === newItem.link)
      );

      if (newItems.length === 0) {
        hasNextPage = false;
      } else {
        allItems.push(...newItems);
        currentPage++;

        // Add a random pause between page loads
        const randomTimeout =
          Math.floor(Math.random() * (6000 - 3000 + 1)) + 3000;
        console.log("Crawling page " + currentPageUrl);
        console.log("Crawled " + newItems.length + " new items");
        console.log("Total items so far: " + allItems.length);
        console.log("Sleeping for " + randomTimeout + " ms");
        await setTimeout(randomTimeout);
      }
    }

    return allItems;
  } catch (err) {
    console.error("Error during crawling:", err);
    await failCrawlJob(crawlConfig.job_id);
    await insertCrawlError(
      crawlConfig.job_id,
      crawlConfig.start_url,
      crawlConfig.category_code,
      err
    );
    throw err;
  } finally {
    await page.close();
    await browser.close();
  }
}
