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

      if (crawlConfig.use_infinite_scroll) {
        console.log("Using infinite scroll...");

        // Store the current item count to check if new items were loaded
        const currentItemCount = allItems.length;

        // For infinite scroll, scroll down and wait longer for content to load
        const previousHeight = await page.evaluate(
          "document.documentElement.scrollHeight"
        );

        // Scroll to bottom
        await page.evaluate(
          "window.scrollTo(0, document.documentElement.scrollHeight)"
        );

        // Wait a bit longer for the content to load (increased from 5000ms to 8000ms)
        try {
          // First try to detect by height change
          await page.waitForFunction(
            `document.documentElement.scrollHeight > ${previousHeight}`,
            { timeout: 8000 }
          );

          // Additional wait for content to render fully
          await setTimeout(2000);
        } catch (e) {
          console.log(
            "No immediate height change detected, checking for new items anyway..."
          );

          // Even if no height change, check if new items appeared
          const checkForNewItems = await page.evaluate((crawlConfig) => {
            const currentItems = document.querySelectorAll(
              crawlConfig.all_items_sel
            ).length;
            return currentItems;
          }, crawlConfig);

          // If we don't have new items after scrolling, we've likely reached the end
          if (checkForNewItems <= itemsOnPage.length) {
            console.log("No new items found after scrolling, ending crawl");
            hasNextPage = false;
          } else {
            // There are new items, keep going
            console.log(
              `Found ${
                checkForNewItems - itemsOnPage.length
              } more items after scrolling`
            );
            // Give extra time for rendering
            await setTimeout(2000);
          }
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
      } else if (crawlConfig.use_url_page_parameter) {
        // Existing URL-based pagination logic
        currentPageUrl = `${crawlConfig.start_url}${crawlConfig.url_page_parameter}${currentPage}`;
        await page.goto(currentPageUrl, {
          waitUntil: "domcontentloaded",
          timeout: 30000,
        });
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
