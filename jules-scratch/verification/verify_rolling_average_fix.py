import asyncio
from playwright.async_api import async_playwright

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()
        await page.goto("http://localhost:8000")

        # --- Test Case 1: Enable Left Rolling Average ---
        await page.click('.rolling-avg-toggle-btn[data-axis="left"]')
        await asyncio.sleep(1) # Wait for chart to re-render
        await page.screenshot(path="jules-scratch/verification/rolling_avg_left_on.png")

        # --- Test Case 2: Enable Right Rolling Average ---
        await page.click('.rolling-avg-toggle-btn[data-axis="right"]')
        await asyncio.sleep(1)
        await page.screenshot(path="jules-scratch/verification/rolling_avg_both_on.png")

        # --- Test Case 3: Enable Histogram ---
        await page.click('.histogram-toggle-btn')
        await asyncio.sleep(1)
        await page.screenshot(path="jules-scratch/verification/histogram_on_disables_right_avg.png")

        # --- Test Case 4: Enable Left Rolling Average while Histogram is On ---
        await page.click('.rolling-avg-toggle-btn[data-axis="left"]')
        await asyncio.sleep(1)
        await page.screenshot(path="jules-scratch/verification/left_avg_on_disables_histogram.png")

        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
