import asyncio
from playwright.async_api import async_playwright
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def scrape_page():
    try:
        async with async_playwright() as p:
            browser = await p.webkit.launch(
                headless=True,
                args=['--no-startup-window']
            )

            context = await browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                viewport={'width': 1280, 'height': 720}
            )

            page = await context.new_page()

            url = "https://ev.or.kr/nportal/buySupprt/initSubsidyPaymentCheckAction.do"
            logger.info(f"Start loading page: {url}")

            await page.goto(url)
            await page.wait_for_load_state('networkidle')
            await asyncio.sleep(2)

            content = await page.content()
            logger.info("Successfully retrieved page content")

            await browser.close()

            return content

    except Exception as e:
        logger.error(f"Error occurred: {str(e)}")
        raise


async def main():
    try:
        content = await scrape_page()
        print(content)

    except Exception as e:
        logger.error(f"Main function error: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Program interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error occurred: {str(e)}")
