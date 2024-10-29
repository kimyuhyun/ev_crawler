import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from datetime import datetime


async def scrape_page():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=[
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-dev-shm-usage',
            '--disable-gpu',
            '--disable-software-rasterizer',
            '--disable-extensions',
            '--window-size=1280,720',
            '--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        ])

        # 여기서는 async with가 아닌, 일반 await 구문으로 페이지를 생성합니다.
        page = await browser.new_page()

        # 요청 차단을 위한 함수 설정 (이미지, 스타일시트, 폰트 등)
        async def intercept(route, request):
            if request.resource_type in ['image', 'stylesheet', 'font']:
                await route.abort()
            else:
                await route.continue_()

        await page.route("**/*", intercept)

        url = "https://ev.or.kr/nportal/buySupprt/initSubsidyPaymentCheckAction.do"
        print(f"Loading page: {url}")

        await page.goto(url, wait_until='networkidle', timeout=30000)

        content = await page.content()
        soup = BeautifulSoup(content, 'html.parser')
        print(soup)

        # 브라우저 종료
        await browser.close()

    return True


async def main():
    success = await scrape_page()
    if success:
        print("Scraping completed successfully")
    else:
        print("Scraping failed")

if __name__ == "__main__":
    asyncio.run(main())
