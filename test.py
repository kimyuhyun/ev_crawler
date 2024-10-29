from datetime import datetime
from bs4 import BeautifulSoup
import asyncio
from pyppeteer import launch


async def main():
    browser = await launch(
        headless=True,
        args=[
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-dev-shm-usage'
        ],
        ignoreHTTPSErrors=True
    )
    page = await browser.newPage()

    url = "https://ev.or.kr/nportal/buySupprt/initSubsidyPaymentCheckAction.do"
    await page.goto(url)
    content = await page.content()

    print(content)

    # 모든 작업이 끝난 후 브라우저 닫기
    await browser.close()

    # soup = BeautifulSoup(content, 'html.parser')
    # print(soup)
    # trs = soup.select("#editForm > div.contentList.fz13 > table > tbody > tr")
    # for tr in trs:
    # print(tr)


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())
