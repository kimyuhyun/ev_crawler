from datetime import datetime
import asyncio
from bs4 import BeautifulSoup
from pyppeteer import launch


async def main():
    # headless=False로 설정하여 브라우저 창을 실제로 띄움
    browser = await launch(
        headless=False,
        args=[
            '--window-size=375,812',
            '--no-sandbox',  # sandbox 비활성화
            '--disable-setuid-sandbox',  # setuid sandbox 비활성화
            '--disable-dev-shm-usage'  # shared memory 사용 비활성화
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
