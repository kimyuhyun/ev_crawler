from datetime import datetime
import asyncio
from bs4 import BeautifulSoup
from pyppeteer import launch


async def main():
    # headless=False로 설정하여 브라우저 창을 실제로 띄움
    browser = await launch(headless=False, args=['--window-size=375,812'])
    page = await browser.newPage()

    # iPhone X 프로파일로 에뮬레이션 설정
    await page.emulate({
        'name': 'iPhone X',
        'viewport': {
            'width': 375,
            'height': 812,
            'deviceScaleFactor': 3,
            'isMobile': True,
            'hasTouch': True,
        },
        'userAgent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 11_0 like Mac OS X) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/11.0 Mobile/15A372 Safari/604.1'
    })

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


if __name__ == "__main__":
    asyncio.run(main())
