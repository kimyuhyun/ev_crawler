import pymysql
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import os
from datetime import datetime
import asyncio
from pyppeteer import launch
from urllib.parse import urlencode  # 이 import 추가

load_dotenv()

# 환경 변수 가져오기
db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')


conn = pymysql.connect(
    host=db_host,  # 데이터베이스 서버
    user=db_user,  # 사용자명
    password=db_password,  # 비밀번호
    db=db_name,  # 데이터베이스 이름
    charset='utf8mb4',  # 문자셋 설정
    cursorclass=pymysql.cursors.DictCursor
)

codes = []


async def main():
    global codes
    getCodes()

    with conn.cursor() as cursor:
        # 기존데이터 삭제!
        sql = f"DELETE FROM EV_SUBSIDI_AMOUNT_tbl"
        cursor.execute(sql)
        conn.commit()

    # 브라우저 한 번만 실행
    browser = await launch(
        headless=True,
        args=[
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-dev-shm-usage'
        ],
        ignoreHTTPSErrors=True
    )

    query = ""

    try:
        for index, obj in enumerate(codes):
            # 배열 합치기!
            list = await crawling(browser, 2024, obj['code1'])
            arr = [tuple(item.values()) for item in list]

            if query == "":
                query, v = createQuery(list[0])

            with conn.cursor() as cursor:
                # 한방에 입력!
                print(index, '/', len(codes))
                print('총 자동차 갯수:', len(arr))
                sql = f"INSERT INTO EV_SUBSIDI_AMOUNT_tbl SET {query}"
                cursor.executemany(sql, arr)
                conn.commit()
                cursor.close()

            # 선택적: 요청 간 딜레이 추가
            await asyncio.sleep(1)
    finally:
        # 모든 작업이 끝난 후 브라우저 종료
        await browser.close()


async def crawling(browser, year, code1):
    print(code1)
    # headers = {
    #     'Content-Type': 'application/x-www-form-urlencoded'
    # }
    # url = "https://ev.or.kr/nportal/buySupprt/psPopupLocalCarModelPrice.do"
    # payload = f"year={year}&local_cd={code1}&car_type=11"
    # response = requests.request('POST', url, headers=headers, data=payload)
    # soup = BeautifulSoup(response.text, 'html.parser')

    url = "https://ev.or.kr/nportal/buySupprt/psPopupLocalCarModelPrice.do"
    page = await browser.newPage()

    # Form HTML 생성
    form_html = f'''
        <form id="postForm" action="{url}" method="POST">
            <input type="hidden" name="year" value="{year}">
            <input type="hidden" name="local_cd" value="{code1}">
            <input type="hidden" name="car_type" value="11">
        </form>
        <script>
            document.getElementById('postForm').submit();
        </script>
    '''

    await page.setContent(form_html)
    await page.waitForNavigation()

    content = await page.content()

    # 페이지만 닫고 브라우저는 유지
    await page.close()

    soup = BeautifulSoup(content, 'html.parser')
    trs = soup.select("tbody > tr")
    arr = []
    for tr in trs:
        obj = {}
        tds = tr.find_all('td')

        obj['code1'] = code1
        obj['car_type'] = tds[0].get_text(strip=True)
        obj['car_maker'] = tds[1].get_text(strip=True)
        obj['car_model'] = tds[2].get_text(strip=True)
        obj['central_amount'] = tds[3].get_text(strip=True)
        obj['local_amount'] = tds[4].get_text(strip=True)
        obj['total_amount'] = tds[5].get_text(strip=True)

        now = datetime.now()
        str_now = now.strftime("%Y-%m-%d %H:%M:%S")
        obj['created'] = str_now

        arr.append(obj)

    return arr


def createQuery(data):
    set_clause = ", ".join([f"{key} = %s" for key in data.keys()])
    values = list(data.values())
    return set_clause, values


def getCodes():
    global codes
    with conn.cursor() as cursor:
        sql = "SELECT * FROM REGION_CODE_tbl ORDER BY idx ASC"
        cursor.execute(sql)
        rows = cursor.fetchall()
        for row in rows:
            codes.append(row)


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
