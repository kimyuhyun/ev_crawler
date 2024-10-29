import pymysql
from bs4 import BeautifulSoup
from datetime import datetime
from dotenv import load_dotenv
import os
import asyncio
from playwright.async_api import async_playwright

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

        parsing(content)


def parsing(content):
    soup = BeautifulSoup(content, 'html.parser')

    trs = soup.select("#editForm > div.contentList.fz13 > table > tbody > tr")

    today = datetime.today()
    formatted_today = today.strftime('%Y-%m-%d')

    getCodes()

    for tr in trs:
        obj = {}
        tds = tr.find_all('td')

        obj['ymd'] = formatted_today
        obj['region'] = tds[0].get_text(strip=True)
        obj['si'] = tds[1].get_text(strip=True)

        obj['code1'] = findCode1(obj['region'], obj['si'])

        obj['p_planned'] = getVehicleCount(tds[5], -4)
        obj['p_submitted'] = getVehicleCount(tds[6], -4)
        obj['p_deployed'] = getVehicleCount(tds[7], -4)
        obj['p_remaining'] = getVehicleCount(tds[8], -4)

        obj['c_planned'] = getVehicleCount(tds[5], -3)
        obj['c_submitted'] = getVehicleCount(tds[6], -3)
        obj['c_deployed'] = getVehicleCount(tds[7], -3)
        obj['c_remaining'] = getVehicleCount(tds[8], -3)

        obj['t_planned'] = getVehicleCount(tds[5], -2)
        obj['t_submitted'] = getVehicleCount(tds[6], -2)
        obj['t_deployed'] = getVehicleCount(tds[7], -2)
        obj['t_remaining'] = getVehicleCount(tds[8], -2)

        obj['n_planned'] = getVehicleCount(tds[5], -1)
        obj['n_submitted'] = getVehicleCount(tds[6], -1)
        obj['n_deployed'] = getVehicleCount(tds[7], -1)
        obj['n_remaining'] = getVehicleCount(tds[8], -1)

        now = datetime.now()
        str_now = now.strftime("%Y-%m-%d %H:%M:%S")
        obj['created'] = str_now

        # 데이터가 있는지 체크
        with conn.cursor() as cursor:
            sql = f"""
                SELECT 
                    count(*) as cnt 
                FROM REGION_SUBSIDI_AMOUNT_tbl 
                WHERE ymd = '{obj['ymd']}' 
                AND si = '{obj['si']}'
                AND region = '{obj['region']}'
            """
            cursor.execute(sql)
            rs = cursor.fetchone()

            query, values = createQuery(obj)

            if rs['cnt'] == 0:
                sql = f"INSERT INTO REGION_SUBSIDI_AMOUNT_tbl SET {query}"
                print(sql)
                cursor.execute(sql, values)
                conn.commit()
            else:
                sql = f"UPDATE REGION_SUBSIDI_AMOUNT_tbl SET {query} WHERE ymd = %s AND code1 = %s"
                values.append(obj['ymd'])
                values.append(obj['code1'])
                print(sql)
                cursor.execute(sql, values)
                conn.commit()

    cursor.close()


def getVehicleCount(td, seq):
    tmp = td.get_text(separator=' ', strip=True)
    tmp = tmp.split()[seq].strip('()')
    return tmp


def findCode1(region, si):
    global codes
    for entry in codes:
        if entry['region'] == region and entry['si'] == si:
            return entry['code1']
    return '없다'


def getCodes():
    global codes
    with conn.cursor() as cursor:
        sql = "SELECT * FROM REGION_CODE_tbl ORDER BY idx ASC"
        cursor.execute(sql)
        rows = cursor.fetchall()
        for row in rows:
            codes.append(row)


def createQuery(data):
    set_clause = ", ".join([f"{key} = %s" for key in data.keys()])
    values = list(data.values())
    return set_clause, values


if __name__ == "__main__":
    asyncio.run(main())
