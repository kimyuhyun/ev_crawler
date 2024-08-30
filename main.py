import pymysql
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from dotenv import load_dotenv
import os

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


def main():
    global codes
    headers = {
        "Content-Type": "application/json;charset=UTF-8",
        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 13_2_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.3 Mobile/15E148 Safari/604.1",
    }
    url = "https://ev.or.kr/nportal/buySupprt/initSubsidyPaymentCheckAction.do"
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')
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

        # print(obj)

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
    main()
