import pymysql
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import os
import re

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


def main():
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    url = "https://ev.or.kr/nportal/buySupprt/psPopupLocalCarPirce.do"
    payload = 'car_type=11&carType=car&evCarTypeDtl=11&year1=2024'
    response = requests.request('POST', url, headers=headers, data=payload)
    soup = BeautifulSoup(response.text, 'html.parser')
    trs = soup.select("tbody > tr")

    for tr in trs:
        obj = {}
        tds = tr.find_all('td')

        obj['region'] = tds[0].get_text(strip=True)
        obj['si'] = tds[1].get_text(strip=True)

        # 세 번째 <td> 요소의 <a> 태그에서 onclick 속성의 값 추출
        a_tag = tds[2].find('a', class_='btnDown')
        if a_tag and a_tag.has_attr('onclick'):
            onclick_value = a_tag['onclick']
            # 정규표현식을 사용하여 '5000' 추출
            match = re.search(r"psPopupLocalCarModelPrice\('2024','(\d+)'", onclick_value)
            if match:
                obj['code1'] = match.group(1)  # '5000'이 저장됩니다.
            else:
                obj['code1'] = ''

        print(obj)

        # 데이터가 있는지 체크
        with conn.cursor() as cursor:
            # 간단한 쿼리 실행 (예: 현재 데이터베이스 버전 확인)
            cursor.execute("SELECT VERSION()")

            # 결과 가져오기
            result = cursor.fetchone()
            print("Database connection successful. MySQL version:", result['VERSION()'])

            sql = f"""
                SELECT 
                    count(*) as cnt 
                FROM REGION_CODE_tbl 
                WHERE si = '{obj['si']}'
                AND region = '{obj['region']}'
            """
            cursor.execute(sql)
            rs = cursor.fetchone()

            query, values = createQuery(obj)

            if rs['cnt'] == 0:
                sql = f"INSERT INTO REGION_CODE_tbl SET {query}"
                print(sql)
                cursor.execute(sql, values)
                conn.commit()
            else:
                sql = f"UPDATE REGION_CODE_tbl SET {query} WHERE si = %s AND region = %s"
                values.append(obj['si'])
                values.append(obj['region'])
                print(sql)
                cursor.execute(sql, values)
                conn.commit()

    cursor.close()


def createQuery(data):
    """
    주어진 데이터 딕셔너리로부터 SQL 쿼리를 생성합니다.

    :param table_name: SQL 테이블 이름 (문자열)
    :param data: 삽입할 데이터를 포함한 딕셔너리
    :return: 생성된 SQL 쿼리 문자열과 해당 값들의 튜플
    """
    # 필드와 값을 추출하여 SET 형식의 쿼리문 생성
    set_clause = ", ".join([f"{key} = %s" for key in data.keys()])

    values = list(data.values())

    return set_clause, values


if __name__ == "__main__":
    main()
