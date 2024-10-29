from datetime import datetime
from bs4 import BeautifulSoup
import asyncio
from pyppeteer import launch
import sys
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def main():
    browser = None
    try:
        # 브라우저 옵션 추가
        browser = await launch(
            headless=True,
            args=[
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-gpu',
                '--window-size=1920,1080',
                '--disable-notifications',
                '--no-first-run',
                '--disable-extensions',
                '--disable-popup-blocking'
            ],
            ignoreHTTPSErrors=True
        )
        
        page = await browser.newPage()
        
        # 타임아웃 설정
        page.setDefaultNavigationTimeout(30000)  # 30초
        
        # 유저 에이전트 설정
        await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36')
        
        url = "https://ev.or.kr/nportal/buySupprt/initSubsidyPaymentCheckAction.do"
        
        # 페이지 로딩 대기
        logger.info("페이지 로딩 시작...")
        response = await page.goto(url, {'waitUntil': 'networkidle0'})
        
        # 상태 코드 확인
        if response.status != 200:
            logger.error(f"페이지 로딩 실패. 상태 코드: {response.status}")
            return
            
        # 페이지가 완전히 로드될 때까지 대기
        await page.waitForSelector('table')  # 테이블이 로드될 때까지 대기
        
        content = await page.content()
        
        # BeautifulSoup으로 파싱
        soup = BeautifulSoup(content, 'html.parser')
        
        # 데이터 추출
        trs = soup.select("#editForm > div.contentList.fz13 > table > tbody > tr")
        
        if not trs:
            logger.warning("테이블 데이터를 찾을 수 없습니다.")
        else:
            for tr in trs:
                print(tr.text.strip())  # 각 행의 텍스트 출력
                
    except Exception as e:
        logger.error(f"에러 발생: {str(e)}")
        
    finally:
        if browser:
            await browser.close()
            logger.info("브라우저가 종료되었습니다.")

if __name__ == '__main__':
    try:
        asyncio.get_event_loop().run_until_complete(main())
    except KeyboardInterrupt:
        logger.info("프로그램이 사용자에 의해 중단되었습니다.")
    except Exception as e:
        logger.error(f"예상치 못한 에러 발생: {str(e)}")