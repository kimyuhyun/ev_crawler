from datetime import datetime
from bs4 import BeautifulSoup
import asyncio
from pyppeteer import launch
import logging
import subprocess

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def get_chrome_path():
    try:
        # which 명령어로 chromium-browser 경로 찾기
        chrome_path = subprocess.check_output(['which', 'chromium-browser']).decode().strip()
        return chrome_path
    except:
        try:
            # 실패하면 chrome 경로 찾기
            chrome_path = subprocess.check_output(['which', 'chrome']).decode().strip()
            return chrome_path
        except:
            try:
                # 마지막으로 google-chrome 경로 찾기
                chrome_path = subprocess.check_output(['which', 'google-chrome']).decode().strip()
                return chrome_path
            except:
                return None

async def main():
    try:
        chrome_path = await get_chrome_path()
        logger.info(f"Chrome 경로: {chrome_path}")

        if not chrome_path:
            raise Exception("Chrome/Chromium이 설치되어 있지 않습니다.")

        browser = await launch(
            headless=True,
            executablePath=chrome_path,
            args=[
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-gpu',
                '--disable-software-rasterizer',
                '--window-size=1920,1080',
                '--disable-notifications',
                '--no-first-run',
                '--disable-extensions'
            ],
            ignoreHTTPSErrors=True
        )

        logger.info("브라우저 실행 성공")

        page = await browser.newPage()
        await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
        await page.setViewport({'width': 1920, 'height': 1080})
        await page.setDefaultNavigationTimeout(60000)

        url = "https://ev.or.kr/nportal/buySupprt/initSubsidyPaymentCheckAction.do"
        logger.info(f"페이지 로딩 시작: {url}")

        response = await page.goto(url, {
            'waitUntil': 'networkidle0',
            'timeout': 60000
        })

        if response.status != 200:
            logger.error(f"페이지 로딩 실패. 상태 코드: {response.status}")
            return

        content = await page.content()
        logger.info("페이지 콘텐츠 가져오기 성공")
        print(content)

    except Exception as e:
        logger.error(f"에러 발생: {str(e)}")
        raise
    finally:
        if 'browser' in locals():
            await browser.close()
            logger.info("브라우저 종료됨")

if __name__ == '__main__':
    # 필요한 패키지 설치 확인
    try:
        subprocess.run(['apt-get', 'update'], check=True)
        subprocess.run(['apt-get', 'install', '-y', 
                       'chromium-browser',
                       'libnss3',
                       'libgbm1',
                       'libxshmfence1',
                       'libatk1.0-0',
                       'libatk-bridge2.0-0',
                       'libcups2',
                       'libdrm2',
                       'libxcomposite1',
                       'libxdamage1',
                       'libxfixes3',
                       'libxrandr2',
                       'libgbm1',
                       'libasound2'], check=True)
    except subprocess.CalledProcessError as e:
        logger.error(f"패키지 설치 중 오류 발생: {str(e)}")
        logger.info("sudo 권한으로 실행해주세요.")
        exit(1)

    asyncio.get_event_loop().run_until_complete(main())