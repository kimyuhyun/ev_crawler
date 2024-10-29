from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import logging
import time
import subprocess
import shutil
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def check_requirements():
    # Firefox snap 설치 확인
    try:
        subprocess.run(['snap', 'list', 'firefox'], check=True, capture_output=True)
        logger.info("Firefox가 설치되어 있습니다.")
    except subprocess.CalledProcessError:
        logger.error("Firefox가 설치되어 있지 않습니다.")
        return False

    # Geckodriver 확인
    if not os.path.exists('/usr/local/bin/geckodriver'):
        logger.error("Geckodriver가 설치되어 있지 않습니다.")
        return False

    logger.info("모든 요구사항이 충족됩니다.")
    return True


def setup_driver():
    try:
        firefox_options = Options()
        firefox_options.add_argument('--headless')
        firefox_options.add_argument('--width=1920')
        firefox_options.add_argument('--height=1080')
        firefox_options.set_preference('general.useragent.override',
                                       'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0')

        service = Service(executable_path='/usr/local/bin/geckodriver')

        driver = webdriver.Firefox(
            service=service,
            options=firefox_options
        )

        return driver
    except Exception as e:
        logger.error(f"드라이버 설정 중 오류 발생: {str(e)}")
        raise


def install_requirements():
    try:
        # Firefox 설치 (snap 사용)
        subprocess.run(['sudo', 'snap', 'install', 'firefox'], check=True)

        # Geckodriver 설치
        subprocess.run(['sudo wget', 'https://github.com/mozilla/geckodriver/releases/download/v0.33.0/geckodriver-v0.33.0-linux64.tar.gz'], check=True)
        subprocess.run(['sudo tar', '-xvzf', 'geckodriver-v0.33.0-linux64.tar.gz'], check=True)
        subprocess.run(['sudo', 'mv', 'geckodriver', '/usr/local/bin/'], check=True)
        subprocess.run(['sudo', 'chmod', '+x', '/usr/local/bin/geckodriver'], check=True)

        # 임시 파일 정리
        if os.path.exists('geckodriver-v0.33.0-linux64.tar.gz'):
            os.remove('geckodriver-v0.33.0-linux64.tar.gz')

        logger.info("필요한 구성 요소 설치 완료")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"설치 중 오류 발생: {str(e)}")
        return False


def main():
    driver = None
    try:
        if not check_requirements():
            logger.info("필요한 구성 요소 설치 시작...")
            if not install_requirements():
                logger.error("설치 실패")
                return

        logger.info("Firefox 드라이버 초기화 중...")
        driver = setup_driver()

        url = "https://ev.or.kr/nportal/buySupprt/initSubsidyPaymentCheckAction.do"
        logger.info(f"페이지 로딩 시작: {url}")

        driver.get(url)

        wait = WebDriverWait(driver, 60)
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))

        time.sleep(3)

        content = driver.page_source
        logger.info("페이지 콘텐츠 가져오기 성공")
        print(content)

    except Exception as e:
        logger.error(f"에러 발생: {str(e)}")
        raise
    finally:
        if driver:
            driver.quit()
            logger.info("브라우저 종료됨")


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logger.info("프로그램이 사용자에 의해 중단되었습니다.")
    except Exception as e:
        logger.error(f"예상치 못한 오류 발생: {str(e)}")
