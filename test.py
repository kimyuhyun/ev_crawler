from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def setup_driver():
    firefox_options = Options()
    firefox_options.add_argument('--headless')
    
    return webdriver.Firefox(options=firefox_options)

def main():
    driver = None
    try:
        logger.info("Firefox 드라이버 초기화 중...")
        driver = setup_driver()
        
        url = "https://ev.or.kr/nportal/buySupprt/initSubsidyPaymentCheckAction.do"
        logger.info(f"페이지 로딩 시작: {url}")
        
        driver.get(url)
        
        wait = WebDriverWait(driver, 60)
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        
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
    # Firefox와 geckodriver 설치
    try:
        import subprocess
        subprocess.run(['sudo apt-get', 'update'], check=True)
        subprocess.run(['sudo apt-get', 'install', '-y', 'firefox-esr'], check=True)
        subprocess.run(['sudo apt-get', 'install', '-y', 'firefox-geckodriver'], check=True)
    except:
        logger.warning("패키지 설치 실패. 이미 설치되어 있거나 권한이 없을 수 있습니다.")
    
    main()