import asyncio
from playwright.async_api import async_playwright
import logging
import psutil
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def scrape_page():
    try:
        # 메모리 사용량 모니터링 시작
        process = psutil.Process()
        initial_memory = process.memory_info().rss / 1024 / 1024
        logger.info(f"초기 메모리 사용량: {initial_memory:.2f} MB")

        async with async_playwright() as p:
            # webkit은 메모리를 덜 사용합니다
            browser = await p.webkit.launch(
                headless=True,
                args=[
                    '--disable-gpu',
                    '--disable-dev-shm-usage',
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--single-process'
                ]
            )

            context = await browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )

            page = await context.new_page()

            url = "https://ev.or.kr/nportal/buySupprt/initSubsidyPaymentCheckAction.do"
            logger.info(f"페이지 로딩 시작: {url}")

            # 페이지 로드
            await page.goto(url)

            # 페이지가 완전히 로드될 때까지 대기
            await page.wait_for_load_state('networkidle')

            # 추가 대기 시간
            await asyncio.sleep(2)

            # 페이지 내용 가져오기
            content = await page.content()
            logger.info("페이지 콘텐츠 가져오기 성공")

            # 브라우저 종료
            await browser.close()

            # 최종 메모리 사용량 확인
            final_memory = process.memory_info().rss / 1024 / 1024
            logger.info(f"최종 메모리 사용량: {final_memory:.2f} MB")
            logger.info(f"메모리 증가량: {final_memory - initial_memory:.2f} MB")

            return content

    except Exception as e:
        logger.error(f"에러 발생: {str(e)}")
        raise


async def main():
    try:
        content = await scrape_page()
        # 여기서 content를 사용하여 원하는 작업 수행
        print(content)

    except Exception as e:
        logger.error(f"메인 함수 에러: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("프로그램이 사용자에 의해 중단되었습니다.")
    except Exception as e:
        logger.error(f"예상치 못한 오류 발생: {str(e)}")
