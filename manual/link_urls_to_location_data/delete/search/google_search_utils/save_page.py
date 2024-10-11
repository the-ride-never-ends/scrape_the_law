
from playwright.async_api import async_playwright


def save_mhtml(path: str, text: str):
    with open(path, mode='w', encoding='UTF-8', newline='\n') as file:
        file.write(text)


def save_page(url: str, path: str):
    with async_playwright() as playwright:
        browser = playwright.chromium.launch(headless=False)
        page = browser.new_page()
        page.goto(url)

        client = page.context.new_cdp_session(page)
        mhtml = client.send("Page.captureSnapshot")['data']
        save_mhtml(path, mhtml)
        browser.close()

        