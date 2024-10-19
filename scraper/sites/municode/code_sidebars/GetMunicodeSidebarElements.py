from collections import deque
import time
from typing import NamedTuple


from playwright.async_api import (
    async_playwright,
    ElementHandle,
    expect,
    Locator,
    Error as AsyncPlaywrightError,
    TimeoutError as AsyncPlaywrightTimeoutError
)


from scraper.child_classes.playwright.AsyncPlaywrightScrapper import AsyncPlaywrightScrapper


from utils.shared.make_sha256_hash import make_sha256_hash
from utils.shared.sanitize_filename import sanitize_filename
from utils.shared.decorators.adjust_wait_time_for_execution import adjust_wait_time_for_execution
from utils.shared.load_from_csv import load_from_csv
from utils.shared.decorators.try_except import try_except

from config import *

from logger import Logger
logger = Logger(logger_name=__name__)


output_folder = os.path.join(OUTPUT_FOLDER, "get_sidebar_urls_from_municode")
if not os.path.exists(output_folder):
    print(f"Creating output folder: {output_folder}")
    os.mkdir(output_folder)


class GetMunicodeSidebarElements(AsyncPlaywrightScrapper):
    """
    Get the sidebar elements from a Municode URL page.
    NOTE This uses Playwright rather than Selenium.
    Using a synchronous library to deal with asynchronous JavaScript is more trouble than it's worth.
    Also, fuck multiple libraries.
    """

    def __init__(self,
                domain: str,
                pw_instance,
                *args,
                user_agent: str="*",
                **kwargs):
        super().__init__(domain, pw_instance, *args, user_agent=user_agent, **kwargs)
        self.xpath_dict = {
            "current_version": '//*[@id="codebankToggle"]/button/text()',
            "version_button": '//*[@id="codebankToggle"]/button/',
            "version_text_paths": '//mcc-codebank//ul/li//button/text()',
            'toc': "//input[starts-with(@id, 'genToc_')]" # NOTE toc = Table of Contents
        }
        self.queue = deque()
        self.output_folder = output_folder

    def test(self):
        self.page.set_content

    async def _get_past_front_page(self):
        # See whether or not the ToC button is on the page.
        locator = self.page.locator("")
        toc_button_locator = await self.page.get_by_text("Browse table of contents")
        expect(toc_button_locator).to_be_visible()

        #         <div class="col-sm-6 hidden-md hidden-lg hidden-xl" style="margin-top: 8px;">
        #     <button type="button" class="btn btn-raised btn-primary" ng-click="$root.zoneMgrSvc.toggleVisibleZone()">
        #         <i class="fa fa-list-ul"></i> Browse table of contents
        #     </button>
        # </div>

    async def scrape_version_and_menu(self):
        """
        Scrape a code version pop-up menu
        """

        # Define variables
        js_kwargs = {}
        js_kwargs['codebank_label'] = codebank_label = 'CodeBank' # 'span.text-sm text-muted'
        codebank_button = f'button:has({codebank_label}):has(i.fa.fa-caret-down)'

        # Wait for the codebank button to be visible
        await self.page.wait_for_selector(codebank_button)

        # Get the text of the codebank button
        # This should also be the current version date.
        current_version = self.page.get_by_role('button').locator("CodeBank").text_content
        logger.debug(f"current_version: {current_version}")

        # Click the codebank to open the popup menu
        await self.page.click(codebank_button)
        
        # Wait for the popup menu to appear
        popup_selector = 'aria-label.List of previous versions of code' # NOTE'.popup-menu' is a CSS selector!
        await self.page.wait_for_selector(popup_selector)

        # Go into the CodeBank list and get the button texts.
        # These should be all the past version dates.
        prev_code_locator: Locator = self.page.get_by_label("List of previous versions of code")
        all_code_versions = [
            button.text_content for button in prev_code_locator.get_by_role('button').all()
        ]
        for i, version in enumerate(all_code_versions, start=1):
            logger.info(f"version {i}: {version}")
        
        return 

    async def _scrape_toc(self, base_url: str, wait_time: int) -> list[dict]:
        """
        Scrape a Municode URL's Table of Contents.
        """
        # Initialize the queue
        initial_selector = "li[ng-repeat='node in toc.topLevelNodes track by node.Id']"
        self.queue.append((self.page.url, initial_selector))
        all_data = []

        while self.queue:
            # Get the current url and its selector from the queue
            current_url, selector = self.queue.popleft()

            # Ensure we're on the correct page
            if self.page.url != current_url:
                await self.page.goto(current_url)

            # Push the sidebar button.
            # NOTE This does not appear when going to the site in a regular Chrome browser.
            # TODO 
            # Get all the elements currently in the sidebar.
            # elements = WebDriverWait(self.driver, wait_time).until(
            #     EC.presence_of_all_elements_located((By.XPATH, xpath))
            # )

            # Wait for and get all the elements currently in the sidebar
            elements = await self.page.wait_for_selector(selector, state="attached", timeout=wait_time * 1000)
            elements = await self.page.query_selector_all(selector)


            for element in elements:
                # Get the innerHTML of the element
                inner_html = await element.inner_html()

                # Extract data and add to all_data
                # (You'll need to implement the extract_data method for Playwright)
                data = await self.extract_data(element)
                all_data.append(data)

                # Find the expand button
                expand_button = await element.query_selector('button.toc-item-expand')

                if expand_button:
                    is_expanded = await expand_button.get_attribute('aria-expanded')
                    if is_expanded != 'true':
                        await expand_button.click()
                        # Wait for expansion
                        await self.page.wait_for_selector(f"{selector}[aria-expanded='true']", state="attached", timeout=wait_time * 1000)

                        # Add child nodes to the queue
                        child_selector = f"{selector} > ul > li[ng-repeat='node in node.Children track by node.Id']"
                        self.queue.append((self.page.url, child_selector))

        return all_data



    def extract_data(self, node: ElementHandle) -> dict:
        """
        Extract and return the heading and href data from a toc node
        """
        pass
        # select = node.find_element(By.CSS_SELECTOR, 'a')
        # return {'heading': select.text, 'href': select.get_attribute('href')}


    def _skip_if_we_have_url_already(self, url: str) -> list[dict]|None:
        """
        Check if we already have a CSV file of the input URL. 
        If we do, load it as a list of dictionaries and return it. Else, return None
        """
        url_file_path = os.path.join(OUTPUT_FOLDER, f"{sanitize_filename(url)}.csv")
        if os.path.exists(url_file_path):
            logger.info(f"Got URL '{url}' already. Loading csv...")
            output_dict = load_from_csv(url_file_path)
            return output_dict
        else:
            return None


    # Decorator to wait per Municode's robots.txt
    # NOTE Since code URLs are processed successively, we can subtract off the time it took to get all the pages elements
    # from the wait time specified in robots.txt. This should speed things up (?).
    @try_except(exception=[AsyncPlaywrightError])
    @adjust_wait_time_for_execution(wait_in_seconds=LEGAL_WEBSITE_DICT["municode"]["wait_in_seconds"])
    async def get_municode_sidebar_elements(self, 
                                      i: int,
                                      row: NamedTuple,
                                      len_df: int,
                                      ) -> dict:
        """
        Extract the code versions and table of contents from a city's Municode page.
        NOTE This function orchestrates all the methods of this class, similar to main.py

        Example Input:
            row
            Pandas(Index=0, 
                    url=https://library.municode.com/az/cottonwood, 
                    gnis: 12345, 
                    place_name: Town of Cottonwood,
                    url_hash=ed22a03bd810467b0fe30f1306a2aaa9c1d047d9799be5...)

        Example Output:
            output_dict = {
                'url_hash': ed22a03bd810467b0fe30f1306a2aaa9c1d047d9799be5,
                'input_url': "https://library.municode.com/az/cottonwood",
                'gnis': 123456789,
                'current_code_version': 'July 26th, 2024',
                'all_code_versions': ['July 26th, 2024', June 4th, 2023'],
                'table_of_contents_urls': ['www.municode_example69.com',''www.municode_example420.com'],
            }
        """
        logger.info(f"Processing URL {i} of {len_df}...")
        # Check to make sure the URL is a municode one, then initialize the dictionary.
        assert "municode" in row.url, f"URL '{row.url}' is not for municode."
        input_url = row.url
        wait_time = 10

        # Skip the webpage if we already got it.
        output_dict = self._skip_if_we_have_url_already(input_url)
        if output_dict:
            return output_dict

        output_dict = {
            'url_hash': row.url_hash, 
            'input_url': input_url, 
            'gnis': row.gnis
        }

        await self.navigate_to(input_url)
        logger.info("Navigated to input_url")

        self.take_screenshot(input_url, full_page=True, open_image_after_save=True)
        time.sleep(30)

        return

        # self.scrape_version_and_menu()


        # toc_button = """
        # //button[contains(@class, 'btn-success') and contains(., 'View what')]
        # """
        # browser_toc_button = "btn btn-raised btn-primary"

        # # Wait for the webpage to fully load, based on the button element.

        # # Get the html of the opening page.
        # html_content = self.driver.page_source

        # # Write the HTML content to a file
        # _path = os.path.join(output_folder, f"{row.gnis}_opening_webpage.html")
        # with open(_path, "w", encoding="utf-8") as file:
        #     file.write(html_content)
        #     print(f"HTML content from {input_url} has been saved to webpage.html")
        # raise # TODO Remove this line after debug.

        # # Get the URLs from the table of contents.
        # # NOTE The tables are recursive, so this won't get all the buried URLs. 
        # toc_urls = self._scrape_toc(input_url, wait_time=wait_time)
        # output_dict["table_of_contents_urls"] = toc_urls

        # # Get the date the code was last updated.
        # output_dict['current_code_version'] = self._get_current_code_version(input_url)

        # # Get all the previous dates the code was updated.
        # # NOTE This does not grab their links, just their text.
        # all_code_versions: list[str] = self._get_all_code_versions(input_url)
        # output_dict['all_code_versions'] = all_code_versions

        # logger.info(f"Found all elements from Municode URL for gnis '{row.gnis}'")
        # # Export output_dict as a CSV file for safe-keeping.
        # url_file_path = os.path.join(output_folder, f"{sanitize_filename(input_url)}.csv")
        # save_to_csv(output_dict, url_file_path)

        # return output_dict

import pandas as pd




async def get_sidebar_urls_from_municode_with_playwright(sources_df: pd.DataFrame) -> pd.DataFrame:
    """
    Get href and text of sidebar elements in a Municode city code URL.
    """

    # Initialize webdriver.
    logger.info("Initializing webdriver...")

    # Define options for the webdriver.
    pw_options = {
        "headless": False
    }
    domain = "https://municode.com/"
    from scraper.child_classes.playwright.AsyncPlaywrightScrapper import AsyncPlaywrightScrapper


    # Get the sidebar URLs and text for each Municode URL
    async with async_playwright() as pw_instance:
        logger.info("Playwright instance initialized successfully.")

        # We use a factory method to instantiate the class to avoid context manager fuckery.
        # TODO MAKE CODE NOT CURSED.
        municode: GetMunicodeSidebarElements = await GetMunicodeSidebarElements(domain, pw_instance, user_agent='*', **pw_options).start(domain, pw_instance, user_agent='*', **pw_options)
        logger.info("GetMunicodeSidebarElements initialized successfully")

        logger.info(f"Starting get_municode_sidebar_elements loop. Processing {len(sources_df)} URLs...")

        # Go through each URL.
        # NOTE This will take forever, but we can't afford to piss off Municode. 
        # Just 385 randomly chosen ones should be enough for a statistically significant sample size.
        # We also only need to do this once.
        list_of_lists_of_dicts: list[dict] = [ # NOTE Adding the 'if row else None' is like adding 'continue' to a regular for-loop.
            await municode.get_municode_sidebar_elements(i, row, len(sources_df)) if row else None for i, row in enumerate(sources_df.itertuples(), start=1)
        ]

        await municode.exit()


    logger.info("get_municode_sidebar_elements loop complete. Flattening...")
    # Flatten the list of lists of dictionaries into just a list of dictionaries.
    output_list = [item for sublist in list_of_lists_of_dicts for item in sublist]
    
    logger.info("get_sidebar_urls_from_municode_with_selenium function complete. Making dataframes and saving...")
    save_code_versions_to_csv(output_list) # We save first to prevent pandas fuck-upery.
    urls_df = make_urls_df(output_list)


    return urls_df


def make_urls_df(output_list: list[dict]) -> pd.DataFrame:
    """
    Make urls_df

    Example Input:
    >>> output_list = [{
    >>>     'url_hash': ed22a03bd810467b0fe30f1306a2aaa9c1d047d9799be5,
    >>>     'input_url': "https://library.municode.com/az/cottonwood",
    >>>     'gnis': 123456789,
    >>>     'current_code_version': 'July 26th, 2024',
    >>>     'all_code_versions': ['July 26th, 2024', June 4th, 2023'],
    >>>     'table_of_contents_urls': ['www.municode_example69.com',''www.municode_example420.com'],
    >>> },...]

    Example Output:
        >>> urls_df.head()
        >>> url_hash    query_hash              gnis    url
        >>> 3beb75cb    not_found_from_query    156909  https://library.municode.com/.../PTIICO_CH82TA_ARTIILERETA
        >>> 4648a64b    not_found_from_query    156909  https://library.municode.com/.../PTIICO_CH26BU_ARTIINGE_S26-2IMUNTABU
        >>> 58cd5049    not_found_from_query    156909  https://library.municode.com/.../PTIICO_CH98ZO_ARTIVSURE_DIV2OREPALORE
        >>> 76205dbb    not_found_from_query    156909  https://ecode360.com/WE1870/document/224899757.pdf
        >>> 30935d36    not_found_from_query    156909  https://ecode360.com/NE0395/document/647960636.pdf
        >>> 792b4192    not_found_from_query    254139  https://ecode360.com/LO1625/document/430360980.pdf
        >>> 792b4192    not_found_from_query    254139  https://ecode360.com/LO1625/document/430360980.pdf
    """
    # Turn the list of dicts into a dataframe.
    urls_df = pd.DataFrame.from_records(output_list)

    # Make url hashes for each url
    urls_df['url_hash'] = urls_df.apply(lambda row: make_sha256_hash(row['gnis'], row['url']))

    # Rename toc urls to match the format of the table 'urls' in the MySQL database.
    urls_df.rename(columns={"table_of_contents_urls": "url"})

    # Add the dummy query_hash column.
    urls_df['query_hash'] = "not_found_from_query"

    # Drop the code version columns.
    urls_df.drop(['current_code_version','all_code_versions'], axis=1, inplace=True)

    return urls_df


from utils.shared.save_to_csv import save_to_csv
def save_code_versions_to_csv(output_list: list[dict]) -> None:
    """
    Example Input:
    >>> output_list = [{
    >>>     'url_hash': ed22a03bd810467b0fe30f1306a2aaa9c1d047d9799be5,
    >>>     'input_url': "https://library.municode.com/az/cottonwood",
    >>>     'gnis': 123456789,
    >>>     'current_code_version': 'July 26th, 2024',
    >>>     'all_code_versions': ['July 26th, 2024', June 4th, 2023'],
    >>>     'table_of_contents_urls': ['www.municode_example69.com',''www.municode_example420.com'],
    >>> },...]
    """

    # Turn the list of dicts into a dataframe.
    code_versions_df = pd.DataFrame.from_records(output_list)

    # Drop the urls columns.
    code_versions_df.drop(['url_hash','table_of_contents_urls'], axis=1, inplace=True)

    # Save the dataframe to the output folder.
    output_file = os.path.join(output_folder, sanitize_filename(output_list[0]['input_url']))
    save_to_csv(code_versions_df, output_file)

    return



    # def _get_current_code_version(self, selector: str) -> str:
    #     """
    #     Get the date for the current version of the municipal code.
    #     """

    #     # Wait for the button to be visible
    #     button_selector = 'button:has(span.text-xs.text-muted):has(i.fa.fa-caret-down)'
    #     self.page.wait_for_selector(button_selector)

    #     # Initialize HTML targets and JavaScript command.
    #     version_date_id = 'span.text-sm.text-muted'
    #     args = {"version_date_id": version_date_id}
    #     js = '() => document.querySelector("{version_date_id}").textContent'

    #     # Wait for the element to be visible
    #     self.page.wait_for_selector(version_date_id)

    #     # Get the code with JavaScript
    #     version_date: str = self.evaluate_js(js, js_kwargs=args)

    #     logger.debug(f'version_date: {version_date}')
    #     return version_date.strip()


    # async def _get_all_code_versions(self, url: str) -> list[str]:
    #     """
    #     Get the dates for current and past versions of the municipal code.
    #     NOTE: You need to click on each individual button to get the link itself.
    #     """
    #     version_date_button_selector = 'span.text-sm.text-muted'

    #     version_date = self._get_current_code_version()


    #     # Press the button that shows the code archives pop-up
    #     version_button = None
    #     await self.click_on(version_button)
    #     self.press_buttons(url, xpath=self.xpath_dict['version_button'])

    #     # Get all the dates in the pop-up.
    #     buttons = self.wait_for_and_then_return_elements(
    #         self.xpath_dict['version_text_paths'], wait_time=10, poll_frequency=0.5
    #     )
    #     version_list = [
    #         button.text.strip() for button in buttons
    #     ]
    #     logger.debug(f'version_list\n{version_list}',f=True)
    #     return version_list
