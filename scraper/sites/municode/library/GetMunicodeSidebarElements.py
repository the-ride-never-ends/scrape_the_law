import asyncio
from collections import deque
import time
from typing import NamedTuple

import pandas as pd

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
from utils.shared.decorators.adjust_wait_time_for_execution import adjust_wait_time_for_execution, async_adjust_wait_time_for_execution
from utils.shared.load_from_csv import load_from_csv
from utils.shared.save_to_csv import save_to_csv
from utils.shared.decorators.try_except import try_except, async_try_except

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
        self.output_folder:str = output_folder
        self.place_name:str = None

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

    async def _choose_browse_when_given_choice(self):
        """
        Choose to browse the table of contents if given the choice between that and Municode's documents page.
        """
        pass


    async def is_regular_municode_page(self) -> bool:
        # Define the selector for the button.
        # As the all 'regular' pages on Municode have a sidebar, 
        # we can use the presence of the sidebar to determine if we're on a 'regular' page.
        button_selector = '#codebankToggle button[data-original-title="CodeBank"]'

        # Wait 5 seconds for the button to be visible
        try:
            await self.page.wait_for_selector(button_selector, state='visible', timeout=5000)
            logger.info("Codebank button visible.")
            return True
        except:
            logger.info("CodeBank button not visible. ")
            self.take_screenshot(
                self.page.url,
                prefix="is_regular_municode_page",
                full_page=True, 
            )
            return False


    async def get_code_version_button_texts(self, max_retries: int=3, retry_delay: int=1) -> list[str]:
        counter = 0
        for attempt in range(max_retries):
            try:

                logger.debug(f"Attempt {attempt + 1} of {max_retries} to get button texts")
                
                # Wait for the container first
                await self.page.wait_for_selector("#codebank", timeout=5000)
                
                # Wait a brief moment for Angular rendering
                await asyncio.sleep(0.5)
                
                # Try different selectors
                buttons = await self.page.locator("#codebank button").all()
                if not buttons:
                    buttons = await self.page.locator(".timeline-entry button").all()
                if not buttons:
                    buttons = await self.page.locator(".card-body button").all()
                    
                if buttons:
                    logger.debug(f"Found {len(buttons)} buttons on attempt {attempt + 1}")
                    
                    versions = []
                    for button in buttons:
                        try:
                            # Wait for each button to be stable
                            await button.wait_for(state="attached", timeout=1000)
                            text = await button.text_content()
                            if text and text.strip():
                                versions.append(text.strip())
                        except Exception as e:
                            logger.warning(f"Failed to get text from button: {e}")
                            continue
                    
                    if versions:
                        logger.info(f"Successfully got {len(versions)} version texts")
                        return versions
                        
                logger.warning(f"No valid versions found on attempt {attempt + 1}")
                await asyncio.sleep(retry_delay)

            except TimeoutError as e:
                counter += 1
                logger.warning(f"Timeout on attempt {attempt + 1}: {e}")
                await asyncio.sleep(retry_delay)
            except Exception as e:
                counter += 1
                logger.error(f"Unexpected error on attempt {attempt + 1}: {e}")
                await asyncio.sleep(retry_delay)
        
        # If we get here, all retries failed
        logger.exception(f"Failed to get button texts after {max_retries} attempts. Returning...")
        return


    #@async_try_except(exception=[AsyncPlaywrightTimeoutError, AsyncPlaywrightError])
    async def scrape_code_version_popup_menu(self, place_id: str):
        """
        Scrape a code version pop-up menu
        """

        # Define variables
        js_kwargs = {}
        js_kwargs['codebank_label'] = codebank_label = 'CodeBank' # 'span.text-sm text-muted'
        # NOTE Brute force time baby!
        codebank_button = '#codebankToggle button[data-original-title="CodeBank"]'

        # '#codebankToggle button[data-intro="Switch between old and current versions."]' # f'button:has({codebank_label}):has(i.fa fa-caret-down)'

        logger.info("Waiting for the codebank button to be visible...")
        await self.page.wait_for_selector(codebank_button, state='visible')

        logger.info("Codebank button is visible. Getting current version from it...") # CSS selector ftw???
        current_version = await self.page.locator(codebank_button).text_content() #codebank > ul > li:nth-child(1) > div.timeline-entry > div > div
        logger.debug(f"current_version: {current_version}")


        # Hover over and click the codebank to open the popup menu
        logger.info(f"Got current code version '{current_version}'. Hovering over and clicking Codebank button...")
        await self.move_mouse_cursor_to_hover_over(codebank_button)
        await self.page.click(codebank_button)
        
        # Wait for the popup menu to appear
        logger.info("Codebank button was hovered over and clicked successfully. Waiting for popup menu...")
        popup_selector = 'List of previous versions of code'

        # 'aria-label.List of previous versions of code' # NOTE'.popup-menu' is a CSS selector! Also, since the aria-label is hidden, you need to use state='hidden' in order to get it.
        await self.page.wait_for_selector(popup_selector, state='hidden')
        #codebank > ul > li:nth-child(1) > div.timeline-entry > div > div > button
        # Go into the CodeBank list and get the button texts.
        # These should be all the past version dates.
        logger.info("Popup menu is visible. Getting previous code versions...")

        # NOTE: This is a CSS selector!
        # 1. First, wait for the timeline to be actually present and visible
        try: 
            await self.page.wait_for_selector("ul.timeline", timeout=5000)
            logger.debug("Timeline UL found")
        except Exception as e:
            logger.error(f"Timeline UL not found: {e}")
            raise

        # 2. Then, get all the buttons inside the timeline
        versions = await self.get_code_version_button_texts()

        if versions:
            # Save the versions to a CSV
            df = pd.DataFrame(versions, columns=['version'])
            df.to_csv(os.path.join(self.output_dir, f'all_code_versions_{place_id}.csv'), index=False, quoting=1)

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


    def extract_heading_and_href_from_toc_node(self, node: ElementHandle) -> dict:
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


    async def get_page_version(self) -> bool:
        return self.is_regular_municode_page()

    # Decorator to wait per Municode's robots.txt
    # NOTE Since code URLs are processed successively, we can subtract off the time it took to get all the pages elements
    # from the wait time specified in robots.txt. This should speed things up (?).
    @try_except(exception=[AsyncPlaywrightError])
    #@async_adjust_wait_time_for_execution(wait_in_seconds=LEGAL_WEBSITE_DICT["municode"]["wait_in_seconds"])
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
        self.place_name = place_name = row.place_name.lower().replace(" ", "_")
        place_id = f"{place_name}_{row.gnis}"

        # Skip the webpage if we already got it.
        output_dict = self._skip_if_we_have_url_already(input_url)
        if output_dict:
            return output_dict

        output_dict = {
            'url_hash': row.url_hash, 
            'input_url': input_url, 
            'gnis': row.gnis
        }

        # Go to the webpage
        await self.navigate_to(input_url, idx=i)
        logger.info("Navigated to input_url")

        # Screenshot the initial page.
        await self.take_screenshot(
            place_id, 
            prefix="navigate_to",
            full_page=True, 
            open_image_after_save=True
        )

        await self.save_page_html_content_to_output_dir(f"{place_id}_opening_webpage.html")

        # See what version of the page we're on.
        regular_page = await self.is_regular_municode_page()

        # Get the current code version and all code versions if we're on a regular page.
        if regular_page: # We assume that all regular Municode pages have a version number sidebar element.
            prefix = "scrape_code_version_popup_menu"
            await self.scrape_code_version_popup_menu(place_id)
            # Close the version menu
            await self.click_on_version_sidebar_closer()
        else:
            logger.info(f"{place_name}, gnis {row.gnis} does not have a regular municode page. Skipping...")
            prefix = "is_not_regular_municode_page"

        # Screenshot the page after running scrape_code_version_popup_menu.
        await self.take_screenshot(
            input_url,
            prefix=prefix,
            full_page=True, 
            open_image_after_save=True
        )
        # Get the html of the opening page and write it to a file.
        await self.save_page_html_content_to_output_dir(f"{place_id}_{prefix}.html")

        return

    #@async_try_except(exception=[AsyncPlaywrightError, AsyncPlaywrightTimeoutError],raise_exception=True)
    async def click_on_version_sidebar_closer(self):
        """
        Click on the 'X' button for the version menu

        #toc > div.zone-body.toc-zone-body > div.toc-wrapper > div
        #toc > div.zone-body.toc-zone-body > div.toc-wrapper > div > button
        """
        logger.debug("Waiting for the codebank 'X' button to be visible...")
        # codebank_button = '#toc button[class="btn btn-icon-toggle btn-default pull-right"]'
        codebank_close_button = "i.md.md-close"
        prefix = "click_on_version_sidebar_closer"

        await self.take_screenshot(
            self.place_name,
            prefix=f"{prefix}_before",
            full_page=True,
            open_image_after_save=True
        )
        await self.save_page_html_content_to_output_dir(f"{self.place_name}_{prefix}_before.html")

        logger.debug(f"Clicking via JS with selector '{codebank_close_button}'...")
        # args = {"codebank_close_button": codebank_close_button}
        js = """
            let button = document.querySelector("i.md.md-close");
            if (button) {
                button.click();
            }
        """
        await self.page.evaluate(js)
        logger.debug(f"JS button clicking code for selector '{codebank_close_button}' evaluated.")
        await self.take_screenshot(
            self.place_name,
            prefix=f"{prefix}_after",
            full_page=True,
            open_image_after_save=True
        )

        await self.save_page_html_content_to_output_dir(f"{self.place_name}_{prefix}_after.html")

        return 






        # try: # NOTE get_by commands return a LOCATOR and thus are NOT Coroutines that need to be awaited.

        #     num = await self.page.locator(codebank_button).count()
        #     logger.info(f"Found {num} 'Close' buttons")

        #     # button: Locator = self.page.get_by_label("Table of Contents").get_by_role("button").and_(self.page.get_by_text("Close"))
        #     button: Locator = await self.page.locator(codebank_button)

        #     await self.save_page_html_content_to_output_dir(f"{self.place_name}_{prefix}.html")

        #     #self.page.get_by_test_id("toc").get_by_role("button", name="Close", include_hidden=True) #self.page.wait_for_selector(codebank_button, state="visible", timeout=5000)

        #     logger.info("version menu 'X' button found")
        # except Exception as e:
        #     logger.exception(f"version menu 'X' button not found: {e}")
        #     raise

        # # # Hover over and click the X to open the popup menu
        # # logger.debug(f"Hovering over and pressing version menu 'X' button to close the version menu...")
        # # await element.hover()
        # # #await self.move_mouse_cursor_to_hover_over(codebank_button)
        # logger.debug(f"Hover over version menu 'X' button successful...\nClicking...")
        # await asyncio.sleep(1)
        # await button.click()

        # # logger.debug("Clicking with force=True")
        # # await asyncio.sleep(1)
        # # await button.click(force=True),

        # await self.save_page_html_content_to_output_dir(f"{self.place_name}_{prefix}_after_click.html")

        # # JavaScript click
        # logger.debug("Clicking with JS")
        # await asyncio.sleep(1)
        # await button.evaluate('element => element.click()'),
        

        # # Dispatch click event
        # logger.debug("Clicking with dispatch event click")
        # await asyncio.sleep(1)
        # await button.dispatch_event('click'),

        # # Double click
        # logger.debug("Double Clicking")
        # await asyncio.sleep(1)
        # await button.dblclick(),

        # # Click with delay
        # logger.debug("Clicking with delay")
        # await asyncio.sleep(1)
        # await button.click(delay=100),

        # # if await self.page.get_by_role("button").and_(self.page.get_by_text("Close")).count() > 0:
        # #     logger.error(f"version menu 'X' button still visible after clicking. Clicking again...")
        # #     await self.page.get_by_role("button").and_(self.page.get_by_text("Close")).click()

        # logger.debug(f"Version menu 'X' button clicked successfully.\nReturning...")

        # return


    # async def debug_button_click(self, button: Locator):

    #     # 1. Basic element checks
    #     logger.info("=== BASIC ELEMENT CHECKS ===")
    #     count = await button.count()
    #     logger.info(f"Elements found: {count}")
        
    #     if count == 0:
    #         logger.error("Button not found in DOM!")
    #         return
            
    #     # 2. Visibility checks
    #     logger.info("\n=== VISIBILITY CHECKS ===")
    #     is_visible = await button.is_visible()
    #     is_hidden = await button.is_hidden()
    #     logger.info(f"Is visible: {is_visible}")
    #     logger.info(f"Is hidden: {is_hidden}")
        
    #     # 3. Element properties
    #     logger.info("\n=== ELEMENT PROPERTIES ===")
    #     properties = await self.page.evaluate('''element => {
    #         const computedStyle = window.getComputedStyle(element);
    #         return {
    #             display: computedStyle.display,
    #             visibility: computedStyle.visibility,
    #             opacity: computedStyle.opacity,
    #             position: computedStyle.position,
    #             zIndex: computedStyle.zIndex,
    #             pointerEvents: computedStyle.pointerEvents,
    #             disabled: element.disabled,
    #             offsetWidth: element.offsetWidth,
    #             offsetHeight: element.offsetHeight,
    #             getBoundingClientRect: element.getBoundingClientRect()
    #         }
    #     }''')
    #     logger.info(f"Element properties: {properties}")
        
    #     # 4. Check event listeners
    #     logger.info("\n=== EVENT LISTENERS ===")
    #     has_listeners = await button.evaluate('''element => {
    #         const listeners = window.getEventListeners ? window.getEventListeners(element) : {};
    #         return {
    #             hasClickListener: 'click' in listeners,
    #             totalListeners: Object.keys(listeners).length,
    #             listenerTypes: Object.keys(listeners)
    #         }
    #     }''')
    #     logger.info(f"Event listeners: {has_listeners}")
        
    #     # 5. Set up console monitoring
    #     logger.info("\n=== ATTEMPTING CLICKS ===")
    #     self.page.on('console', lambda msg: logger.debug(f'Console: {msg.text}'))
    #     self.page.on('pageerror', lambda err: logger.error(f'Page error: {err.text}'))
        
    #     # 6. Try different click methods

    #     click_attempts = [
    #         # Force click
    #         await button.click(force=True),

    #         # JavaScript click
    #         await button.evaluate('element => element.click()'),
            
    #         # Dispatch click event
    #         await button.dispatch_event('click'),

    #         # Double click
    #         await button.dblclick(),

    #         # Click with delay
    #         await button.click(delay=100),

    #     ]

    #     # Try each click method
    #     for i, click_attempt in enumerate(click_attempts, 1):
    #         try:
    #             logger.info(f"\nTrying click method {i}...")
    #             await click_attempt()
    #             logger.info(f"Click method {i} completed without errors")

    #             # Check if element still exists after click
    #             post_click_count = await button.count()
    #             logger.info(f"Element still exists after click: {post_click_count > 0}")

    #             # Brief pause to observe any changes
    #             await asyncio.sleep(0.5)
                
    #         except Exception as e:
    #             logger.error(f"Click method {i} failed: {str(e)}")
        
    #     # 7. Check for overlapping elements
    #     logger.info("\n=== CHECKING FOR OVERLAPPING ELEMENTS ===")
    #     overlapping = None #or await self.page.evaluate('''() => {
    #     #     const element = document.querySelector('#toc button[aria-label="Close"]');
    #     #     if (!element) return [];
            
    #     #     const rect = element.getBoundingClientRect();
    #     #     const elements = document.elementsFromPoint(
    #     #         rect.left + rect.width/2,
    #     #         rect.top + rect.height/2
    #     #     );
    #     #     return elements.map(el => ({
    #     #         tag: el.tagName,
    #     #         id: el.id,
    #     #         class: el.className,
    #     #         zIndex: window.getComputedStyle(el).zIndex
    #     #     }));
    #     # }''')
    #     logger.info(f"Elements at click position: {overlapping}")
        
    #     # 8. Final element state
    #     logger.info("\n=== FINAL ELEMENT STATE ===")
    #     final_count = await button.count()
    #     final_visible = await button.is_visible() if final_count > 0 else False
    #     logger.info(f"Element still exists: {final_count > 0}")
    #     logger.info(f"Element still visible: {final_visible}")
        
    #     return {
    #         "found": count > 0,
    #         "visible": is_visible,
    #         "properties": properties,
    #         "has_listeners": has_listeners,
    #         "overlapping_elements": overlapping
    #     }


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
        # TODO MAKE START CODE NOT CURSED.
        municode: GetMunicodeSidebarElements = await GetMunicodeSidebarElements(
                                                        domain, pw_instance, 
                                                        user_agent='*', **pw_options
                                                        ).start(
                                                            domain, pw_instance, 
                                                            user_agent='*', **pw_options
                                                        )
        assert municode.browser is not None, "Browser is None."
        logger.info("GetMunicodeSidebarElements initialized successfully")

        logger.info(f"Starting get_municode_sidebar_elements loop. Processing {len(sources_df)} URLs...")

        # Go through each URL.
        # NOTE This will take forever, but we can't afford to piss off Municode. 
        # Just 385 randomly chosen ones should be enough for a statistically significant sample size.
        # We also only need to do this once.
        list_of_lists_of_dicts: list[dict] = [ 
            await municode.get_municode_sidebar_elements(
                i, row, len(sources_df) # NOTE Adding the 'if row else None' is like adding 'continue' to a regular for-loop.
                ) if row else None for i, row in enumerate(sources_df.itertuples(), start=1)
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
