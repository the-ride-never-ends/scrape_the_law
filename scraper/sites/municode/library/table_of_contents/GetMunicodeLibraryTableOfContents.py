

import asyncio
from collections import deque
import re
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


output_folder = os.path.join(OUTPUT_FOLDER, "scrape_table_of_contents")
if not os.path.exists(output_folder):
    print(f"Creating output folder: {output_folder}")
    os.mkdir(output_folder)


class GetMunicodeLibraryTableOfContents(AsyncPlaywrightScrapper):

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


    async def scrape_toc(self, wait_time: int) -> list[dict]:
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


            # Wait for and get all the elements currently in the sidebar
            #elements = await self.page.wait_for_selector(selector, state="attached", timeout=wait_time * 1000)
            elements = await self.page.query_selector_all(selector)


            for node in elements:
                # Get the innerHTML of the element
                inner_html = await node.inner_html()

                # Extract data and add to all_data
                data = await self._extract_heading_and_href_from_toc_node(node)
                all_data.append(data)

                # Find the expand button
                expand_button = await node.query_selector('button.toc-item-expand')

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

    async def _extract_heading_and_href_from_toc_node(self, node: ElementHandle) -> dict[str, str]:
        """
        Extract and return the heading and href data from a toc node.

        Args:
            node (ElementHandle): The ToC node from which to extract the data
        Returns:
            dict: A dictionary containing the heading and href data
        """
        try:
            # Find the anchor element within the node
            anchor = await node.query_selector('a')
            
            if anchor:
                # Get the href attribute
                href = await anchor.get_attribute('href')
                
                # Get the text content (label)
                label = await anchor.text_content()
                
                return {
                    'heading': label.strip() if label else None,
                    'href': href
                }
            else:
                logger.warning("No anchor element found in the toc node")
                return {'heading': None, 'href': None}
        except Exception as e:
            logger.exception(f"Error extracting heading and href: {e}")
            return {'heading': None, 'href': None}
        
    