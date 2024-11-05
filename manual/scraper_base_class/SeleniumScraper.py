import asyncio
import os
import time
import traceback
from typing import Any, AsyncGenerator, Never

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

from utils.manual.scrape_legal_websites_utils.extract_urls_using_javascript import extract_urls_using_javascript
from utils.manual.scrape_legal_websites_utils.fetch_robots_txt import fetch_robots_txt
from utils.manual.scrape_legal_websites_utils.parse_robots_txt import parse_robots_txt 
from utils.manual.scrape_legal_websites_utils.can_fetch import can_fetch

from config.config import LEGAL_WEBSITE_DICT, CONCURRENCY_LIMIT, OUTPUT_FOLDER

from database.database import MySqlDatabase

from logger.logger import Logger
logger = Logger(logger_name=__name__)

class SeleniumScraper:

    def __init__(self,
                 robots_txt_url: str=None,
                 user_agent: str="*",
                 **driver_options):
        self.robots_txt_url: str = robots_txt_url
        self.user_agent: str = user_agent
        self.driver_options = driver_options

        self.semaphore: asyncio.Semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
        self.driver: webdriver.Chrome = None
        self.robot_rules: dict[str, dict[str, Any]] = {}

        self.site_dict: dict = None
        self.scrape_url_length: int = None
        self.source: str = self.site_dict['source']
        self.base_url: str = self.site_dict['base_url']
        self.target_class: str = self.site_dict['target_class']
        self.robots_txt_url: str = robots_txt_url or self.site_dict['robots_txt']

    def type_check_site_dict(self, child_class_name: str, robots_txt_url: str=None) -> None:
        if self.site_dict:
            if not self.site_dict['robots_txt']:
                if not robots_txt_url:
                    logger.error(f"LEGAL_WEBSITE_DICT robots.txt entry missing for {child_class_name}.")
                    raise ValueError(f"LEGAL_WEBSITE_DICT robots.txt entry missing for {child_class_name}.")
                else:
                    logger.warning(f"LEGAL_WEBSITE_DICT robots.txt entry missing for {child_class_name}. Defaulting to input robots_txt_url...")
        else:
            raise ValueError(f"LEGAL_WEBSITE_DICT entry missing for {child_class_name}..")

    def get_robot_rules(self, robots_txt_url) -> None:
        robots_txt = fetch_robots_txt(robots_txt_url)
        rules: dict[str,dict[str|Any]] = parse_robots_txt(robots_txt)
        self.robot_rules = rules

    def _load_driver(self) -> None:
        chrome_options = Options()
        for key, value in self.driver_options.items():
            chrome_options.add_argument(f"--{key}={value}")
        service = Service()
        self.driver = webdriver.Chrome(service=service, options=chrome_options)

    def _close_driver(self) -> None:
        if self.driver:
            self.driver.quit()
            self.driver = None

    @classmethod
    def start(cls, robots_txt_url, user_agent, **driver_options) -> 'SeleniumScraper':
        instance = cls(robots_txt_url, user_agent=user_agent, **driver_options)
        instance._load_driver()
        instance.get_robot_rules(instance.robots_txt_url)
        return instance

    def close(self) -> None:
        self._close_driver()

    def __enter__(self) -> 'SeleniumScraper':
        self._load_driver()
        self.get_robot_rules(self.robots_txt_url)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def _open_page(self, url: str) -> None:
        self.driver.get(url)
        WebDriverWait(self.driver, 30).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )

    def _fetch_urls_from_page(self, url: str) -> list[dict[str,str]] | list[dict[Never]]:
        url_dict_list = [{"href":None, "text": None}]
        try:
            self._open_page(url)
            url_dict_list = extract_urls_using_javascript(self.driver, self.source)
        except TimeoutException as e:
            logger.info(f"url '{url}' timed out. Returning empty dict list...")
            logger.debug(e)
        except Exception as e:
            logger.info(f"url '{url}' caused an unexpected exception: {e} ")
            traceback.print_exc()
        return url_dict_list

    def _respectful_fetch(self, url: str) -> list[dict[str,str]] | list[dict[Never]]:
        fetch, delay = can_fetch(url, self.robot_rules)
        if not fetch:
            logger.warning(f"Cannot scrape URL '{url}' as it's disallowed in robots.txt")
            return [{"href":None, "text": None}]
        else:
            time.sleep(delay)
            return self._fetch_urls_from_page(url)

    def scrape(self, url: str) -> list[dict[str,str]] | None:
        return self._respectful_fetch(url)

    def _build_url(self, state_code: str) -> str:
        pass

    def check_url_length(self, scrape_url: str):
        if len(scrape_url) == self.scrape_url_length:
            logger.warning(f"scrape_url is not {self.scrape_url_length} characters, but {len(scrape_url)}")
            logger.debug(f"url : {scrape_url}")
            traceback.print_exc()
            raise ValueError(f"scrape_url is not {self.scrape_url_length} characters, but {len(scrape_url)}")
        else:
            logger.debug(f"{self.source} scrape_url built successfully.")
            return scrape_url

    def build_urls(self, locations_df: pd.DataFrame) -> list[dict[str,str]]:
        state_codes_df = locations_df.drop_duplicates(subset=['state_code'])
        state_url_dict_list = [
            {"state_code": row.state_code, "state_url": self._build_url(row.state_code)}
            for row in state_codes_df.itertuples()
        ]
        logger.info("Created state_code URLs for General Code",f=True)
        return state_url_dict_list

    def _save_output_df_to_csv(self, dic: dict) -> dict:
        state_code, state_url, result = tuple(dic.keys())
        output_filename = f"{state_code}_{self.source}.csv"
        output_path = os.path.join(OUTPUT_FOLDER, output_filename)

        if result:
            logger.info(f"Got {len(result)} results for URL {state_url}. ")
            logger.debug(f"dic for {state_url}\n{dic}", f=True)

            try:
                output_df: pd.DataFrame = pd.json_normalize(dic, "result", ["state_code", "state_url", "href", "text"])
                logger.debug(f"output_df\n{output_df.head()}",f=True)
                output_df.to_csv(output_path)
                logger.info(f"{output_filename} saved to output folder successfully.")
            except Exception as e:
                logger.exception(f"Exception while saving {output_filename} to output folder: {e}")
            finally:
                return dic
        else:
            logger.info(f"No results returned for {state_url}.")
            return dic

    def _filter_urls(self, urls: list[dict[str,str]], db: MySqlDatabase) -> list[dict[str,str]]:
        command = """
        SELECT source_municode, source_general_code, source_american_legal, source_code_publishing_co, source_place_domain
        FROM sources
        """
        unprocessed_urls = []
        processed_urls = db.query_to_dataframe(command)
        for url in urls:
            output_filename = f"{url['state_code']}_{self.source}.csv"
            output_path = os.path.join(OUTPUT_FOLDER, output_filename)
            if not os.path.isfile(output_path):
                unprocessed_urls.append(url)
        return unprocessed_urls

    def scrape_all(self, locations_df: pd.DataFrame, urls: list[dict[str,str]], db: MySqlDatabase) -> pd.DataFrame:
        results_url_dict_list = []
        unprocessed_urls = self._filter_urls(urls, db)

        logger.info(f"Getting URLs from {self.source}...")
        for url in unprocessed_urls:
            result = self.scrape(url['state_code'])
            dic = {
                'state_code': url['state_code'],
                'state_url': url['state_url'],
                'result': result
            }
            results_url_dict_list.append(self._save_output_df_to_csv(dic))

        output_df = pd.DataFrame.from_dict(results_url_dict_list)
        logger.debug(f"output_df\n{output_df}",f=True)
        site_df = locations_df.merge(results_url_dict_list, on="state_code", how="inner")
        logger.debug(f"site_df\n{site_df}",f=True)

        site_df = site_df.drop_duplicates(subset=['href'])
        site_df['source'] = self.source

        return site_df
    
