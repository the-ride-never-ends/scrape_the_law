# -*- coding: utf-8 -*-
"""ELM Web Scraping - Google Search API"""
import asyncio
import os
import re
import time
import traceback

from playwright.async_api import (
    async_playwright,
    Page as PlaywrightPage,
    TimeoutError as PlaywrightTimeoutError,
)

from config.config import GOOGLE_CONCURRENCY_LIMIT, GOOGLE_SEARCH_RESULT_TAG, DEBUG_FILEPATH
from utils.query.clean_search_query import clean_search_query
from utils.shared.make_id import make_id
from utils.shared.sanitize_filename import sanitize_filename
from utils.shared.safe_format import safe_format

from utils.shared.limiter_utils.Limiter import Limiter
limiter  = Limiter(GOOGLE_CONCURRENCY_LIMIT)

from .google_search_utils.navigate_to_google import navigate_to_google
from .google_search_utils.perform_google_search import perform_google_search
from .google_search_utils.extract_links import extract_links

from logger.logger import Logger
log_level=10
logger = Logger(logger_name=__name__,log_level=log_level)

_pw_debug_path = "/mnt/e/AI_TEMP/scrape_the_law_debug/"
pw_debug_path = os.path.join(_pw_debug_path, "playwright")

if not os.path.exists(pw_debug_path):
    os.mkdir(pw_debug_path)


class GoogleSearchAPI():
    pass






