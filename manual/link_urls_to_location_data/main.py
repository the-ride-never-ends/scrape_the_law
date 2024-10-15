"""
Link place URLs from legal websites to ID information from table 'locations'
"""
import asyncio
import os
import re
import time


import pandas as pd
from selenium import webdriver

from pathlib import Path
import sys
parent_dir = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(parent_dir))

from Matcher import Matcher
from database import MySqlDatabase
from config import OUTPUT_FOLDER
from logger import Logger
logger = Logger(logger_name=__name__)



from utils.shared.next_step import next_step
from utils.shared.return_s_percent import return_s_percent


from utils.manual.link_urls_to_location_data.main.make_urls import make_urls
from utils.manual.link_urls_to_location_data.main.get_urls import get_urls
from utils.shared.save_to_csv import save_to_csv
from utils.manual.link_urls_to_location_data.main.save_urls_to_csv import save_urls_to_csv
from utils.manual.link_urls_to_location_data.main.merge_csv_files import merge_csv_files
from utils.manual.link_urls_to_location_data.main.load_from_csv import load_from_csv
from utils.manual.link_urls_to_location_data.main.make_csv_file_path_with_cwd import make_csv_file_path_with_cwd
from utils.manual.link_urls_to_location_data.main.pivot_df_from_long_to_wide import pivot_df_from_long_to_wide




am_legal_file = os.path.join(os.getcwd(), "american_legal_results.csv")
municode_file = os.path.join(os.getcwd(), "municode_results.csv")
general_code_file = os.path.join(os.getcwd(), "general_code_results.csv")


LEGAL_WEBSITE_DICT = {
    "american_legal": {
        "base_url": "https://codelibrary.amlegal.com/regions/",
        "target_class": "browse-link roboto",
        "wait_in_seconds": 5,
        "robots_txt": "https://codelibrary.amlegal.com/robots.txt",
        "source": "american_legal",
    },
    "municode": {
        "base_url": "https://library.municode.com/",
        "target_class": "index-link",
        "target_xpath": '//*[starts-with(@id, "code-toc-node-drawer")]',
        "wait_in_seconds": 15,
        "robots_txt": "https://municode.com/robots.txt",
        "source": "municode",
    },
    "general_code" : {
        "base_url": "https://www.generalcode.com/source-library/?state=",
        "target_class": "codeLink",
        "wait_in_seconds": 0,
        "robots_txt": "https://www.generalcode.com/robots.txt",
        "source": "general_code",
    },
}


def get_state_code_from_url(source_dict: dict[str, str]) -> dict[str, str|None]:
    """
    Extract the state code from the 'url' key in source_dict, and append it under key 'state_code'

    Args:
        source_dict (dict[str, str]): A dictionary containing information about a source.
            Expected keys are 'url', 'href', 'text', and 'source'.
    Returns:
        The input dictionary with an additional 'state_code' key.
            The value of 'state_code' is either thte two-letter state code or None if not found.
    Example:
        >>> source_dict = {'url': 'https://www.generalcode.com/source-library/?state=VA',
                        'href': 'https://ecode360.com/WA1232',
                        'text': 'Warren County',
                        'source': 'general_code'}
        >>> return get_state_code_from_url(source_dict)
        {'url': 'https://www.generalcode.com/source-library/?state=VA',
            'href': 'https://ecode360.com/WA1232',
            'text': 'Warren County',
            'source': 'general_code',
            'state_code': 'VA'}
    """
    source = source_dict['source']
    href = source_dict['href']
    url = source_dict['url']
    match source:
        case "municode":
            pattern = r'/([a-z]{2})(?:/|$)' # Ex: https://library.municode.com/in
        case "american_legal":
            pattern = r'/([a-z]{2})(?:/|$)' # Ex: https://codelibrary.amlegal.com/regions/al
        case source if source in "general_code" or "code_publishing_co":
            pattern = r'state=([A-Za-z]{2})(?:&|$)' # Ex: https://www.generalcode.com/source-library/?state=MO
        case _:
            logger.warning(f"Href '{href}' has an unknown source '{source}'")
    match = re.search(pattern, url.lower())

    if match:
        state_code = match.group(1).upper()
        #logger.debug(f"Found match for URL {href}: {state_code}")
        source_dict['state_code'] = state_code 
    else:
        logger.warning(f"No match found for URL '{href}'")
        logger.debug(f"url: {url}\nhref: {href}")
        source_dict['state_code'] = None
    return source_dict



def get_source_from_url_and_href(source_dict: dict[str, str]
                         ) -> dict[str, str]:
    if "amlegal" in source_dict['url']:
        source_dict["source"] = "american_legal"
    elif "municode" in source_dict['url']:
        source_dict["source"] = "municode"
    else:
        if "codepublishing" in source_dict['href']:
            source_dict["source"] = "code_publishing_co"
        else:
            source_dict["source"] = "general_code"
    return source_dict



async def main():

    logger.info("Step 1. Make American Legal URLs.",f=True)
    source = "american_legal"
    am_legal_urls = make_urls(source)
    logger.info("Made American legal URLs.")


    next_step("Step 2. Scrape American Legal URLs and save to csv.", stop=True)
    file = make_csv_file_path_with_cwd(source)
    if not os.path.exists(am_legal_file):
        am_legal_results = await get_urls(am_legal_urls, source=source)
        logger.info(f"Got {len(am_legal_results)} URLs. Saving to csv...")
        save_to_csv(am_legal_results, file)
    else:
        logger.info("Already got URLs from American Legal.")


    # NOTE Vermont, Virginia, Washington are the only holdouts.
    next_step("Step 3. Make Municode URLs")
    source = "municode"
    municode_urls = make_urls(source)
    logger.info("Made Municode URLs.")


    # NOTE Municode is a bitch with downloading these all at once, but just keep running it over and over. You'll get them all eventually.
    next_step("Step 4. Scrape Municode URLs and save to csv.")
    file = make_csv_file_path_with_cwd(source)
    if not os.path.exists(file):
        driver = webdriver.Chrome()
        municode_results = await get_urls(municode_urls, driver=driver, source=source)
        assert len(municode_results) == 50, f"We haven't gotten all the states yet! We've only got {len(municode_results)}"
        save_urls_to_csv(municode_results, file)
    else:
        logger.info("Already got URLs from Municode.")


    next_step("Step 5. Make General Code URLs.") 
    source = "general_code"
    gc_urls = make_urls(source)
    logger.info("Made General Code URLs.")


    next_step("Step 6. Scrape General Code URLs and save to csv.") 
    file = make_csv_file_path_with_cwd(source)
    if not os.path.exists(file):
        driver = webdriver.Chrome()
        gc_results = await get_urls(gc_urls, source=source, driver=driver)
        save_urls_to_csv(gc_results, file)
    else:
        logger.info(f"Already got URLs from {source}.")


    next_step("Step 7. Check if we got all CSV files for all source sites. If we do, merge and save them to a CSV.")
    source = "source"
    merge_csv_files(source) # NOTE path logic is already inside it.


    next_step("Step 8. Attach sources and state_code to sources.")
    sources = load_from_csv(f"{source}_results.csv")
    fixed_sources = [
        get_state_code_from_url(get_source_from_url_and_href(source)) for source in sources
    ]


    next_step("Step 9. Concatenate URL with href if href starts with '/', then turn it into a pandas Dataframe.")
    sources_with_fixed_href = [
        {**dic, 'href': dic['url'] + dic['href'] if dic['href'].startswith('/') else dic['href']}
        for dic in fixed_sources
    ]
    sources_df = pd.DataFrame.from_dict(sources_with_fixed_href)
    logger.debug(f"sources_df: {sources_df.head()}")


    next_step("Step 10. Get locations data from MySQL database")
    async with MySqlDatabase(database="socialtoolkit") as db:
        location_df = await db.async_query_to_dataframe("""
            SELECT gnis, place_name, class_code, state_code FROM locations;
        """)
        #logger.debug(f"locations_df: {location_df.head()}")

        next_step("Step 11. Match href URLs to their associated cities.")
        output_df_path = os.path.join(OUTPUT_FOLDER, "output_df.csv")
        if not os.path.exists(output_df_path):
            matcher = Matcher(sources_df, location_df)
            output_df = matcher.match()
            logger.debug(f"output_head: {output_df.head()}")
            logger.debug(f"output_df: {output_df}")
            time.sleep(30)
        else:
            logger.info(f"Already matched URLs to their places.")
            output_df = load_from_csv(output_df_path)
            output_df = pd.DataFrame.from_records(output_df)
            logger.debug(f"output_head: {output_df.head()}")


        next_step("Step 12. Pivot output_df from long to wide format.")
        output_df = pivot_df_from_long_to_wide(output_df)


        next_step("Step 13. Insert output_df into table 'sources' in the MySQL database.")
        output_list_of_dicts = output_df.to_dict('records') 
        logger.debug(f"output_list_of_dicts\n{output_list_of_dicts[0:10]}")


        update_column_names = ["source_municode", "source_general_code", "source_american_legal", "source_code_publishing_co"]
        update_placeholder = [
            f"{column} = VALUES({column})" for column in update_column_names
        ]
        columns = ["gnis", "place_name", "state_code", "source_municode", "source_general_code", "source_american_legal", "source_code_publishing_co"]
        args = {
            "columns": ", ".join(columns),
            "placeholders": return_s_percent(columns),
            "update": ", ".join(update_placeholder)
        }
        await db.async_execute_sql_command(
            """
            INSERT INTO sources ({columns}) VALUES({placeholders})
            ON DUPLICATE KEY UPDATE
            {update};
            """,
            params=output_list_of_dicts,
            args=args
        )
        logger.info("Insert successful!")

        logger.debug(f"output_df: {output_df.head()}")


    logger.info(f"End {__file__}")
    exit(0)


if __name__ == "__main__":
    import os
    base_name = os.path.basename(__file__) 
    program_name = base_name if base_name != "main.py" else os.path.basename(os.getcwd())
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"{program_name} program stopped.")


