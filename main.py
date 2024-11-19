import asyncio
import sys


import pandas as pd


from pipeline_validated.input_step.input import InputProcessor
from pipeline_validated.sources_step.sources import Sources
from pipeline_validated.query_step.query import SearchQueryGenerator

from pipeline_development.search_step.search import SearchEngine


from pipeline_development.archive_step.archive import SaveToInternetArchive
from pipeline_development.scraper.sites.internet_archive.scrape import ScrapeInternetArchive
from pipeline_development.filter_step.filter import FilterUrls
from pipeline_development.cleaner_step.clean import Cleaner
from pipeline_development.metadata_step.metadata import Meta

from utils.shared.next_step import next_step

from database.database import MySqlDatabase
from config.config import DATAPOINT, RAND_SEED, SEARCH_ENGINE, HEADLESS, USE_API_FOR_SEARCH, SLOW_MO
from logger.logger import Logger

logger = Logger(logger_name=__name__)


async def main():

    next_step("Step 1. Get the input data from the database based on our datapoint.")
    processor = InputProcessor(datapoint=DATAPOINT, rand_seed=RAND_SEED)
    locations_df = await processor.get_initial_dataframe()

    # Output should look like below
    # NOTE Some of the domains here dont't work, or are for malware sites. Motherfucker...
    # main locations_df:
    #     id     gnis               place_name state_code                     domain_name
    # 0  18441  2405150          Town of Andrews         SC   http://www.townofandrews.org/
    # 1  20994  2410786       City of Honeyville         UT  http://www.honeyvillecity.com/
    # 2  19760  2410225    City of Copperas Cove         TX  http://www.copperascovetx.gov/
    # 3  18321  1215391  Borough of Harveys Lake         PA        http://harveyslakepa.us/
    # 4   3036  2412161            City of Vista         CA    https://www.cityofvista.com/
    logger.debug(f"main locations_df:\n{locations_df.head()}")
    logger.info("Step 1 Complete.")


    # # Randomly select 30 rows from locations_df
    # sample_locations_df = locations_df.sample(n=30, random_state=RAND_SEED)
    # logger.debug(f"Randomly sampled 30 locations:\n{sample_locations_df}")

    next_step("Step 2: Make queries based on the input cities.")
    skip = input("Skip step 3? y/n: ")
    if "n" or "N" in skip:
        common_terms = [
                "law", "code", "ordinance", "regulation", "statute",
                "municipal code", "city code", "county code", "local law"
            ]
        sources_df = await Sources().get_search_urls_from_sources()

        generator = SearchQueryGenerator(DATAPOINT, common_terms=common_terms, search_engine=SEARCH_ENGINE)
        queries_df = await generator.make_queries(locations_df, sources_df)
        logger.debug(f"main queries_df:\n{queries_df.head()}")
        logger.info("Step 2 Complete.")
        # Output should look like below
        # 2024-09-21 22:43:34,897 - __main___logger - DEBUG - main.py: 56 - main queries_df:
        #       gnis                                            queries          source
        # 0  2409449  site:https://library.municode.com/tx/childress...        municode
        # 1  2390602  site:https://ecode360.com/ City of Hurricane W...    general_code
        # 2  2412174  site:https://codelibrary.amlegal.com/codes/wal...  american_legal
        # 3   885195           site:https://demarestnj.org/ "sales tax"    place_domain
        # 4  2411145  site:https://library.municode.com/ca/monterey/...        municode


    next_step("Step 3. Search these up on Google and get the URLs.")
    skip = input("Skip step 3? y/n: ")
    if "n" or "N" in skip:
        # NOTE. We might have to use the Google Search API here. Google will probably get wise to this eventually.
        # This will also be pretty slow.
        SKIP_SEARCH = False
        search: SearchEngine = SearchEngine().start_engine(SEARCH_ENGINE, USE_API_FOR_SEARCH, headless=HEADLESS, slow_mo=SLOW_MO)
        urls_df = await search.results(queries_df, skip_seach=SKIP_SEARCH)
        logger.debug(f"main urls_df:\n{urls_df.head()}")
        logger.info("Step 3 Complete.")
    else:
        logger.info("Step 3 skipped.")


    next_step("Step 4. Filter out URLs from search that are obviously bad or wrong.")
    skip = input("Skip step 4? y/n: ")
    if "n" or "N" in skip:
        # NOTE This step will be less and less necessary as queries get more refined.
        url_filter: FilterUrls = FilterUrls()
        filtered_urls_df = url_filter.strain(urls_df)
        get_filtered_urls_from_db = False
    else:
        logger.info("Step 4 skipped.")
        get_filtered_urls_from_db = True


    next_step("Step 5. Check if these links are in the Wayback Machine. If they aren't save them.", stop=True)
    async with MySqlDatabase(database="socialtoolkit") as db:
        if get_filtered_urls_from_db:
            filtered_urls_df = db.async_query_to_dataframe("""
            SELECT DISTINCT url_hash, url, gnis FROM urls
            """)

        db: MySqlDatabase
        ia_saver: SaveToInternetArchive = SaveToInternetArchive()
        ia_urls_df: pd.DataFrame = await ia_saver.check(filtered_urls_df, db)
        ia_urls_df: pd.DataFrame = await ia_saver.save(ia_urls_df, db)


        next_step("Step 6. Scrape the saved urls from the Wayback Machine.")
        scraper = ScrapeInternetArchive(db)
        text_df = scraper.scrape(ia_urls_df)


        next_step("Step 7. Get metadata for the text.")
        # Metadata:
        ### Field ###
        # url_hash
        # gnis: gnis id
        # doc_type: pdf, xlsx, etc.
        # title: What the document's title is, if available.
        # doc_creation_date: When the document was created, if available
        # saved_in_database: Whether or not the document is in the database
        # other_metadata
        meta = Meta()
        metadata_df = meta.data(text_df)
        db.async_insert_by_batch(metadata_df)


        next_step("Step 8. Clean the text and save it to the database")
        clean = Cleaner(db)
        text_df: pd.DataFrame = clean.clean(text_df)
        text_df.to_dict('records') # -> list[dict]
        db.async_insert_by_batch(
            """
            INSERT INTO doc_metadata
            """
        )



    sys.exit(0)


if __name__ == "__main__":
    import os
    base_name = os.path.basename(__file__) 
    program_name = base_name if base_name != "main.py" else os.path.dirname(__file__)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"{program_name} program stopped.")
