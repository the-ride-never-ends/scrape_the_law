import asyncio
import sys


from input import InputProcessor
from search import SearchEngine
from query import SearchQueryGenerator
from archive import SaveToInternetArchive
from scrape import GetFromInternetArchive

from utils.shared.next_step import next_step

from database import MySqlDatabase
from config import DATAPOINT, RAND_SEED, SEARCH_ENGINE
from logger import Logger

logger = Logger(logger_name=__name__)

SKIP = True
HEADLESS = False
SLOW_MO = 100

async def main():

    logger.info("Step 1. Get the input data from the database based on our datapoint.")
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

    next_step(step=2)
    logger.info("Step 2: Make queries based on the input cities.")
    # Since we haven't made the query maker class, we'll just use an example.
    common_terms = [
            "law", "code", "ordinance", "regulation", "statute",
            "municipal code", "city code", "county code", "local law"
        ]


    generator = SearchQueryGenerator(DATAPOINT, common_terms=common_terms, search_engine=SEARCH_ENGINE)
    queries_df = await generator.make_queries(locations_df)
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


    next_step(step=3)
    # Step 3. Search these up on Google and get the links.
    # NOTE. We might have to use the Google Search API here. Google will probably get wise to this eventually.
    # This will also be pretty slow.
    search: SearchEngine = SearchEngine().start_engine(SEARCH_ENGINE, headless=HEADLESS, slow_mo=SLOW_MO)
    urls_df = await search.results(queries_df)
    logger.debug(f"main urls_df:\n{urls_df.head()}")
    logger.info("Step 3 Complete.")


    next_step(step=4, stop=True)
    # Step 4. Check if these links are in the Wayback Machine. If they aren't save them.
    async with await MySqlDatabase as db:
        ia_saver = SaveToInternetArchive(db)
        ia_links_df = await ia_saver.save(urls_df)
        logger.info("Step 4 Complete.")



    next_step(step=5)
    # Step 5. Scrape the results from the Wayback Machine using the Waybackup program.
    async with await MySqlDatabase as db:
        ia_getter = GetFromInternetArchive(db)
        text_df = ia_getter.get(ia_links_df)


    next_step(step=6)
    # Step 6. Clean the text and save it to the database
    import clean


    next_step(step=7)
    # Step 7. Get metadata from the text: 


    sys.exit(0)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("scrape_the_law program stopped.")
