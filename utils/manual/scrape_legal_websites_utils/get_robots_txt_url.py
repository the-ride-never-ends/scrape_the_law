from config import LEGAL_WEBSITE_DICT

def get_robots_txt_url(scraper_name: str) -> str:
    """
    Get a robots.txt url path based on a scrapers name 
    """
    scraper_to_url = {
        "MunicodeScraper": LEGAL_WEBSITE_DICT["municode"]["robots_txt"],
        "AmericanLegalScraper": LEGAL_WEBSITE_DICT["american_legal"]["robots_txt"],
        "GeneralCodeScraper": LEGAL_WEBSITE_DICT["general_code"]["robots_txt"],
    }
    if scraper_name not in scraper_to_url:
        raise NotImplementedError(f"Scraper '{scraper_name}' has not been implemented.")
    return scraper_to_url[scraper_name]
