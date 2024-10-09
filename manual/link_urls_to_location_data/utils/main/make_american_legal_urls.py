from config import LEGAL_WEBSITE_DICT, US_STATE_CODES

def make_american_legal_urls():
    url = LEGAL_WEBSITE_DICT["american_legal"]["base_url"]
    output = [
        f"{url}{code.lower()}" for code in US_STATE_CODES
    ]
    return output