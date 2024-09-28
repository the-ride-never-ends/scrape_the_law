from config import JINJA_API_KEY, JINJA_URL
from logger import Logger
import aiohttp

logger = Logger(logger_name=__name__)

class Cleaner:

    def __init__(self):
        self.jinja_api_key: str = JINJA_API_KEY

    def clean():
        pass

    def clean_html():
        pass

    def  clean_pdf():
        pass

    def clean_txt():
        pass

    async def clean_with_jinja(self, url: str | list[str], session: aiohttp.ClientSession, **kwargs) -> str | list[tuple[int, str, str]]:
        """
        Get a url's text and use Jinja to turn it into an LLM-readable format.
        See: https://jina.ai/reader/
        """
        headers = {
            'Authorization': f'Bearer {self.jinja_api_key}'
        }
        # Get any extra kwargs and append them to the headers dictionary.
        for key, value in kwargs:
            headers[key] = value

        try:
            if isinstance(url, list): # Multiple url route.
                response_text_list = []

                for idx, url in enumerate(url):
                    jinja_url = f"https://r.jina.ai/{url}"
                    async with await session.get(jinja_url, headers=headers, allow_redirects=True, timeout=0.01) as response: 
                        response_text_list.extend(
                            (idx, url, response.text,)
                        )
                return response_text_list 

            else: # Single url route
                jinja_url = f"https://r.jina.ai/{url}"
                async with await session.get(jinja_url, headers=headers, allow_redirects=True, timeout=0.01) as response: 
                    return response.text

        except Exception as e:
            logger.warning(f"Jinja could not parse url '{url}': {e}.")




