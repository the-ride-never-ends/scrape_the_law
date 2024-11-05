

from database.database import MySqlDatabase


class Sources:
    def __init__(self):
        pass

    @staticmethod
    async def get_search_urls_from_sources():
        async with MySqlDatabase(database="socialtoolkit") as db:
            sources_df = db.query_to_dataframe("""
            SELECT gnis, 
                'municode' AS source, 
                source_municode AS value 
                FROM sources 
            WHERE source_municode IS NOT NULL 
                UNION ALL 
            SELECT 
                gnis, 
                'general_code' AS source, 
                source_general_code AS value 
                FROM sources 
            WHERE source_general_code IS NOT NULL 
                UNION ALL 
            SELECT 
                gnis, 
                'american_legal' AS source, 
                source_american_legal AS value 
                FROM sources 
            WHERE source_american_legal IS NOT NULL 
                UNION ALL 
            SELECT 
                gnis, 
                'code_publishing_co' AS source, 
                source_code_publishing_co AS value 
                FROM sources 
            WHERE source_code_publishing_co IS NOT NULL 
                UNION ALL 
            SELECT 
                gnis, 
                'place_domain' AS source, 
                source_place_domain AS value 
            FROM sources 
            WHERE source_place_domain IS NOT NULL;
            """
            )
        return sources_df
