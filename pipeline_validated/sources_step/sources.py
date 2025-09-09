

from database.database import MySqlDatabase


class Sources:
    """
    Handler for retrieving legal document source URLs from the database.
    
    This class provides methods to fetch legal code source URLs from various
    providers including Municode, General Code, American Legal, Code Publishing Co,
    and place-specific domains.
    """
    
    def __init__(self):
        """
        Initialize the Sources handler.
        
        Args:
            None
        
        Returns:
            None
        
        Raises:
            None
        
        Example:
            >>> sources = Sources()
        """
        pass

    @staticmethod
    async def get_search_urls_from_sources():
        """
        Retrieve legal code source URLs from the database.
        
        Queries the sources table to collect URLs from various legal code providers
        including Municode, General Code, American Legal, Code Publishing Co, and
        place-specific domains. Returns data in a normalized format with source type.
        
        Args:
            None
        
        Returns:
            pd.DataFrame: DataFrame with columns 'gnis', 'source', 'value' containing
                legal code source URLs for each location and provider.
        
        Raises:
            Exception: Database connection or query errors.
        
        Example:
            >>> sources_df = await Sources.get_search_urls_from_sources()
            >>> print(sources_df.columns.tolist())
            ['gnis', 'source', 'value']
            >>> sources_df['source'].unique()
            array(['municode', 'general_code', 'american_legal', 'code_publishing_co', 'place_domain'])
        """
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
