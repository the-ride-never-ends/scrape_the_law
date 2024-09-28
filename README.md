
# Program: scrape_the_law

1. Problem definition
2. High-level architecture
3. Data structures
4. Algorithms - TODO
5. Function/method signatures - TODO
6. Error handling - TODO
7. Testing strategy - TODO
8. Code organization - TODO
9. Naming conventions - TODO
10. External dependencies - TODO
11. Performance considerations - TODO
12. Scalability - TODO
13. Security considerations - TODO
14. Documentation needs - TODO

# 1. Problem Definition
Objective: Retrieve, store, and maintain text of all local legal codes in the US related to an input datapoint.
Languages: Python, MySQL, JavaScript
Inputs:
   •   A string of a single, specific datapoint name (e.g. "sales tax").
Input validation:
   •   Ensure the input is a non-empty string, containing only alphanumeric characters and spaces, maximum 100 characters.
   •   Support for non-English inputs (e.g. impuestos, Steuer)
   •   Potential for adding a list of datapoints in the future.
Outputs: A MySQL database of all local legal codes in the US related to the input datapoint. The ultimate variable “pg_content” is in plain text.

## Constraints:
1. Data Integrity:
   • Only primary/official sources (e.g. government websites, contracted law repository websites).
   • Publicly accessible online.
   • Text must be archived with a third party (e.g. Internet Archive, Libgen).
2. Scalability:
   • Handle and store millions or more pages of text and associated metadata.
   • Metadata sufficient for rigorous academic citation.
   • Extensible beyond local laws if needed.
3. Performance:
   • Complete full retrieval and cleaning cycle monthly.
   • Maximum time for retrieval and cleaning: 14 days.
   • Cost optimization: Minimize cost per character of output text so that the dataset is produced as cheaply as possible.
4. Reliability: Ensure high dataset size, factual accuracy, and trustworthiness
5. Maintainability: 
   • Design for monthly execution and updates.
   • Extensible to weekly or daily updates if needed.
6. Robustness:
   •  Input format agnostic (plaintext, html, pdf, etc.)
   •  Rate limiting of scraping and search engine usage.
   •  Overcome data gatekeeping attempts; alert user if unsuccessful.
7. Output format: Human-readable plaintext.


## Scope:
1. Geographic Coverage:
   - Included: Incorporated communities (local (city, township) and county laws).
   - Excluded: Federal and State-level legal codes, although state-level legal codes may be added in the future.
2. Timeframe
   - Focus on current year's laws (2024)
   - Quarterly updates for significant changes.
   - Historical laws are currently excluded, but support for them may be added in the future. 
3. Language
   - Primary focus on English
   - Other languages are currently ignored unless otherwise specified.
   - Architecture allowing for future multilingual support.
4. Content Completeness
   - Flag and store overlapping or conflicting laws.
   - Flag gaps in data so that they can be found through alternative methods or sources.
5. Data validation:
   - Take random sample of 385 content pages and qualitatively judge them against the text on the webpage itself.
6. Error handling: Implement a robust system to flag, log, and report errors.
   - Network Errors
   - Query Errors
   - Cleaning Errors
7. Legal compliance
   - Respect copyright laws and terms of service. 
   - Implement ethical web scraping practices. 


## Data Integrity and Verification:
1. Implement checksums for downloaded and processed documents.
2. Cross-reference data with multiple sources where possible.
3. Periodic audits of random samples.
4. Version control system for tracking changes in laws over time.
Scalability Plans
1. Design database schema to accommodate multiple datapoints.
2. Implement modular code structure for easy addition of new data sources.
3. Plan for distributed processing capabilities for handling increased load.
Update Process:
1. Full database refresh: Annually
2. Incremental updates: Monthly checks for significant law changes
3. Version control: Maintain historical versions of laws for tracking changes.
4. Update notifications: Alert system for users when relevant laws are updated by comparing versions gathered over time.
Use cases:
1. Constructing a dataset of legal codes for extracting legal data by hand or via LLM.
   - Key Metric: Size of Dataset
2. Legal researchers: Analyze trends in local legislation across jurisdictions.
   - Key Metric: Accuracy of Cleaned Text to Source Text
3. Legal professionals: Quick reference for local laws on specific topics.
   - Key Metric: Accuracy of Cleaned Text to Source Text
4. ML researchers: Train models on local legal language and structures.
   - Key Metric: Size
5. Policy analysts: Compare local laws across different regions.
   - Key Metric: Accuracy of Cleaned Text to Source Text.

# 2. High-Level Architecture

1. **Input Processing Module 'input.py'**
   - Validates and sanitizes the input datapoint
   - Handles potential future expansion to multiple datapoints
   - **COMPLETE**

2. **Search Engine Interface 'search.py'**
   - Manages API connections to search engines
   - Implements rate limiting and error handling
   - Stores search results and metadata in the ‘locations’ and ‘searches’ tables, respectively
   - Already implemented via “google_seach.py” and the “PlaywrightGoogleLinkSearch” class.
   - **COMPLETE BUT FUCK GOOGLE**

3. **Query Generator 'query.py'**
   - Generates search engine queries based on input datapoint and location information
   - Implements query formatting and construction
   - Handles query hashing for efficient storage and retrieval
   - Stores generated queries and their metadata in the 'searches' table
   - Provides functionality to retrieve existing queries from the database
   - **COMPLETE BUT NEEDS FINE-TUNING**

4. **Archiving Module 'archive.py'**
   - Saves search result URLs to the Internet Archive
   - Implements ethical scraping practices and respects robots.txt
   - Stores archived url metadata in 'ia_url_metadata' tables

5. **Web Scraping Module 'Waybackup subproccess'**
   - Retrieves URLs from the Internet Archive based on search results
   - Handles different input formats (HTML, PDF, etc.)
   - Stores raw content and metadata in “doc_content” and “doc_metadata” tables, respectively.
   - Retrieving URLs is already handled in a separate python program/subprocess “waybackup”
   - **COMPLETE BUT NEEDS FINE-TUNING**

6. **Content Extraction and Cleaning Module 'clean.py'**
   - Extracts text from various document formats
   - Cleans and normalizes the extracted text
   - Stores processed content in 'doc_content' table
   - Kind of have this already in the files in “webs” folder.

7. **Metadata Management Module 'metadata.py'**
   - Extracts and manages metadata from documents
   - Stores metadata in 'doc_metadata' table

8. **Database Management System 'database.py'**
   - Handles all database operations (CRUD)
   - Implements database schema and relations
   - Manages data integrity and consistency
   - **COMPLETE**

9. **Update and Version Control Module 'update.py'**
   - Manages full and incremental updates
   - Implements version control for tracking law changes
   - Generates update notifications
   - **IN FUTURES FOLDER**

10. **Error Handling and Logging System 'logger.py'**
   - Centralized error handling for all modules
   - Comprehensive logging for debugging and auditing
   - **COMPLETE**

11. **Data Validation and Integrity Module 'validate.py'**
   - Implements checksums for downloaded documents
   - Manages periodic audits and cross-referencing
   - **IN FUTURES FOLDER**

12. **Scalability Management Module 'scale.py'**
   - Handles distributed processing (if implemented)
   - Manages resources for increased load
   - **IN FUTURES FOLDER**

13. **API/Interface Layer**
   - Provides access to the collected data for various use cases
   - Implements security and access control
   - Unnecessary, as the data will be accessed via MySQL database by different programs.
   - This program is essentially backend for Socialtoolkit.

14. **Scheduler 'schedule.py'**
   - Manages the execution of full retrieval cycles and updates
   - Coordinates the operation of other modules
   - **IN FUTURES FOLDER**

# 3. Data Structures

- Uses Pandas dataframes for handling data and manipulation.
- Input/output variables defined in MySQL database schemas.
- Key configs defined in a private and public config yaml.















