

Program: scrape_the_law

1.	Problem definition
2.	High-level architecture
3.	Data structures
4.	Algorithms
5.	Function/method signatures
6.	Error handling
7.	Testing strategy
8.	Code organization
9.	Naming conventions
10.	External dependencies
11.	Performance considerations
12.	Scalability
13.	Security considerations
14.	Documentation needs
15.	User interface (if applicable)

# Problem Definition
Objective: Retrieve, store, and maintain text of all local legal codes in the US related to an input datapoint.
Languages: Python, MySQL
Inputs:
•   A string of a single, specific datapoint name (e.g. "sales tax").
•   Input validation
•   Ensure the input is a non-empty string, containing only alphanumeric characters and spaces, maximum 100 characters.
•   Support for non-English inputs (e.g. impuestos, Steuer)
•   Potential for adding a list of datapoints in the future.
Outputs: A SQL database of all local legal codes in the US related to the input datapoint. The ultimate variable “pg_content” is in plain text. The database schema is listed below.

# Database "socialtoolkit"
## Table 1: 'locations'
|-------------------|---------------------|------|-----|---------|----------------|
| Field             | Type                | Null | Key | Default | Extra          |
|-------------------|---------------------|------|-----|---------|----------------|
| id                | int unsigned        | NO   | PRI | NULL    | auto_increment |
| gnis              | mediumint unsigned  | NO   | MUL | NULL    |                |
| fips              | mediumint unsigned  | NO   | MUL | NULL    |                |
| place_name        | varchar(60)         | NO   |     | NULL    |                |
| state_name        | varchar(32)         | NO   | MUL | NULL    |                |
| class_code        | varchar(2)          | NO   |     | NULL    |                |
| primary_lat_dec   | decimal(9,7)        | NO   |     | NULL    |                |
| primary_long_dec  | decimal(9,7)        | NO   |     | NULL    |                |
| primary_point     | varchar(33)         | NO   |     | NULL    |                |
| state_code        | char(2)             | YES  |     | NULL    |                |
| domain_name       | varchar(50)         | YES  |     | NULL    |                |
|-------------------|---------------------|------|-----|---------|----------------|
### 'locations' Notes:
a. id: insert id 
b. gnis: crosswalk variable, a geographic ID code from the US geological survey that is unique for each incorporated community, county, and state in the US.
c. fips: FIPS code, another geographic identifier. Useful crosswalk for linking data with outside US government data e.g. US Census' ACS.
d. place_name: the full legal name for an incorporated community (e.g. "Town of Corker," "City of Compton," etc.)
e. state_name: full name of the state the incorporated community is in (e.g. California, Arizona, etc.)
f. class_code: an identifier used by the US Census bureau to categorize incorporated communities into "cities" and "counties." 
g. Latitude, longitude, and point coordinates.
h. state_code: two-letter abbreviation of the state (e.g. CA, AZ, etc.)
i. domain_name: an internet domain name used by a community for official business e.g. www.cityofcompton.gov


## Table 2: 'urls'
|-------------|---------------------|------|-----|-------------------|-------------------|
| Field       | Type                | Null | Key | Default           | Extra             |
|-------------|---------------------|------|-----|-------------------|-------------------|
| id          | int                 | NO   | PRI | NULL              | auto_increment    |
| url_hash    | varchar(256)        | NO   | MUL | NULL              |                   |
| query_hash  | varchar(256)        | NO   | MUL | NULL              |                   |
| gnis        | mediumint unsigned  | NO   | MUL | NULL              |                   |
| url         | text                | NO   |     | NULL              |                   |
| ia_url      | text                | YES  |     | NULL              |                   |
| created_at  | timestamp           | NO   |     | CURRENT_TIMESTAMP | DEFAULT_GENERATED |
|-------------|---------------------|------|-----|-------------------|-------------------|
### 'urls' Notes:
a. id: Auto-incrementing primary key
b. url_hash: Unique hash of the gnis and URL for quick lookups
c. query_hash: See Searches.
d. gnis: See Locations.
e. url: The actual URL of the webpage
f. ia_url: Internet Archive URL for the webpage (if available)
g. created_at: Timestamp of when the record was created


## Table 3: 'searches'
|---------------|----------------------|------|-----|-------------------|----------------|
| Field         | Type                 | Null | Key | Default           | Extra          |
|---------------|----------------------|------|-----|-------------------|----------------|
| id            | bigint               | NO   | PRI | NULL              | auto_increment |
| query_hash    | varchar(256)         | NO   |     | NULL              |                |
| gnis          | mediumint unsigned   | NO   | MUL | NULL              |                |
| query_text    | varchar(1028)        | NO   |     | NULL              |                |
| num_results   | mediumint unsigned   | NO   |     | NULL              |                |
| source_site   | varchar(64)          | NO   |     | NULL              |                |
| search_engine | varchar(64)          | NO   |     | NULL              |                |
| time_stamp    | datetime             | NO   |     | NULL              |                |
| created_at    | timestamp            | NO   |     | CURRENT_TIMESTAMP |                |
|---------------|----------------------|------|-----|-------------------|----------------|
### 'searches' Notes:
a. id: Auto-incrementing primary key
b. query_hash: Hash of gnis, query_text, and time_stamp for quick lookups
c. gnis: See Locations.
d. query_text: The actual search query text
e. num_results: Number of results returned by the search
f. source_site: Website or platform where the search was performed
g. search_engine: Name of the search engine used (e.g., Google, Bing)
h. time_stamp: Date and time when the search was performed
i. created_at: Timestamp of when the record was created


## Table 4: 'ia_url_metadata'
|---------------|--------------|------|-----|-------------------|----------------|
| Field         | Type         | Null | Key | Default           | Extra          |
|---------------|--------------|------|-----|-------------------|----------------|
| id            | int          | NO   | PRI | NULL              | auto_increment |
| ia_id         | int          | NO   |     | NULL              |                |
| time_stamp    | datetime     | NO   | MUL | NULL              |                |
| digest        | varchar(32)  | NO   | MUL | NULL              |                |
| mimetype      | varchar(100) | NO   | MUL | NULL              |                |
| http_status   | varchar(4)   | NO   | MUL | NULL              |                |
| url           | text         | NO   |     | NULL              |                |
| domain        | varchar(64)  | NO   |     | NULL              |                |
| created_at    | timestamp    | NO   | MUL | CURRENT_TIMESTAMP |                |
|---------------|--------------|------|-----|-------------------|----------------|
### IA URL Metadata Notes:
a. id: Auto-incrementing primary key
b. ia_id: Internet Archive identifier
c. time_stamp: Date and time of the archived snapshot
d. digest: Unique identifier for the archived content
e. mimetype: MIME type of the archived content
f. http_status: HTTP status code of the archived page
g. url: Original URL of the archived page
h. domain: Domain name of the archived page
i. created_at: Timestamp of when the record was created


## Table 5: 'doc_metadata'
|-------------------|---------------------|------|-----|-------------------|----------------|
| Field             | Type                | Null | Key | Default           | Extra          |
|-------------------|---------------------|------|-----|-------------------|----------------|
| id                | bigint              | NO   | PRI | NULL              | auto_increment |
| url_hash          | varchar(256)        | NO   | UNI | NULL              |                |
| gnis              | mediumint unsigned  | NO   | MUL | NULL              |                |
| doc_type          | varchar(16)         | NO   | MUL | NULL              |                |
| title             | varchar(255)        | YES  |     | NULL              |                |
| doc_creation_date | datetime            | YES  | MUL | NULL              |                |
| saved_in_database | bool                | NO   |     | NULL              |                |
| other_metadata    | json                | YES  |     | NULL              |                |
| created_at        | timestamp           | NO   | MUL | CURRENT_TIMESTAMP |                |
|-------------------|---------------------|------|-----|-------------------|----------------|
### 'doc_metadata' Notes:
a. id: Auto-incrementing primary key
b. url_hash: See Urls.
c. gnis: See locations.
d. doc_type: Type of document (e.g., pdf, xlsx)
e. title: Title of the document
f. doc_creation_date: Date when the document was created
g. saved_in_database: Boolean indicating if the document is saved in the database
h. other_metadata: JSON field for additional metadata
i. created_at: Timestamp of when the record was created

## Table 6: 'doc_content'
|-------------------|--------------------|------|-----|---------|----------------|
| Field             | Type               | Null | Key | Default | Extra          |
|-------------------|--------------------|------|-----|---------|----------------|
| id                | bigint unsigned    | NO   | PRI | NULL    | auto_increment |
| url_hash          | varchar(256)       | NO   | UNI | NULL    |                |
| title             | varchar(255)       | NO   | MUL | NULL    |                |
| pg_num            | smallint unsigned  | NO   |     | NULL    |                |
| pg_content        | longtext           | NO   |     | NULL    |                |
| data_was_cleaned  | bool               | NO   |     | FALSE   |                |
| local_file_path   | varchar(255)       | YES  | MUL | NULL    |                |
|-------------------|--------------------|------|-----|---------|----------------|
### 'doc_content' Notes:
a. id: Auto-incrementing primary key
b. url_hash: See Urls
c. title: Title of the document
d. pg_num: Page number of the content
e. pg_content: Cleaned content of the page
f. data_was_cleaned: Boolean indicating if the content has been cleaned
g. local_file_path: Path to the local file (if applicable)

Constraints:
1.	Data Integrity:
a.	Only primary/official sources (e.g. government websites, contracted law repository websites).
b.	Publicly accessible online.
c.	Text must be archived with a third party (e.g. Internet Archive, Libgen).
2.	Scalability:
a.	Handle and store millions or more pages of text and associated metadata.
b.	Metadata sufficient for rigorous academic citation.
c.	Extensible beyond local laws if needed.
3.	Performance:
a.	Complete full retrieval and cleaning cycle monthly.
b.	Maximum time for retrieval and cleaning: 14 days.
c.	Cost optimization: Minimize cost per character of output text so that the dataset is produced as cheaply as possible.
4.	Reliability: Ensure high dataset size, factual accuracy, and trustworthiness
5.	Maintainability: 
a.	Design for monthly execution and updates.
b.	Extensible to weekly or daily updates if needed.
6.	Robustness:
a.	Input format agnostic (plaintext, html, pdf, etc.)
b.	Rate limiting of scraping and search engine usage.
c.	Overcome data gatekeeping attempts; alert user if unsuccessful.
7.	Output format: Human-readable plaintext.
Scope:
1.	Geographic Coverage:
o	Included: Incorporated communities (local (city, township) and county laws).
o	Excluded: Federal and State-level legal codes, although state-level legal codes may be added in the future.
2.	Timeframe
o	Focus on current year's laws (2024)
o	Quarterly updates for significant changes.
o	Historical laws are currently excluded, but support for them may be added in the future. 
3.	Language
o	Primary focus on English
o	Other languages are currently ignored unless otherwise specified.
o	Architecture allowing for future multilingual support.
4.	Content Completeness
o	Flag and store overlapping or conflicting laws.
o	Flag gaps in data so that they can be found through alternative methods or sources.
5.	Data validation:
o	Take random sample of 385 content pages and qualitatively judge them against the text on the webpage itself.
6.	Error handling: Implement a robust system to flag, log, and report errors.
o	Network Errors
o	Query Errors
o	Cleaning Errors
7.	Legal compliance
o	Respect copyright laws and terms of service. 
o	Implement ethical web scraping practices. 
Data Integrity and Verification:
1.	Implement checksums for downloaded and processed documents.
2.	Cross-reference data with multiple sources where possible.
3.	Periodic audits of random samples.
4.	Version control system for tracking changes in laws over time.
Scalability Plans
1.	Design database schema to accommodate multiple datapoints.
2.	Implement modular code structure for easy addition of new data sources.
3.	Plan for distributed processing capabilities for handling increased load.
Update Process:
1.	Full database refresh: Annually
2.	Incremental updates: Monthly checks for significant law changes
3.	Version control: Maintain historical versions of laws for tracking changes.
4.	Update notifications: Alert system for users when relevant laws are updated by comparing versions gathered over time.
Use cases:
1.	Constructing a dataset of legal codes for extracting legal data by hand or via LLM.
o	Key Metric: Size of Dataset
2.	Legal researchers: Analyze trends in local legislation across jurisdictions.
o	Key Metric: Accuracy of Cleaned Text to Source Text
3.	Legal professionals: Quick reference for local laws on specific topics.
o	Key Metric: Accuracy of Cleaned Text to Source Text
4.	ML researchers: Train models on local legal language and structures.
o	Key Metric: Size
5.	Policy analysts: Compare local laws across different regions.
o	Key Metric: Accuracy of Cleaned Text to Source Text.


High-level architecture

# High-Level Architecture for get_all_local_laws

1. **Input Processing Module**
   - Validates and sanitizes the input datapoint
   - Handles potential future expansion to multiple datapoints 
- Done for the moment as Input.py

2. **Search Engine Interface**
   - Manages API connections to search engines
   - Implements rate limiting and error handling
   - Stores search results and metadata in the ‘locations’ and ‘searches’ tables, respectively
- Already implemented via “google_seach.py” and the “PlaywrightGoogleLinkSearch” class.
- Still needs a little bit but otherwise going well.

3. **Archiving Module**
- Saves search results URLs to the Internet Archive
   - Implements ethical scraping practices and respects robots.txt
- Stores archived url metadata in 'ia_url_metadata' tables
- copy-pasted 


3. **Web Scraping Module**
   - Retrieves URLs from the Internet Archive based on search results
   - Handles different input formats (HTML, PDF, etc.)
   - Stores raw content and metadata in “doc_content” and “doc_metadata” tables, respectively.
- Retrieving URLs is already handled in a separate python program/subprocess “waybackup”

4. **Content Extraction and Cleaning Module**
   - Extracts text from various document formats
   - Cleans and normalizes the extracted text
   - Stores processed content in 'doc_content' table
- Kind of have this already in the files in “webs” folder.

5. **Metadata Management Module**
   - Extracts and manages metadata from documents
   - Stores metadata in 'doc_metadata' table

6. **Database Management System**
   - Handles all database operations (CRUD)
   - Implements database schema and relations
   - Manages data integrity and consistency
- Already implemented as “database.py” with “MySqlDatabase” class.


7. **Update and Version Control Module**
   - Manages full and incremental updates
   - Implements version control for tracking law changes
   - Generates update notifications

8. **Error Handling and Logging System**
   - Centralized error handling for all modules
   - Comprehensive logging for debugging and auditing
- Already implemented as “logger.py” with “Logger” class.

9. **Data Validation and Integrity Module**
   - Implements checksums for downloaded documents
   - Manages periodic audits and cross-referencing

10. **Scalability Management Module**
    - Handles distributed processing (if implemented)
    - Manages resources for increased load
- Unnecessary at the moment, but leave a place for it.

11. **API/Interface Layer**
    - Provides access to the collected data for various use cases
    - Implements security and access control
- Unnecessary, as the data will be accessed via MySQL database by different programs.

12. **Scheduler**
    - Manages the execution of full retrieval cycles and updates
    - Coordinates the operation of other modules

Data Structures

















