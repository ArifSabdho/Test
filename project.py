import scrapy
import re
from scrapy.exporters import XmlItemExporter
from itemadapter import ItemAdapter

# Define the item structure to hold the scraped data
class GithubRepoItem(scrapy.Item):
    """
    Represents a single GitHub repository's scraped information.
    """
    url = scrapy.Field()          # URL of the repository
    about = scrapy.Field()        # Description/About text
    last_updated = scrapy.Field() # Timestamp of the last update
    languages = scrapy.Field()    # Dictionary of languages and percentages
    num_commits = scrapy.Field()  # Number of commits (integer or None)
    repo_name = scrapy.Field()    # Temporary field to hold repo name for 'About' logic

# Define the Spider to perform the scraping
class GithubSpider(scrapy.Spider):
    """
    A Scrapy Spider designed to crawl a GitHub user's repository page,
    extract details about each repository, and follow pagination.
    """
    name = 'github_repositories' # Unique name for the spider

   
    start_urls = ['https://github.com/Duckens03? tab=repositories']

    # Custom settings specific to this spider
    custom_settings = {
        # Configure the XML feed export
        'FEEDS': {
            'repositories.xml': { # Output filename
                'format': 'xml',                # Output format
                'encoding': 'utf8',             # File encoding
                'item_export_kwargs': {
                    'root_element': 'repositories', # Root element tag in XML
                    'item_element': 'repository',   # Tag for each item in XML
                },
                'overwrite': True, # Overwrite the file if it already exists
            },
        },
        # Define item pipelines if more complex data processing/cleaning is needed
        'ITEM_PIPELINES': {
            # Example: 'your_project_name.pipelines.SomePipeline': 300,
        },
        # == Scraping Behavior Settings ==
        # Add a small delay between requests to avoid overwhelming the server
        'DOWNLOAD_DELAY': 1,
        # Limit concurrent requests to the same domain
        'CONCURRENT_REQUESTS_PER_DOMAIN': 8,
        # Enable AutoThrottle to automatically adjust scraping speed based on server load
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_START_DELAY': 1, # Initial delay for AutoThrottle
        'AUTOTHROTTLE_MAX_DELAY': 60, # Maximum delay
        # Optional: Set a user agent to identify your bot
        # 'USER_AGENT': 'My Github Scraper Bot (+http://www.mywebsite.com)',
    }

    def parse(self, response):
        """
        Parses the main repository listing page.
        Identifies each repository entry and yields a request to its individual page.
        Handles pagination to scrape all repository pages.

        Args:
            response: The Scrapy Response object for the repository listing page.

        Yields:
            scrapy.Request: A request for each individual repository page.
            scrapy.Request: A request for the next page of repositories, if it exists.
        """
        self.logger.info(f'Parsing repository list page: {response.url}')

        # Selector for the main list containing all repository entries
        # Note: GitHub's structure might change, requiring selector updates.
        repo_list_selector = '#user-repositories-list'

        # Selector for individual repository 'li' elements within the list
        repo_selector = f'{repo_list_selector} ul > li'

        repositories_found = 0
        for repo in response.css(repo_selector):
            repositories_found += 1
            item = GithubRepoItem()

            # --- Extract Repo Name and URL ---
            # Select the 'a' tag containing the repository name and link
            repo_link_element = repo.css('div.d-inline-block.mb-1 > h3 > a')
            if not repo_link_element:
                self.logger.warning("Could not find repository link element. Skipping repo.")
                continue # Skip this list item if basic structure is missing

            repo_name = repo_link_element.css('::text').get()
            relative_url = repo_link_element.css('::attr(href)').get()

            if not repo_name or not relative_url:
                self.logger.warning("Could not extract repo name or URL. Skipping repo.")
                continue # Skip if essential info is missing

            item['repo_name'] = repo_name.strip() # Store repo name temporarily
            item['url'] = response.urljoin(relative_url) # Construct absolute URL

            # --- Extract About/Description ---
            # Select the 'p' tag containing the description
            about_text = repo.css('div[itemprop="description"] p::text').get() # More specific selector
            if not about_text:
                 # Fallback selector if the primary one fails
                 about_text = repo.css('div.col-10.col-lg-9.d-inline-block > p::text').get()

            # Use strip() to remove leading/trailing whitespace, handle None
            item['about'] = about_text.strip() if about_text else None

            # --- Extract Last Updated Time ---
            # Select the 'relative-time' element which contains the timestamp
            last_updated_text = repo.css('relative-time::attr(datetime)').get()
            item['last_updated'] = last_updated_text # Store as ISO 8601 string

            # --- Yield Request for Detailed Page ---
            # Pass the partially filled item to the next parsing stage via meta
            yield scrapy.Request(
                item['url'],
                callback=self.parse_repo_page,
                meta={'item': item}
            )

        if repositories_found == 0:
             self.logger.warning(f"No repositories found on page: {response.url} using selector: {repo_selector}")

        # --- Handle Pagination ---
        # Find the 'Next' page link
        next_page_selector = 'a.next_page::attr(href)' # Standard Scrapy practice
        next_page = response.css(next_page_selector).get()

        if next_page is not None:
            self.logger.info(f'Following pagination link to: {next_page}')
            # Yield a request to follow the pagination link
            yield response.follow(next_page, self.parse)
        else:
            self.logger.info('No more pages found.')


    def parse_repo_page(self, response):
        """
        Parses the individual repository page to extract detailed information
        like languages and commit count, then applies the 'About' logic.

        Args:
            response: The Scrapy Response object for the individual repository page.

        Yields:
            GithubRepoItem: The fully populated item for the repository.
        """
        # Retrieve the item passed from the previous stage
        item = response.meta['item']
        self.logger.info(f"Parsing individual repo page: {item['url']}")

        # --- Extract Languages ---
        languages = {}
        # Selector targeting the language statistics section in the sidebar
        lang_elements_selector = 'div.Layout-sidebar .BorderGrid-row ul.list-style-none li a span:nth-of-type(1)::text'
        lang_perc_selector = 'div.Layout-sidebar .BorderGrid-row ul.list-style-none li a span:nth-of-type(2)::text'

        lang_names = response.css(lang_elements_selector).getall()
        lang_percs = response.css(lang_perc_selector).getall()

        if lang_names and lang_percs and len(lang_names) == len(lang_percs):
            for name, perc in zip(lang_names, lang_percs):
                 if name and perc: # Ensure both name and percentage are extracted
                     languages[name.strip()] = perc.strip()
            item['languages'] = languages if languages else None # Store dict or None
            self.logger.debug(f"Languages found for {item['repo_name']}: {languages}")
        else:
             item['languages'] = None # Explicitly set to None if no languages found
             self.logger.debug(f"No language stats found for {item['repo_name']}")

        # --- Extract Number of Commits ---
        # Selector targeting the commit count, usually a strong tag within a link
        commit_selector = 'ul.list-style-none a[href*="/commits/"] strong::text'
        # Alternative selector if the primary one fails
        commit_selector_alt = 'ul.list-style-none li:contains("commit") span strong::text'

        commit_count_text = response.css(commit_selector).get()
        if not commit_count_text:
            commit_count_text = response.css(commit_selector_alt).get() # Try alternative

        if commit_count_text:
             # Clean the text (remove commas) and convert to integer
             try:
                 commit_count_cleaned = commit_count_text.strip().replace(',', '')
                 item['num_commits'] = int(commit_count_cleaned)
                 self.logger.debug(f"Commit count found for {item['repo_name']}: {item['num_commits']}")
             except (ValueError, AttributeError) as e:
                 item['num_commits'] = None # Handle cases where conversion fails
                 self.logger.warning(f"Could not convert commit count '{commit_count_text}' to int for {item['repo_name']}: {e}")
        else:
             item['num_commits'] = None # Explicitly set to None if commit count not found
             self.logger.debug(f"No commit count found for {item['repo_name']}")

        # --- Apply Logic for 'About' field (Rule 1) ---
        # Check if the repository is considered "empty" based on lack of languages/commits (Rule 2)
        is_empty_proxy = item['languages'] is None and item['num_commits'] is None

        # If 'about' is empty (None or whitespace) AND the repo is NOT considered empty,
        # use the repo name as the 'about' text.
        current_about = item.get('about')
        if not current_about and not is_empty_proxy:
             item['about'] = item['repo_name'] # Use repo name as fallback description
             self.logger.info(f"Using repo name '{item['repo_name']}' as 'About' text because original was empty and repo is not empty.")

        # --- Final Item Preparation ---
        # Remove the temporary repo_name field before yielding the final item
        # Using ItemAdapter for cleaner field manipulation is good practice
        final_item_adapter = ItemAdapter(item)
        del final_item_adapter['repo_name'] # Remove temporary field

        self.logger.info(f"Finished processing repo: {item['url']}. Yielding item.")
        yield final_item_adapter.item # Yield the final, processed item
