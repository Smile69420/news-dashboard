import feedparser
import time
import ssl # Import the ssl module
import os
import json # For storing lists as JSON strings in DB
from bs4 import BeautifulSoup # For cleaning HTML from summaries
from supabase import create_client, Client # Supabase client
from datetime import datetime, timedelta # For date calculations
from dotenv import load_dotenv # Import the library
    

    # Attempt to fix SSL CERTIFICATE_VERIFY_FAILED for feedparser
    # This tries to use certifi's certificates if available
try:
        import certifi
        ssl._create_default_https_context = lambda: ssl.create_default_context(cafile=certifi.where())
        print("INFO: Attempting to use certifi's SSL certificates.")
except ImportError:
        print("WARNING: certifi not found. SSL verification might still fail for some feeds.")
        pass

load_dotenv() # Load environment variables from .env

# Supabase Configuration
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY") # Using service key for server-side script

supabase: Client = None # Initialize to None

# --- Configuration for Data Retention ---
DATA_RETENTION_DAYS = 7 # Delete articles older than 7 days

# 1. Define a dictionary named RSS_FEEDS

# 1. Define a dictionary named RSS_FEEDS
RSS_FEEDS = {
    'GoogleNews_World': 'https://news.google.com/rss?hl=en-US&gl=US&ceid=US:en&topic=w',
    'BBC_News_World': 'http://feeds.bbci.co.uk/news/world/rss.xml',
    'TOI_World': 'https://timesofindia.indiatimes.com/rssfeeds/296589292.cms',
    'TOI_Top_Stories': 'http://timesofindia.indiatimes.com/rssfeedstopstories.cms',
    'TOI_Most_Recent': 'http://timesofindia.indiatimes.com/rssfeedmostrecent.cms',
    'TOI_India': 'http://timesofindia.indiatimes.com/rssfeeds/-2128936835.cms',
    'TOI_Pune': 'http://timesofindia.indiatimes.com/rssfeeds/-2128821991.cms',
    'PIB_Mumbai_Economy': 'https://pib.gov.in/RssMain.aspx?ModId=3&Lang=1&Regid=3', # PIB Mumbai - Economy related
    'PIB_Mumbai_Infrastructure': 'https://pib.gov.in/RssMain.aspx?ModId=14&Lang=1&Regid=3', # PIB Mumbai - Infrastructure
    'Hindustan_Times_Business': 'https://www.hindustantimes.com/feeds/rss/business/rssfeed.xml',
    'Hindustan_Times_Pune': 'https://www.hindustantimes.com/feeds/rss/cities/pune-news/rssfeed.xml',
    'Hindustan_Times_Education': 'https://www.hindustantimes.com/feeds/rss/education/rssfeed.xml',
    'Hindustan_Times_Employment': 'https://www.hindustantimes.com/feeds/rss/education/employment-news/rssfeed.xml',
    'Hindustan_Times_Infographic_Economy': 'https://www.hindustantimes.com/feeds/rss/infographic/economy/rssfeed.xml',
    'Hindustan_Times_Infographic_Industry': 'https://www.hindustantimes.com/feeds/rss/infographic/industry/rssfeed.xml',
    'Hindustan_Times_Infographic_Markets': 'https://www.hindustantimes.com/feeds/rss/infographic/markets/rssfeed.xml',
    'Hindustan_Times_Infographic_Money': 'https://www.hindustantimes.com/feeds/rss/infographic/money/rssfeed.xml',
    'Hindustan_Times_Infographic_Technology': 'https://www.hindustantimes.com/feeds/rss/infographic/technology/rssfeed.xml',
    'Hindustan_Times_Top_News': 'https://www.hindustantimes.com/feeds/rss/top-news/rssfeed.xml'
}

MCCIA_SECTORS = [
    'Agriculture',
    'Foreign Trade',
    'Manufacturing', # More specific than Local Industry
    'MSME', # Micro, Small, and Medium Enterprises
    'Automotive',
    'Tech Innovation',
    'Women Entrepreneurship',
    'Policy Updates', # For specific policy news
    'General Business News' # Catch-all
]


# --- Functions to replace Gemini API calls ---

# Sample keyword lists (these need to be much more comprehensive and tailored)
MCCIA_RELEVANCE_KEYWORDS = [
    # Core MCCIA & Location
    "mccia", "maharashtra chamber", "pune chamber", "maharashtra", "pune",
    "western maharashtra", "marathwada", "vidarbha", # Broader regions if relevant
    # General Business Terms
    "industry", "trade", "commerce", "business", "economy", "economic", "enterprise",
    "sme", "msme", "startup", "entrepreneurship", "innovation",
    "policy", "regulation", "gst", "taxation", "budget",
    "investment", "fdi", "funding", "venture capital",
    "export", "import", "foreign trade", "supply chain", "logistics",
    "manufacturing", "production", "industrial growth", "industrial policy",
    "skill development", "workforce", "employment",
    "infrastructure", "development",
    # Specific MCCIA activities/interests
    "mccia event", "mccia initiative", "mccia report", "mccia survey",
    "business delegation", "trade fair", "exhibition", "conference maharashtra",
    "ease of doing business", "sectoral growth", "economic outlook maharashtra",
    
    "manufacturing plant", "industrial production", "factory", "plant operations", "oem",
    "industrial automation", "production line", "supply chain manufacturing", "lean manufacturing",
    "heavy industry", "light industry", "industrial machinery", "make in india", "make in maharashtra",

    "farm", "farmer", "farming", "crop", "agri", "agriculture policy", "agribusiness",
    "horticulture", "irrigation", "apmc", "msp", "fertilizer", "pesticide",
    "food processing", "dairy", "livestock", "fisheries", "rural development",

    "export", "import", "international trade", "exim", "foreign investment",
    "trade agreement", "fta", "customs", "tariff", "duties", "wto",
    "logistics international", "shipping", "global value chain", "trade balance", "forex",

    "msme", "sme", "small and medium enterprise", "micro enterprise", "small scale industry",
    "msme policy", "sme finance", "msme support", "udyog aadhaar", "msme registration", "startup india",

    "automotive industry", "auto sector", "vehicle manufacturing", "car production", "two-wheeler",
    "commercial vehicle", "electric vehicle", "ev policy", "auto components", "auto ancillary",
    "automobile", "automotive supply chain",

    "technology", "innovation", "startup ecosystem", "r&d", "research and development",
    "it services", "software", "saas", "ai", "ml", "iot", "blockchain", "fintech",
    "biotech", "pharma", "healthtech", "edtech", "deep tech", "incubation", "accelerator", "intellectual property",

    "women entrepreneurs", "female founders", "women in business", "women-led startups", "women empowerment",
    "she leads", "women's economic development", "gender equality business", "women in tech", "women in manufacturing",

    "government policy", "regulatory changes", "policy announcement", "draft policy", "public consultation",
    "government notification", "policy impact", "legislative update", "economic policy", "trade policy",
    "industrial policy", "budget announcement", "tax reform", "gst council",

    "economy", "economic growth", "gdp", "inflation", "fiscal policy", "monetary policy",
    "market trend", "business sentiment", "policy update", "government scheme", "budget allocation",
    "corporate news", "mergers", "acquisitions", "financial results" # if very general
]

MCCIA_SECTOR_KEYWORDS_MAP = {
    'Agriculture': [
        "farm", "farmer", "farming", "crop", "agri", "agriculture policy", "agribusiness",
        "horticulture", "irrigation", "apmc", "msp", "fertilizer", "pesticide",
        "food processing", "dairy", "livestock", "fisheries", "rural development"
    ],
    'Foreign Trade': [
        "export", "import", "international trade", "exim", "foreign investment",
        "trade agreement", "fta", "customs", "tariff", "duties", "wto",
        "logistics international", "shipping", "global value chain", "trade balance", "forex"
    ],
    'Manufacturing': [
        "manufacturing plant", "industrial production", "factory", "plant operations", "oem",
        "industrial automation", "production line", "supply chain manufacturing", "lean manufacturing",
        "heavy industry", "light industry", "industrial machinery", "make in india", "make in maharashtra"
    ],
    'MSME': [
        "msme", "sme", "small and medium enterprise", "micro enterprise", "small scale industry",
        "msme policy", "sme finance", "msme support", "udyog aadhaar", "msme registration", "startup india"
    ],
    'Automotive': [
        "automotive industry", "auto sector", "vehicle manufacturing", "car production", "two-wheeler",
        "commercial vehicle", "electric vehicle", "ev policy", "auto components", "auto ancillary",
        "automobile", "automotive supply chain"
    ],
    'Tech Innovation': [
        "technology", "innovation", "startup ecosystem", "r&d", "research and development",
        "it services", "software", "saas", "ai", "ml", "iot", "blockchain", "fintech",
        "biotech", "pharma", "healthtech", "edtech", "deep tech", "incubation", "accelerator", "intellectual property"
    ],
    'Women Entrepreneurship': [
        "women entrepreneurs", "female founders", "women in business", "women-led startups", "women empowerment",
        "she leads", "women's economic development", "gender equality business", "women in tech", "women in manufacturing"
    ],
    'Policy Updates': [
        "government policy", "regulatory changes", "policy announcement", "draft policy", "public consultation",
        "government notification", "policy impact", "legislative update", "economic policy", "trade policy",
        "industrial policy", "budget announcement", "tax reform", "gst council"
    ],
    'General Business News': [ # Catch-all for broader economic news if not fitting specific categories above
        "economy", "economic growth", "gdp", "inflation", "fiscal policy", "monetary policy",
        "market trend", "business sentiment", "policy update", "government scheme", "budget allocation",
        "corporate news", "mergers", "acquisitions", "financial results" # if very general
    ]
}


def qualify_article_relevance(title: str, summary: str) -> dict:
    """Determines relevance based on keywords."""
    text_to_check = (title + " " + summary).lower()
    for keyword in MCCIA_RELEVANCE_KEYWORDS:
        if keyword.lower() in text_to_check:
            return {'relevant': True, 'justification': f"Keyword '{keyword}' found."}
    return {'relevant': False, 'justification': 'No MCCIA relevant keywords found.'}

def categorize_article_by_keywords(title: str, summary: str) -> str:
    """Categorizes based on keyword matching, returns the most relevant sector."""
    text_to_check = (title + " " + summary).lower()
    sector_scores = {sector: 0 for sector in MCCIA_SECTORS}

    for sector, keywords in MCCIA_SECTOR_KEYWORDS_MAP.items():
        for keyword in keywords:
            if keyword.lower() in text_to_check:
                sector_scores[sector] += 1
    
    # Refined Categorization Logic:
    best_sector_candidate = "Uncategorized"
    highest_score = 0

    # Find the sector with the highest score
    for sector, score in sector_scores.items():
        if score > highest_score:
            highest_score = score
            best_sector_candidate = sector
        elif score == highest_score and score > 0: # Handle ties
            # If current best is "General Business News" and the new tied sector is more specific, prefer the specific one.
            if best_sector_candidate == "General Business News" and sector != "General Business News":
                best_sector_candidate = sector

    if highest_score >= 2:  # Requires at least two keyword matches for a specific category
        return best_sector_candidate
    elif highest_score == 1: # Only one keyword matched
        # If the single match is for "General Business News", it's acceptable.
        # Otherwise, a single keyword for a more specific sector is often too weak.
        return "General Business News" if best_sector_candidate == "General Business News" else "Uncategorized"
    return "Uncategorized" # No keywords matched or score too low
def generate_social_media_templates(title: str, summary: str, category: str, url: str) -> dict:
    """Generates social media content using basic templates."""
    clean_category_hashtag_base = category.replace(" ", "").replace("&", "And") # Sanitize for hashtag

    # Tweet
    tweet_text = f"📢 {category} Update: {title[:120]}... Read more: {url} #MCCIA #{clean_category_hashtag_base}"
    
    # Instagram Caption
    insta_caption = (
        f"📢 MCCIA Update: {category} News!\n\n"
        f"Headline: {title}\n\n"
        f"Summary: {summary[:300]}...\n\n"
        f"Stay informed with MCCIA! Full details at the link in our bio or visit: {url}\n\n"
        f"#MCCIA #{clean_category_hashtag_base} #Pune #Maharashtra #{clean_category_hashtag_base}News #IndustryUpdates"
    )
    
    # LinkedIn Post
    linkedin_post = (
        f"Key Development in {category} for MCCIA Members & Maharashtra's Business Ecosystem:\n\n"
        f"**{title}**\n\n"
        f"{summary[:400]}...\n\n"
        f"This update could have significant implications for businesses in the region. We encourage our members to delve deeper.\n\n"
        f"Read the full article here: {url}\n\n"
        f"What are your perspectives on this? Share your thoughts below.\n\n"
        f"#MCCIA #{clean_category_hashtag_base} #{category.replace(' ', '')}Updates #MaharashtraBusiness #EconomicDevelopment #Pune"
    )

    # Hashtag Generation
    hashtags = ["#MCCIA", f"#{clean_category_hashtag_base}"]
    if category != "General Business News":
        specific_cat_tag = clean_category_hashtag_base
        # Shorten common long words in hashtags
        replacements = {"Entrepreneurship": "Entr", "Innovation": "Tech", "Manufacturing": "Mfg", "Development": "Dev"}
        for old, new in replacements.items():
            specific_cat_tag = specific_cat_tag.replace(old, new)
        hashtags.append(f"#{specific_cat_tag}News")

    common_business_tags = ["#PuneBusiness", "#MaharashtraEconomy", "#IndustryNews", "#BusinessUpdates"]
    title_words = [word for word in title.split() if len(word) > 4 and word.isalnum()]
    if title_words:
        potential_title_hashtag = f"#{title_words[0].capitalize()}"
        if len(potential_title_hashtag) < 20:
             hashtags.append(potential_title_hashtag)
    all_hashtags = list(set(hashtags + common_business_tags))
    
    # Image Keyword Generation
    image_keywords = [category.split(" ")[0]]
    image_keywords.extend([kw for kw in title.split() if len(kw) > 3 and kw.lower() not in ["the", "and", "for", "is", "of"]])
    image_keywords.extend(["business", "Maharashtra", "news update", "industry report"])
    unique_image_keywords = list(set(kw.lower().capitalize() for kw in image_keywords if kw))

    return {
        'tweet': tweet_text[:280], # Ensure tweet length
        'instagram_caption': insta_caption,
        'linkedin_post': linkedin_post,
        'hashtags': all_hashtags[:7], # More hashtags, unique and limited
        'image_keywords': unique_image_keywords[:5], # Unique and limited
        'error': None
    }

# def init_db():
    # This function is no longer needed as the table is created directly in Supabase SQL Editor.
    # We can add a check here to ensure Supabase client is initialized if needed.
    # global supabase
    # if not supabase:
    #     if SUPABASE_URL and SUPABASE_SERVICE_KEY:
    #         supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
    #         print("Supabase client initialized.")
    #     else:
    #         print("ERROR: Supabase URL or Key not configured for init_db.")
    # else:
    #      print("Supabase client already initialized.")

def is_article_processed(url: str) -> bool:
    """
    Checks if an article URL exists in the database.
    Returns True if processed, False otherwise.
    """
    try:
        response = supabase.table('articles').select('url', count='exact').eq('url', url).execute()
        return response.count > 0
    except Exception as e:
        print(f"Error checking if article processed '{url}': {e}")
        return False # Assume not processed on error to allow attempt

def add_new_article_basic(url: str, title: str, summary: str, feed_source_name: str):
    """Adds a new article with basic info if it doesn't exist."""
    try:
        # processed_at and last_updated_at will be set by DB default (TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP)
        data = {
            'url': url,
            'title': title,
            'summary': summary, # This is the cleaned summary
            'feed_source_name': feed_source_name
            # 'last_updated_at': 'now()' # Can be explicitly set or rely on DB default/trigger
        }
        response = supabase.table('articles').insert(data, upsert=False).execute() # upsert=False to avoid overwriting if somehow exists
        
        # Error checking for supabase-py v1.x.x (check response.data and response.error)
        # For v2.x.x, it would raise APIError on failure.
        if hasattr(response, 'data') and response.data:
            # print(f"    Successfully added basic info for: {url}")
            pass
        elif hasattr(response, 'error') and response.error:
            print(f"    Error adding basic info for {url}: {response.error.message}")
        else:
            # This case might indicate an issue or a version of supabase-py where errors are raised
            print(f"    Attempted to add basic info for {url}, but response was not as expected or an error occurred.")

    except Exception as e: # Catching general exception, including APIError from supabase-py v2
        print(f"Database error while adding new article '{url}': {e}")

def update_article_details(url: str, **kwargs):
    """Updates specific fields of an article in the database."""
    if not kwargs:
        return # Nothing to update

    update_data = kwargs.copy()
    update_data['last_updated_at'] = 'now()' # Update last_updated_at timestamp
    try:
        response = supabase.table('articles').update(update_data).eq('url', url).execute()
        # Add error checking for response if needed, similar to insert
    except Exception as e:
        print(f"Database error while updating article '{url}': {e}")

def delete_old_articles(retention_days: int):
    """Deletes articles older than the specified retention period."""
    if not supabase:
        print("ERROR: Supabase client not initialized. Cannot delete old articles.")
        return

    cutoff_date = datetime.utcnow() - timedelta(days=retention_days)
    # Format for Supabase/PostgreSQL timestamp query
    cutoff_timestamp_str = cutoff_date.isoformat()

    print(f"--- Deleting articles older than {retention_days} days (before {cutoff_timestamp_str}) ---")
    try:
        response = supabase.table('articles').delete().lt('processed_at', cutoff_timestamp_str).execute()
        if hasattr(response, 'data') and response.data:
            print(f"Successfully deleted {len(response.data)} old articles.")
        elif hasattr(response, 'error') and response.error:
            print(f"Error deleting old articles: {response.error.message}")
        else:
            print("Old articles deletion executed, but no data returned in response (might mean 0 articles deleted or an issue).")
    except Exception as e:
        print(f"Exception during old articles deletion: {e}")

def delete_single_article(url: str):
    """Deletes a single article by its URL."""
    if not supabase:
        print(f"ERROR: Supabase client not initialized. Cannot delete article: {url}")
        return

    print(f"--- Deleting irrelevant article: {url} ---")
    try:
        response = supabase.table('articles').delete().eq('url', url).execute()
        if hasattr(response, 'data') and response.data:
            print(f"Successfully deleted article: {url}")
        elif hasattr(response, 'error') and response.error:
            print(f"Error deleting article {url}: {response.error.message}")
    except Exception as e:
        print(f"Exception during single article deletion ({url}): {e}")

def fetch_and_print_feeds():
    """
    Fetches, parses, and prints titles and links from RSS feeds defined in RSS_FEEDS.
    Skips articles that have already been processed.
    """
    global supabase
    if not supabase:
        if SUPABASE_URL and SUPABASE_SERVICE_KEY:
            supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
            print("Supabase client initialized for fetching feeds.")
        else:
            print("ERROR: Supabase URL or Key not configured. Exiting feed fetch.")
            return

    # Delete old articles before fetching new ones
    delete_old_articles(DATA_RETENTION_DAYS)

    if not RSS_FEEDS:
        print("No RSS feeds defined. Please add feeds to the RSS_FEEDS dictionary.")
        return

    # 2. Iterate through these RSS feeds
    for source_name, feed_url in RSS_FEEDS.items():
        print(f"\n--- Fetching feed: {source_name} from {feed_url} ---")
        try:
            # Parse the feed URL
            feed = feedparser.parse(feed_url)

            # Check for parsing errors (feedparser often returns a 'bozo' flag for malformed feeds)
            if feed.bozo:
                print(f"Warning: Feed '{source_name}' may be malformed. Bozo exception: {feed.bozo_exception}")

            if not feed.entries:
                print(f"No entries found in feed: {source_name}")
                # Add delay even if no entries or error, to be polite
                time.sleep(1) # 6. Add a small delay
                continue

            # 3. For each feed, iterate through its entries
            print(f"Found {len(feed.entries)} entries in {source_name}:")
            for entry in feed.entries:
                # 4. For each entry, extract and print the article title and link
                title = entry.get('title', 'N/A')
                link = entry.get('link') # This is the URL
                # Attempt to get a summary or description from the feed entry
                summary_html = entry.get('summary', entry.get('description', 'No summary available.'))

                # Clean HTML from summary
                soup = BeautifulSoup(summary_html, "html.parser") # Use the fetched summary_html
                summary = soup.get_text(separator=" ", strip=True)
                if not summary: # If summary was purely HTML and now empty, or originally empty
                    summary = "No textual summary available."

                
                if not link:
                    print(f"  Skipping entry (no link): {title}")
                    continue

                # 5. Before processing an article entry, check if its URL is already processed
                if is_article_processed(link):
                    # print(f"  Skipping (already processed): {title} ({link})") # Optional: for verbose logging
                    continue

                # If new article
                print(f"\n  NEW Article Found: {title}")
                print(f"    Link: {link}")

                # Add basic article info to DB first
                add_new_article_basic(link, title, summary, source_name) # summary is already cleaned

                # Fetch full text using newspaper3k
                try:  # Wrap in a try-except to handle potential issues
                    from newspaper import Article # Keep import local to this try-block if it's the only place used
                    article_parser = Article(link)
                    article_parser.download()
                    article_parser.parse()
                    full_text = article_parser.text
                    # Use full_text if available and substantial, otherwise cleaned summary
                    text_for_analysis = full_text if full_text and len(full_text) > len(summary) else summary
                    update_article_details(link, full_text=full_text)
                except Exception as e:
                    print(f"    Newspaper3k error for {link}: {e}. Falling back to summary for analysis.")
                    text_for_analysis = summary # Fallback to cleaned summary
                
                qualification = qualify_article_relevance(title, text_for_analysis) # Use keyword-based relevance
                print(f"    Keyword Qualification: Relevant - {qualification['relevant']}, Justification - {qualification['justification']}")
                update_article_details(link, 
                                       is_relevant=qualification['relevant'], 
                                       relevance_justification=qualification['justification'])

                if not qualification['relevant']:
                    print(f"    INFO: Article '{title}' deemed irrelevant. Deleting from database.")
                    delete_single_article(link) # Delete the irrelevant article
                else: # Article is relevant
                    print(f"    ACTION: Article '{title}' is relevant. (Further processing can be added here)")
                    # Now categorize the relevant article using keywords
                    category = categorize_article_by_keywords(title, text_for_analysis)
                    print(f"    Category: {category}")
                    # Update category for the relevant article
                    update_article_details(link, category=category)
                    
                    if category != 'Uncategorized':
                        # Generate social media content using templates
                        social_posts = generate_social_media_templates(title, summary, category, link) # Use cleaned summary
                        print(f"      Tweet: {social_posts.get('tweet')}")
                        print(f"      Instagram: {social_posts.get('instagram_caption')[:100]}...") # Print snippet
                        print(f"      Hashtags: {social_posts.get('hashtags')}")
                        print(f"      Image Keywords: {social_posts.get('image_keywords')}")
                        if social_posts.get('error'):
                            print(f"      Social Media Generation Error: {social_posts.get('error')}")
                        
                        update_article_details(link,
                                               tweet=social_posts.get('tweet'),
                                               instagram_caption=social_posts.get('instagram_caption'),
                                               linkedin_post=social_posts.get('linkedin_post'),
                                               hashtags=social_posts.get('hashtags'), # Pass list directly, Supabase client handles JSONB
                                               image_keywords=social_posts.get('image_keywords') # Pass list directly
                                               )
                # No need for a separate mark_article_processed if add_new_article_basic handles the initial insert
                # and is_article_processed checks existence.

        # 5. Include basic error handling
        except Exception as e:
            print(f"Error fetching or parsing feed '{source_name}' at {feed_url}: {e}")

        # 6. Add a small delay (e.g., 1 second) between fetching each feed
        print(f"--- Finished fetching {source_name}. Waiting 1 second... ---")
        time.sleep(1)

if __name__ == "__main__":
    fetch_and_print_feeds()
