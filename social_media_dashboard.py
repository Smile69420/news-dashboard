import streamlit as st
import pandas as pd
import datetime
import json # To parse hashtags/keywords from DB
import subprocess # To run the rss_reader.py script
import sys # To get the current python interpreter path
import os # To construct file paths
from config import MCCIA_SECTORS # Import from shared config
from supabase import create_client, Client # Supabase client
import numpy as np # Import numpy for np.ndarray
from dotenv import load_dotenv

load_dotenv() # Load environment variables from .env

# Supabase Configuration
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY") # Or an ANON_KEY if you set up RLS for public dashboards

if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
    st.error("Supabase URL or Key not configured. Please check your .env file or Streamlit secrets.")
    st.stop()

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

def load_data_from_db():
    try:
        response = supabase.table('articles').select(
            'url, title, summary, feed_source_name, processed_at, '
            'relevance_justification, category, tweet, instagram_caption, linkedin_post, flares, '
            'hashtags, image_keywords'
        ).eq('is_relevant', True).not_.is_('category', None).not_.eq('category', 'Uncategorized').order(
            'processed_at', desc=True # Ensure 'processed_at' is the correct column name in Supabase
        ).limit(500).execute()

        if hasattr(response, 'data') and response.data:
            df = pd.DataFrame(response.data)
            df['processed_at_date'] = pd.to_datetime(df['processed_at']).dt.date
        else:
            df = pd.DataFrame() # Empty DataFrame if no data or error
    except Exception as e:
        st.error(f"Error loading data from Supabase: {e}")
        df = pd.DataFrame() # Return empty DataFrame on error
    return df

def process_json_field(data_from_row):
    """
    Safely processes a field from a DataFrame row that is expected to contain
    a list (either directly, as a JSON string, or wrapped in a Series/ndarray).
    Returns a list of strings, or an empty list if data is missing/malformed.
    """
    actual_data = data_from_row

    # 1. Handle bytes
    if isinstance(actual_data, bytes):
        try:
            actual_data = actual_data.decode('utf-8')
        except UnicodeDecodeError:
            return []

    # 2. Handle pandas Series/Arrays and NumPy arrays - try to get scalar or return empty
    if isinstance(actual_data, (pd.Series, pd.arrays.NumpyExtensionArray, np.ndarray)):
        if hasattr(actual_data, 'size') and actual_data.size == 1: # If it's a single-element array
            actual_data = actual_data.item() if hasattr(actual_data, 'item') else actual_data[0]
        else: # Multi-element array or empty array in a cell, treat as unprocessable for this function
            return []

    # 3. At this point, actual_data should be a Python scalar (None, NaN, str, list, int, float etc.)
    #    or a Python list.

    # Check for primary missing indicators
    if actual_data is None: # Python None
        return []

    # If it's not a list or string, then check with pd.isna for other scalar missing types
    if not isinstance(actual_data, (list, str)):
        if pd.isna(actual_data): # For np.nan, pd.NA, etc. This is now safe.
            return []
        # If it's some other scalar (e.g., int, float) that's not NA,
        # it will pass through. The subsequent isinstance checks for list/str will handle it
        # (or it will result in an empty list if not list/str).
    
    processed_list = []
    if isinstance(actual_data, list):
        processed_list = [str(item) for item in actual_data if item is not None and not pd.isna(item)]
    elif isinstance(actual_data, str):
        if not actual_data.strip(): # Handle empty string case by returning empty list
            return []
        try:
            parsed = json.loads(actual_data)
            if isinstance(parsed, list):
                processed_list = [str(item) for item in parsed if item is not None and not pd.isna(item)]
        except (json.JSONDecodeError, TypeError):
            pass # Keep processed_list empty
    return processed_list

st.set_page_config(layout="wide", page_title="MCCIA Social Media Content Hub")
st.title("MCCIA News & Social Media Content Hub")

# CSS for Poppins font and scrollable, styled code blocks
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

body, .stApp, .stApp *, /* Apply to body, .stApp, and all children of .stApp */
h1, h2, h3, h4, h5, h6, p, div, span, button, input, textarea, select, option, label, table, th, td, li, a,
.stButton>button, .stTextInput>div>div>input, .stTextArea>label, .stTextArea>div>textarea,
.stSelectbox>div>div, .stMultiSelect>div>div, .stDateInput>div>div,
.stExpander>div>button>div>p /* Expander header text */
{
    font-family: 'Inter', sans-serif !important;
}

div.stCodeBlock pre {
    max-height: 250px; /* Adjust this height as needed */
    overflow-y: auto !important; /* Important to override any existing styles */
    font-family: 'Inter', sans-serif !important; /* Ensure code blocks also use Inter */
    white-space: pre-wrap !important; /* Allow text to wrap within the code block */
    word-break: break-word !important; /* Break long words to prevent horizontal scroll */
}
</style>
""", unsafe_allow_html=True)

st.sidebar.header("Actions")
if st.sidebar.button("🔄 Refresh News Feeds"):
    with st.spinner("Fetching new articles... This may take a few moments."):
        try:
            # Determine the path to the Python interpreter and the script
            python_executable = sys.executable
            # Assumes rss_reader.py is in the same directory as social_media_dashboard.py
            script_dir = os.path.dirname(__file__)
            script_path = os.path.join(script_dir, "rss_reader.py")

            if not os.path.exists(script_path):
                st.sidebar.error(f"Error: rss_reader.py not found at {script_path}")
            else:
                # Run the rss_reader.py script as a subprocess
                process = subprocess.run(
                    [python_executable, script_path],
                    capture_output=True,  # Capture stdout and stderr
                    text=True,            # Decode output as text
                    check=False           # Don't raise exception for non-zero exit codes
                )

                if process.returncode == 0:
                    st.sidebar.success("News feeds refreshed successfully!")
                    # You can uncomment the lines below to show some output if needed
                    # if process.stdout:
                    #     st.sidebar.caption("Output from rss_reader (last 500 chars):")
                    #     st.sidebar.code(process.stdout[-500:])
                else:
                    st.sidebar.error("Error during news feed refresh.")
                    st.sidebar.caption("Error details:")
                    st.sidebar.code(process.stderr if process.stderr else process.stdout)
        except Exception as e:
            st.sidebar.error(f"An exception occurred while trying to refresh: {e}")
    st.rerun() # Rerun the Streamlit app to reload data from DB

df_articles = load_data_from_db()

if df_articles.empty:
    st.warning("No relevant articles found. Please run the `rss_reader.py` script.")
else:
    st.sidebar.header("Content Filters")

    # Sector Filter - Use MCCIA_SECTORS from config for a persistent list
    # We still get unique categories from df_articles to ensure we only show sectors that *actually have content*
    # or we can show all defined MCCIA_SECTORS. Let's show all defined sectors.
    sectors_in_data = sorted(df_articles['category'].unique().tolist())
    # To ensure all MCCIA_SECTORS are options, even if no articles yet:
    # We can decide if we want to show ALL defined sectors or only those with data.
    # For now, let's stick to sectors present in the current data for simplicity,
    # but ensure "All" is an option. If you want all MCCIA_SECTORS to always show:
    # display_sectors = ["All"] + MCCIA_SECTORS
    # However, filtering by a sector with no data will show nothing.
    # A good compromise is to show all defined sectors, and the user understands some might be empty.
    
    # Using all defined MCCIA_SECTORS for the filter options:
    sectors = ["All"] + MCCIA_SECTORS
    selected_sector = st.sidebar.selectbox("Filter by Sector:", sectors)

        # Date Range Filter
    min_date = df_articles['processed_at_date'].min()
    max_date = df_articles['processed_at_date'].max()
        
    selected_date_range = st.sidebar.date_input(
            "Filter by Processing Date:",
            value=(min_date, max_date), # Default to all dates
            min_value=min_date,
            max_value=max_date,
        )

    # Source Filter
    sources = ["All"] + sorted(df_articles['feed_source_name'].unique().tolist())
    selected_sources = st.sidebar.multiselect("Filter by Source(s):", options=sources, default=["All"])

    # Flare Filter
    unique_flares_set = set()
    # Iterate over the 'flares' column directly from the DataFrame
    for flare_data_from_cell in df_articles['flares']: # This iterates over each cell's content
        list_of_flares_for_this_row = process_json_field(flare_data_from_cell)
        for flare_item in list_of_flares_for_this_row:
            unique_flares_set.add(flare_item) # process_json_field already ensures items are strings
    selected_flares = st.sidebar.multiselect("Filter by Flare(s):", options=["All"] + sorted(list(unique_flares_set)), default=["All"])

    # Apply filters
    df_display = df_articles.copy()

    if selected_sector != "All":
            df_display = df_display[df_display['category'] == selected_sector]
        
    if len(selected_date_range) == 2: # Ensure two dates are selected
            df_display = df_display[(df_display['processed_at_date'] >= selected_date_range[0]) & (df_display['processed_at_date'] <= selected_date_range[1])]

    if "All" not in selected_sources and selected_sources: # if "All" is not selected and some sources are
            df_display = df_display[df_display['feed_source_name'].isin(selected_sources)]
    
    if "All" not in selected_flares and selected_flares:
        # Filter rows where the 'flares' list (after parsing from JSON) contains AT LEAST ONE of the selected_flares
        def has_selected_flare(flare_payload_from_row): # flare_payload_from_row is row['flares']
            article_flares_list_for_check = process_json_field(flare_payload_from_row)
            if not article_flares_list_for_check: # Empty list of flares for this article
                return False
            try:
                article_flares_set_for_check = set(article_flares_list_for_check) # Items are already strings
                return not article_flares_set_for_check.isdisjoint(set(selected_flares))
            except TypeError: # Handles cases where items in article_flares_list_for_check might not be hashable (e.g. dicts)
                return False # Or log an error
        df_display = df_display[df_display['flares'].apply(has_selected_flare)]
    st.info(f"Displaying {len(df_display)} articles.")

    for index, row in df_display.iterrows():
        with st.expander(f"{row['processed_at'][:10]} | {row['category']} | {row['title']}"):
            st.markdown(f"**Source:** {row['feed_source_name']}")
            st.markdown(f"**Link:** [{row['url']}]({row['url']})")            

            st.subheader("Generated Tweet")
            st.code(row['tweet'] if pd.notna(row['tweet']) else "", language='text')

            st.subheader("Generated Instagram Caption")
            st.code(row['instagram_caption'] if pd.notna(row['instagram_caption']) else "", language='text')

            st.subheader("Generated LinkedIn Post")
            st.code(row.get('linkedin_post', '') if pd.notna(row.get('linkedin_post')) else "", language='text')

            # Display Flares
            processed_flares_list = process_json_field(row['flares'])
            if processed_flares_list: # Check if list is not empty
                st.markdown(f"**Flares:** " + " ".join([f"`{flare}`" for flare in processed_flares_list]))
            
            st.markdown(f"**Relevance Justification:** {row['relevance_justification']}") # Moved justification lower
            
            st.subheader("Suggested Hashtags")
            processed_hashtags_list = process_json_field(row['hashtags'])
            st.code(' '.join(processed_hashtags_list), language='text')

            st.subheader("Image Keywords")
            processed_keywords_list = process_json_field(row['image_keywords'])
            st.code(', '.join(processed_keywords_list), language='text')

            st.markdown("---")
