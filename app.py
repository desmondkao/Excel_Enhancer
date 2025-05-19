import streamlit as st
import pandas as pd
import anthropic
import json
import os
import time
import io
import traceback
from dotenv import load_dotenv
import requests
from typing import Dict, Any, List, Optional, Tuple
import re
import concurrent.futures
from datetime import datetime
import math

# Load environment variables
load_dotenv()

# Set page config
st.set_page_config(
    page_title="AI-Powered Excel Data Augmentation Tool",
    page_icon="🔍",
    layout="wide"
)

# Define debug function early - this avoids potential reference issues
if 'debug_mode' not in st.session_state:
    st.session_state.debug_mode = False

def setup_debug():
    if st.session_state.debug_mode:
        return st.write
    else:
        return lambda *args, **kwargs: None
        
st.debug = setup_debug()

# Define a concurrent processing placeholder for storing results
if 'concurrent_results' not in st.session_state:
    st.session_state.concurrent_results = []

# Function to extract JSON from Claude's response
def extract_json_from_response(text: str) -> str:
    """Extract JSON from Claude's response."""
    # Find content between JSON brackets
    match = re.search(r'(\{.*\})', text, re.DOTALL)
    if match:
        return match.group(1)
    return "{}"

# Function to determine if an email is likely a company email
def is_company_email(email: str) -> bool:
    """Determine if an email is likely a company email rather than personal."""
    if not email or '@' not in email:
        return False
        
    common_personal_domains = [
        'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com', 'icloud.com',
        'mail.com', 'protonmail.com', 'zoho.com', 'yandex.com', 'gmx.com', 'live.com'
    ]
    
    domain = email.split('@')[1].lower()
    return domain not in common_personal_domains

# Function to automatically save files to a local directory
def auto_save_data(df: pd.DataFrame, prefix: str = "auto_save", directory: str = "./data_exports"):
    """
    Automatically save dataframe to a local directory with timestamp.
    
    Args:
        df: Dataframe to save
        prefix: Prefix for the filename
        directory: Directory to save files in
    
    Returns:
        Path to the saved file
    """
    # Create directory if it doesn't exist
    os.makedirs(directory, exist_ok=True)
    
    # Generate timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Create filename with timestamp
    filename = f"{prefix}_{timestamp}.xlsx"
    filepath = os.path.join(directory, filename)
    
    # Save file
    try:
        # Create Excel file
        with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Augmented Data')
        
        return filepath
    except Exception as e:
        st.error(f"Error auto-saving file: {str(e)}")
        return None

# Function to get known financial domains
def get_financial_domain_info() -> Dict[str, Dict[str, str]]:
    """Return a dictionary of known financial domains with company and location info."""
    return {
        "bmo.com": {"company": "Bank of Montreal", "country": "Canada", "city": "Montreal"},
        "usbank.com": {"company": "US Bank", "country": "United States", "city": "Minneapolis"},
        "fmr.com": {"company": "Fidelity Investments", "country": "United States", "city": "Boston"},
        "nepc.com": {"company": "New England Pension Consultants", "country": "United States", "city": "Boston"},
        "jpmorgan.com": {"company": "JPMorgan Chase", "country": "United States", "city": "New York"},
        "gs.com": {"company": "Goldman Sachs", "country": "United States", "city": "New York"},
        "ms.com": {"company": "Morgan Stanley", "country": "United States", "city": "New York"},
        "bofa.com": {"company": "Bank of America", "country": "United States", "city": "Charlotte"},
        "citi.com": {"company": "Citigroup", "country": "United States", "city": "New York"},
        "blackrock.com": {"company": "BlackRock", "country": "United States", "city": "New York"},
        "vanguard.com": {"company": "Vanguard Group", "country": "United States", "city": "Valley Forge"},
        "statestreet.com": {"company": "State Street", "country": "United States", "city": "Boston"},
        "wellsfargo.com": {"company": "Wells Fargo", "country": "United States", "city": "San Francisco"},
        "pnc.com": {"company": "PNC Financial Services", "country": "United States", "city": "Pittsburgh"},
        "schwab.com": {"company": "Charles Schwab", "country": "United States", "city": "San Francisco"},
        "tdbank.com": {"company": "TD Bank", "country": "United States", "city": "Cherry Hill"},
        "rbccm.com": {"company": "RBC Capital Markets", "country": "Canada", "city": "Toronto"}
    }

# Function to search with Tavily
def search_with_tavily(query: str, api_key: str) -> Dict[str, Any]:
    """Use Tavily to search for information with improved error handling."""
    try:
        url = "https://api.tavily.com/search"
        headers = {
            "content-type": "application/json",
            "Authorization": f"Bearer {api_key}"
        }
        payload = {
            "query": query,
            "search_depth": "basic",  # Changed from advanced to basic for fewer potential errors
            "include_answer": True,
            "include_domains": ["wikipedia.org", "crunchbase.com", "linkedin.com", "bloomberg.com", "forbes.com", 
                               "marketwatch.com", "investing.com", "ft.com", "wsj.com", "reuters.com"],
            "max_results": 3  # Reduced for quicker response
        }
        
        # Debug info
        st.debug(f"Sending query to Tavily: {query}")
        
        response = requests.post(url, json=payload, headers=headers)
        
        # Check for specific error codes
        if response.status_code == 432 or response.status_code == 401:
            st.warning(f"Authentication issue with Tavily API (Status Code: {response.status_code}). Check your API key.")
            # Return a structured error response
            return {
                "error": "auth_error",
                "status_code": response.status_code,
                "answer": "",
                "results": []
            }
        
        response.raise_for_status()
        result = response.json()
        st.debug(f"Tavily search successful for: {query}")
        return result
    except requests.exceptions.RequestException as e:
        st.error(f"Network error with Tavily API: {str(e)}")
        return {
            "error": "network_error",
            "answer": "",
            "results": []
        }
    except json.JSONDecodeError:
        st.error("Invalid JSON response from Tavily API")
        return {
            "error": "json_error",
            "answer": "",
            "results": []
        }
    except Exception as e:
        st.error(f"Unexpected error with Tavily search: {str(e)}")
        return {
            "error": "unknown_error",
            "answer": "",
            "results": []
        }

# Function to generate search query based on row data and target column
def generate_search_query(row_data: Dict[str, Any], target_column: str, search_context: str) -> str:
    """Generate a search query based on row data and the target column to be filled."""
    # Extract key information from row data
    entity_name = ""
    
    # Try to find name-related columns
    name_keywords = ["name", "company", "organization", "entity", "business"]
    for col in row_data:
        if any(keyword in col.lower() for keyword in name_keywords) and row_data[col]:
            entity_name = str(row_data[col])
            break
    
    # If no name found, try email domain
    if not entity_name and any("email" in col.lower() for col in row_data):
        for col in row_data:
            if "email" in col.lower() and row_data[col] and "@" in str(row_data[col]):
                email = str(row_data[col])
                domain = email.split('@')[1].split('.')[0] if '@' in email else ""
                if domain and len(domain) > 2:  # Avoid very short domains
                    entity_name = domain
                    break
    
    # If still no name found, use first non-empty string value
    if not entity_name:
        for col in row_data:
            if isinstance(row_data[col], str) and row_data[col].strip():
                entity_name = row_data[col]
                break
            elif pd.notna(row_data[col]):
                entity_name = str(row_data[col])
                break
    
    # Generate search query based on target column and context
    query = f"{entity_name} {target_column} {search_context}".strip()
    return query

def extract_data_with_claude(row_data: Dict[str, Any], context: str, target_column: str, claude_api_key: str, using_tavily: bool = True) -> Any:
    """Extract specific data from context using Claude."""
    
    # Adjust the system prompt based on whether we're using Tavily search results or direct extraction
    if using_tavily:
        system_prompt = f"""
        Extract the {target_column} for the entity from the provided search results.
        Return a JSON with a single key "{target_column}" containing the extracted value. If the information is not available, return null.
        
        Return format:
        {{
            "{target_column}": "Extracted Value or null"
        }}
        
        Focus on extracting accurate, specific information. Be precise and avoid speculation.
        If multiple values are mentioned, choose the most authoritative or recent one.
        """
        
        user_content = f"""
        Entity Information: {json.dumps(row_data)}
        
        Search Results: {context}
        
        Extract the {target_column} value for this entity.
        """
    else:
        # More direct extraction approach when not using Tavily
        system_prompt = f"""
        Based on the entity information provided, determine the most likely value for {target_column}.
        Use your knowledge and the provided entity data to make an educated determination.
        
        Return a JSON with a single key "{target_column}" containing your determination. If you cannot determine a value, return null.
        
        Return format:
        {{
            "{target_column}": "Determined Value or null"
        }}
        
        Consider typical patterns in the data. For example:
        - If determining a country based on an email domain like @bmo.com, recognize that BMO (Bank of Montreal) is headquartered in Canada
        - If determining a country based on city, use the city to find the country (e.g., Montreal is in Canada, Boston is in USA)
        - Email domains can often indicate company location - for example:
          * usbank.com → United States
          * bmo.com → Canada
          * nepc.com → United States (New England Pension Consultants)
          * fmr.com → United States (Fidelity Management & Research)
        
        IMPORTANT: Skip determination on individuals with personal email domains such as gmail.com, yahoo.com, etc.
        Only return NULL if you genuinely cannot make a reasonable determination.
        """
        
        user_content = f"""
        Entity Information: {json.dumps(row_data)}
        
        Determine the {target_column} value for this entity.
        """
    
    try:
        # Create Anthropic client with only the API key parameter
        try:
            # Import inside function to ensure fresh import
            from anthropic import Anthropic
            client = Anthropic(api_key=claude_api_key)
        except Exception as client_error:
            st.debug(f"Error creating Claude client: {str(client_error)}")
            return None
        
        try:
            response = client.messages.create(
                model="claude-3-haiku-20240307",
                max_tokens=1000,
                system=system_prompt,
                messages=[{"role": "user", "content": user_content}]
            )
        except Exception as request_error:
            st.debug(f"Error during Claude API request: {str(request_error)}")
            return None
        
        # Extract response
        try:
            json_str = extract_json_from_response(response.content[0].text)
            result = json.loads(json_str)
            return result.get(target_column)
        except Exception as parse_error:
            st.debug(f"Error parsing Claude response: {str(parse_error)}")
            return None
            
    except Exception as e:
        st.debug(f"Error extracting data with Claude: {str(e)}")
        return None

# Process a single row (for concurrent processing)
def process_row_concurrent(
    args: Tuple
) -> Tuple[int, Dict[str, Any]]:
    """Process a single row concurrently and return results."""
    (
        row_data, 
        idx, 
        target_columns,
        search_contexts,
        claude_api_key, 
        tavily_api_key,
        use_tavily,
        use_claude_direct,
        overwrite_existing,
        skip_non_company_emails
    ) = args
    
    results = {}
    
    # Look for email field
    email_field = None
    email_value = None
    
    for col in row_data:
        if 'email' in col.lower() and pd.notna(row_data[col]):
            email_field = col
            email_value = str(row_data[col])
            break
    
    # Get financial domain information
    financial_domains = get_financial_domain_info()
    
    # Check if we should skip this row (individual without company email)
    if skip_non_company_emails and email_field and email_value:
        # Skip if no email or it's not a company email
        if '@' not in email_value:
            return idx, {}
            
        # Check if this appears to be a personal email
        if not is_company_email(email_value):
            return idx, {}
    
    # Extract domain from email for context if available
    email_domain = None
    company_from_email = None
    known_company_info = None
    
    if email_field and email_value and '@' in email_value:
        email_domain = email_value.split('@')[1] if '@' in email_value else None
        
        # Check if this is a known financial domain
        if email_domain in financial_domains:
            known_company_info = financial_domains[email_domain]
            company_from_email = known_company_info["company"]
        else:
            # Get company name from domain if not in our known list
            if email_domain:
                company_from_email = email_domain.split('.')[0].capitalize()
    
    # Extract existing city data for context if available
    existing_city = None
    for col in row_data:
        if 'city' in col.lower() and pd.notna(row_data[col]) and str(row_data[col]).strip():
            existing_city = str(row_data[col]).strip()
            break
    
    for target_column in target_columns:
        # Skip if value already exists and overwrite is not enabled
        current_value = row_data.get(target_column)
        if not overwrite_existing and pd.notna(current_value) and str(current_value).strip():
            continue
            
        # Check if we already have the value from our known domains
        if known_company_info and target_column.lower() in ["country", "city"]:
            if target_column.lower() == "country" and "country" in known_company_info:
                results[target_column] = known_company_info["country"]
                continue
            elif target_column.lower() == "city" and "city" in known_company_info:
                results[target_column] = known_company_info["city"]
                continue
            
        # Generate search query with additional context
        search_context = search_contexts.get(target_column, "")
        
        # Add email domain to context for better search results
        enhanced_context = search_context
        if email_domain and 'country' in target_column.lower():
            enhanced_context = f"{search_context} {email_domain} headquarters country"
        elif email_domain:
            enhanced_context = f"{search_context} {email_domain}"
            
        # Add existing city to context when searching for country
        if existing_city and 'country' in target_column.lower():
            enhanced_context = f"{enhanced_context} {existing_city}"
            
        # Generate the search query
        search_query = generate_search_query(row_data, target_column, enhanced_context)
        
        tavily_context = ""
        
        # Only use Tavily if enabled
        if use_tavily:
            # Perform search with Tavily
            search_result = search_with_tavily(search_query, tavily_api_key)
            
            # Check for errors in Tavily response
            if "error" not in search_result:
                tavily_context = search_result.get('answer', '')
        
        # If we have search results or using Claude direct is enabled
        if tavily_context or use_claude_direct:
            # Create enhanced row data with domain info
            enhanced_row_data = row_data.copy()
            if email_domain and company_from_email:
                enhanced_row_data['_derived_company_domain'] = email_domain
                enhanced_row_data['_derived_company_name'] = company_from_email
            if existing_city:
                enhanced_row_data['_derived_city'] = existing_city
            
            # If no Tavily results but Claude direct is enabled, use row data as context
            context_for_claude = tavily_context if tavily_context else json.dumps(enhanced_row_data)
            
            # Extract the target data using Claude
            extracted_value = extract_data_with_claude(
                enhanced_row_data, 
                context_for_claude, 
                target_column, 
                claude_api_key,
                use_tavily  # Pass whether we're using Tavily to adjust Claude's approach
            )
            
            # Store extracted data
            if extracted_value:
                results[target_column] = extracted_value
    
    return idx, results

# Function to process a batch of rows
def process_batch(
    df: pd.DataFrame, 
    start_idx: int, 
    end_idx: int, 
    target_columns: List[str],
    search_contexts: Dict[str, str],
    claude_api_key: str, 
    tavily_api_key: str,
    use_tavily: bool,
    use_claude_direct: bool,
    overwrite_existing: bool,
    skip_non_company_emails: bool = True,
    progress_bar: Any = None,
    auto_save: bool = False,
    auto_save_interval: int = 50,  # Save every 50 rows by default
    input_filename: str = "data",
    auto_save_directory: str = "./data_exports",
    use_concurrent: bool = True,
    max_workers: int = 4
) -> pd.DataFrame:
    """Process a batch of rows to augment with AI-generated data."""
    
    # Calculate the actual range of indices to process
    actual_end_idx = min(end_idx, len(df))
    total_rows = actual_end_idx - start_idx
    
    if use_concurrent and total_rows > 1:
        # Create args for concurrent processing
        process_args = []
        for idx in range(start_idx, actual_end_idx):
            row_data = df.iloc[idx].to_dict()
            args = (
                row_data, 
                idx, 
                target_columns,
                search_contexts,
                claude_api_key, 
                tavily_api_key,
                use_tavily,
                use_claude_direct,
                overwrite_existing,
                skip_non_company_emails
            )
            process_args.append(args)
        
        # Calculate appropriate number of workers
        # Not too many to avoid API rate limits, but enough for parallelism
        actual_workers = min(max_workers, total_rows)
        
        # Counter for auto-saving
        rows_processed = 0
        last_save_idx = 0
        
        # Process in chunks to avoid memory issues with very large files
        chunk_size = min(100, total_rows)  # Process in chunks of 100 rows
        for chunk_start in range(0, total_rows, chunk_size):
            chunk_end = min(chunk_start + chunk_size, total_rows)
            chunk_args = process_args[chunk_start:chunk_end]
            
            # Process chunk concurrently
            with concurrent.futures.ThreadPoolExecutor(max_workers=actual_workers) as executor:
                # Submit all tasks
                future_to_idx = {executor.submit(process_row_concurrent, args): i for i, args in enumerate(chunk_args)}
                
                # Process as they complete
                for future in concurrent.futures.as_completed(future_to_idx):
                    idx, results = future.result()
                    
                    # Update the dataframe with results
                    for col, value in results.items():
                        df.at[idx, col] = value
                    
                    # Update counter
                    rows_processed += 1
                    
                    # Update progress bar
                    if progress_bar:
                        progress_bar.progress(rows_processed / total_rows)
                    
                    # Auto-save based on interval
                    if auto_save and (rows_processed % auto_save_interval == 0):
                        auto_save_path = auto_save_data(df, prefix=f"{input_filename}_partial", directory=auto_save_directory)
                        if auto_save_path:
                            # Use st.debug instead of st.info to avoid cluttering the UI
                            st.debug(f"Auto-saved progress to {auto_save_path}")
                            last_save_idx = rows_processed
            
            # Auto-save after each chunk
            if auto_save and last_save_idx < rows_processed:
                auto_save_path = auto_save_data(df, prefix=f"{input_filename}_chunk_{chunk_end}", directory=auto_save_directory)
                if auto_save_path:
                    st.debug(f"Auto-saved chunk to {auto_save_path}")
                    last_save_idx = rows_processed
    else:
        # Sequential processing for small batches or when concurrent is disabled
        rows_processed = 0
        last_save_idx = 0
        
        for idx in range(start_idx, actual_end_idx):
            # Convert row to dict
            row_data = df.iloc[idx].to_dict()
            
            # Process the row using a simplified version of process_row_concurrent
            _, results = process_row_concurrent((
                row_data, 
                idx, 
                target_columns,
                search_contexts,
                claude_api_key, 
                tavily_api_key,
                use_tavily,
                use_claude_direct,
                overwrite_existing,
                skip_non_company_emails
            ))
            
            # Update the dataframe with results
            for col, value in results.items():
                df.at[idx, col] = value
            
            # Update counter
            rows_processed += 1
            
            # Update progress bar
            if progress_bar:
                progress_bar.progress(rows_processed / total_rows)
            
            # Auto-save based on interval
            if auto_save and (rows_processed % auto_save_interval == 0):
                auto_save_path = auto_save_data(df, prefix=f"{input_filename}_partial", directory=auto_save_directory)
                if auto_save_path:
                    st.debug(f"Auto-saved progress to {auto_save_path}")
                    last_save_idx = rows_processed
    
    # Final auto-save at the end if not recently saved
    if auto_save and last_save_idx < rows_processed:
        auto_save_path = auto_save_data(df, prefix=f"{input_filename}_complete", directory=auto_save_directory)
        if auto_save_path:
            st.success(f"Auto-saved final results to {auto_save_path}")
    
    return df

def get_default_context(column_name: str) -> str:
    """Return a default search context based on column name."""
    column_lower = column_name.lower()
    
    contexts = {
        "city": "headquarters location city",
        "country": "headquarters country",
        "founded": "year founded established",
        "ceo": "chief executive officer current",
        "industry": "primary business sector industry",
        "revenue": "annual revenue financial",
        "employees": "number of employees workforce size",
        "market_cap": "market capitalization stock value",
        "website": "official website url",
        "description": "company description what they do business",
        "address": "headquarters address office location",
        "phone": "contact phone number",
    }
    
    # Check for partial matches
    for key, value in contexts.items():
        if key in column_lower:
            return value
    
    return ""

def offer_download_options(df: pd.DataFrame):
    """Offer download options for the processed dataframe."""
    col1, col2 = st.columns(2)
    
    with col1:
        st.download_button(
            label="Download as CSV",
            data=df.to_csv(index=False).encode('utf-8'),
            file_name="augmented_data.csv",
            mime="text/csv"
        )
    
    with col2:
        # Create in-memory Excel file
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Augmented Data')
        excel_data = output.getvalue()
        
        st.download_button(
            label="Download as Excel",
            data=excel_data,
            file_name="augmented_data.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

# Main app function
def main():
    st.title("AI-Powered Excel Data Augmentation Tool")
    
    # Add a warning about Tavily API issues
    st.warning("""
    **Note:** We've noticed some users experiencing 432 Client Error with the Tavily API.
    If you encounter this issue, you can still use this tool with Claude's direct data extraction (no web search).
    """)
    
    # Sidebar for API keys
    st.sidebar.header("API Configuration")
    
    # Get API keys from environment or user input
    claude_api_key = os.environ.get("CLAUDE_API_KEY", "")
    tavily_api_key = os.environ.get("TAVILY_API_KEY", "")
    
    if not claude_api_key:
        claude_api_key = st.sidebar.text_input("Claude API Key", type="password")
    else:
        st.sidebar.success("Claude API Key loaded from .env file ✅")
        
    if not tavily_api_key:
        tavily_api_key = st.sidebar.text_input("Tavily API Key", type="password")
    else:
        st.sidebar.success("Tavily API Key loaded from .env file ✅")
    
    # Add debug mode toggle
    st.session_state.debug_mode = st.sidebar.checkbox("Enable Debug Mode", value=False)
    st.debug = setup_debug()  # Update debug function based on checkbox
    
    # Sidebar for API options
    st.sidebar.header("API Options")
    use_tavily = st.sidebar.checkbox("Use Tavily for web search", value=True, 
                                   help="Disable if you're experiencing Tavily API errors")
    use_claude_direct = st.sidebar.checkbox("Use Claude's direct extraction", value=True,
                                          help="Claude will try to determine values even without web search")
    
    # Add auto-save options to UI
    st.sidebar.header("Auto-Save Options")
    enable_auto_save = st.sidebar.checkbox("Enable Auto-Saving", value=True, 
                                        help="Automatically save progress to prevent data loss")
    auto_save_interval = st.sidebar.number_input("Auto-Save Interval (rows)", 
                                              min_value=10, max_value=1000, value=50,
                                              help="Number of rows to process before auto-saving")
    auto_save_directory = st.sidebar.text_input("Auto-Save Directory", 
                                             value="./data_exports",
                                             help="Directory to save files in")
    
    # Add concurrent processing options
    st.sidebar.header("Performance Options")
    use_concurrent = st.sidebar.checkbox("Enable Concurrent Processing", value=True,
                                      help="Process multiple rows at once for faster results")
    max_workers = st.sidebar.slider("Max Concurrent Workers", min_value=2, max_value=16, value=4,
                                 help="Number of concurrent workers (higher = faster but may hit API limits)")
    
    # File uploader
    uploaded_file = st.file_uploader("Upload Excel file", type=["xlsx", "xls", "csv"])
    
    if uploaded_file is not None:
        try:
            # Determine file type and read accordingly
            file_extension = uploaded_file.name.split(".")[-1].lower()
            
            if file_extension == "csv":
                df = pd.read_csv(uploaded_file)
            else:
                df = pd.read_excel(uploaded_file)
                
            st.success(f"File loaded successfully with {len(df)} rows and {len(df.columns)} columns")
            
            # Display file overview
            st.subheader("File Overview")
            col1, col2, col3 = st.columns(3)
            col1.metric("Rows", len(df))
            col2.metric("Columns", len(df.columns))
            col3.metric("Missing Values", df.isna().sum().sum())
            
            # Settings
            st.header("Settings")
            
            # Column selections
            st.subheader("Select Columns to Augment")
            st.markdown("Choose columns you want to fill with AI-powered searches. For each column, you can provide context for better search results.")
            
            # Dynamically create multiselect for all columns
            target_columns = st.multiselect(
                "Select columns to augment with AI searches",
                options=list(df.columns)
            )
            
            # Context for each selected column
            search_contexts = {}
            if target_columns:
                st.subheader("Search Context")
                st.markdown("For each column, provide context to improve search results (optional)")
                
                for col in target_columns:
                    search_contexts[col] = st.text_input(
                        f"Context for {col}",
                        value=get_default_context(col),
                        help=f"Add search terms to help find accurate {col} information"
                    )
            
            # Processing options
            st.subheader("Processing Options")
            col1, col2 = st.columns(2)
            
            with col1:
                test_mode = st.checkbox("Test Mode (process only a small batch)", value=True)
                test_batch_size = st.number_input("Test Batch Size", min_value=1, max_value=20, value=5)
            
            with col2:
                # Advanced options
                create_if_missing = st.checkbox("Create columns if they don't exist", value=True)
                overwrite_existing = st.checkbox("Overwrite existing values", value=False)
                skip_non_company_emails = st.checkbox("Skip entries without company emails", value=True, 
                                                    help="Skip processing individuals with personal email domains like gmail.com")
            
            # Ensure columns exist if needed
            if create_if_missing:
                for col in target_columns:
                    if col not in df.columns:
                        df[col] = None
            
            # Show sample of data
            st.subheader("Data Preview")
            st.dataframe(df.head())
            
            # Processing button
            process_button = st.button("Process Data")
            
            if process_button:
                if not claude_api_key:
                    st.error("Please provide a Claude API Key")
                elif not tavily_api_key and use_tavily:
                    st.warning("No Tavily API Key provided. Disabling Tavily search.")
                    use_tavily = False
                elif not target_columns:
                    st.error("Please select at least one column to augment")
                elif not (use_tavily or use_claude_direct):
                    st.error("Please enable at least one data source (Tavily or Claude direct extraction)")
                else:
                    # Check if all target columns exist
                    missing_columns = [col for col in target_columns if col not in df.columns]
                    if missing_columns and not create_if_missing:
                        st.error(f"The following columns don't exist in your data: {', '.join(missing_columns)}. Enable 'Create columns if they don't exist' or select different columns.")
                    else:
                        # Store original data for comparison
                        original_df = df.copy()
                        
                        # Extract filename without extension for better auto-save names
                        input_filename = os.path.splitext(uploaded_file.name)[0] if uploaded_file else "data"
                        
                        # Determine batch size
                        batch_size = test_batch_size if test_mode else len(df)
                        
                        # Process batch
                        st.subheader("Processing Data")
                        progress_bar = st.progress(0)
                        
                        # Show auto-save status if enabled
                        if enable_auto_save:
                            st.info(f"Auto-saving enabled. Files will be saved to {auto_save_directory} every {auto_save_interval} rows.")
                            # Create directory if it doesn't exist
                            os.makedirs(auto_save_directory, exist_ok=True)
                        
                        # Show concurrency status
                        if use_concurrent:
                            st.info(f"Concurrent processing enabled with {max_workers} workers.")
                        
                        with st.spinner(f"Processing {'test batch' if test_mode else 'entire dataset'}..."):
                            # Record the start time
                            start_time = time.time()
                            
                            # Process the data
                            processed_df = process_batch(
                                df=df.copy(), 
                                start_idx=0, 
                                end_idx=batch_size, 
                                target_columns=target_columns,
                                search_contexts=search_contexts,
                                claude_api_key=claude_api_key, 
                                tavily_api_key=tavily_api_key,
                                use_tavily=use_tavily,
                                use_claude_direct=use_claude_direct,
                                overwrite_existing=overwrite_existing,
                                skip_non_company_emails=skip_non_company_emails,
                                progress_bar=progress_bar,
                                auto_save=enable_auto_save,
                                auto_save_interval=auto_save_interval,
                                input_filename=input_filename,
                                auto_save_directory=auto_save_directory,
                                use_concurrent=use_concurrent,
                                max_workers=max_workers
                            )
                            
                            # Record the end time and calculate duration
                            end_time = time.time()
                            duration = end_time - start_time
                            
                            # Display the processing time
                            st.success(f"Processing completed in {duration:.2f} seconds!")
                        
                        # Show results
                        st.subheader("Results")
                        
                        if test_mode:
                            # Show comparison of before and after
                            st.markdown("### Before Processing")
                            st.dataframe(original_df.head(batch_size))
                            
                            st.markdown("### After Processing")
                            st.dataframe(processed_df.head(batch_size))
                            
                            # Show changes
                            st.markdown("### Changes Made")
                            changes_count = 0
                            for col in target_columns:
                                if col in processed_df.columns and col in original_df.columns:
                                    col_changes = (processed_df.iloc[:batch_size][col] != original_df.iloc[:batch_size][col]).sum()
                                    changes_count += col_changes
                                    st.text(f"Changes in {col}: {col_changes} rows")
                            
                            st.success(f"Total changes made: {changes_count} cells")
                            
                            # Option to process entire file
                            process_all = st.button("Process Entire File")
                            if process_all:
                                # Process entire file (excluding the already processed test batch)
                                st.subheader("Processing Complete File")
                                full_progress_bar = st.progress(0)
                                
                                # Show auto-save status if enabled
                                if enable_auto_save:
                                    st.info(f"Auto-saving enabled. Files will be saved to {auto_save_directory} every {auto_save_interval} rows.")
                                
                                with st.spinner("Processing remaining data..."):
                                    # Record the start time
                                    start_time = time.time()
                                    
                                    # Process remaining rows
                                    if batch_size < len(df):
                                        processed_df = process_batch(
                                            df=processed_df,
                                            start_idx=batch_size, 
                                            end_idx=len(df), 
                                            target_columns=target_columns,
                                            search_contexts=search_contexts,
                                            claude_api_key=claude_api_key, 
                                            tavily_api_key=tavily_api_key,
                                            use_tavily=use_tavily,
                                            use_claude_direct=use_claude_direct,
                                            overwrite_existing=overwrite_existing,
                                            skip_non_company_emails=skip_non_company_emails,
                                            progress_bar=full_progress_bar,
                                            auto_save=enable_auto_save,
                                            auto_save_interval=auto_save_interval,
                                            input_filename=input_filename,
                                            auto_save_directory=auto_save_directory,
                                            use_concurrent=use_concurrent,
                                            max_workers=max_workers
                                        )
                                    
                                    # Record end time and calculate duration
                                    end_time = time.time()
                                    duration = end_time - start_time
                                    
                                    # Display processing time
                                    st.success(f"Full processing completed in {duration:.2f} seconds!")
                                    
                                # Show full processed results
                                st.success("Full file processed!")
                                st.dataframe(processed_df)
                                
                                # If auto-save is enabled, show the auto-save directory
                                if enable_auto_save:
                                    st.success(f"Final results auto-saved to {auto_save_directory}")
                                    # List the last few saved files
                                    if os.path.exists(auto_save_directory):
                                        files = [f for f in os.listdir(auto_save_directory) if f.startswith(input_filename)]
                                        files.sort(reverse=True)  # Most recent first
                                        if files:
                                            st.info(f"Most recent auto-saved files:")
                                            for i, file in enumerate(files[:5]):  # Show last 5 files
                                                st.code(os.path.join(auto_save_directory, file))
                                
                                # Download options
                                offer_download_options(processed_df)
                        else:
                            # If not in test mode, just show processed file and download option
                            st.dataframe(processed_df)
                            
                            # If auto-save is enabled, show the auto-save directory
                            if enable_auto_save:
                                st.success(f"Results auto-saved to {auto_save_directory}")
                                # List the last few saved files
                                if os.path.exists(auto_save_directory):
                                    files = [f for f in os.listdir(auto_save_directory) if f.startswith(input_filename)]
                                    files.sort(reverse=True)  # Most recent first
                                    if files:
                                        st.info(f"Most recent auto-saved files:")
                                        for i, file in enumerate(files[:5]):  # Show last 5 files
                                            st.code(os.path.join(auto_save_directory, file))
                            
                            # Download options
                            offer_download_options(processed_df)
                        
        except Exception as e:
            st.error(f"Error processing file: {str(e)}")
            if st.session_state.debug_mode:
                st.error(f"Exception details: {traceback.format_exc()}")
    
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.error(f"Fatal error: {str(e)}")
        st.error(traceback.format_exc())