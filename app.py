import streamlit as st
import pandas as pd
import anthropic
import json
import os
import time
import io
import traceback
import requests
from typing import Dict, Any, List, Optional, Tuple
import re
import concurrent.futures
from datetime import datetime
import math
from openai import OpenAI

# Set page config
st.set_page_config(
    page_title="CatenaryLM",
    page_icon="⛓️",
    layout="wide"
)

# Define debug function early - this avoids potential reference issues
if 'debug_mode' not in st.session_state:
    st.session_state.debug_mode = False

if 'processed_df' not in st.session_state:
    st.session_state.processed_df = None
if 'test_batch_completed' not in st.session_state:
    st.session_state.test_batch_completed = False
if 'processing_complete' not in st.session_state:
    st.session_state.processing_complete = False

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

# Function to get known financial domains with state information
def get_financial_domain_info() -> Dict[str, Dict[str, str]]:
    """Return a dictionary of known financial domains with company and location info."""
    return {
        "bmo.com": {"company": "Bank of Montreal", "country": "Canada", "city": "Montreal", "state": "Quebec"},
        "usbank.com": {"company": "US Bank", "country": "United States", "city": "Minneapolis", "state": "Minnesota"},
        "fmr.com": {"company": "Fidelity Investments", "country": "United States", "city": "Boston", "state": "Massachusetts"},
        "nepc.com": {"company": "New England Pension Consultants", "country": "United States", "city": "Boston", "state": "Massachusetts"},
        "jpmorgan.com": {"company": "JPMorgan Chase", "country": "United States", "city": "New York", "state": "New York"},
        "gs.com": {"company": "Goldman Sachs", "country": "United States", "city": "New York", "state": "New York"},
        "ms.com": {"company": "Morgan Stanley", "country": "United States", "city": "New York", "state": "New York"},
        "bofa.com": {"company": "Bank of America", "country": "United States", "city": "Charlotte", "state": "North Carolina"},
        "citi.com": {"company": "Citigroup", "country": "United States", "city": "New York", "state": "New York"},
        "blackrock.com": {"company": "BlackRock", "country": "United States", "city": "New York", "state": "New York"},
        "vanguard.com": {"company": "Vanguard Group", "country": "United States", "city": "Valley Forge", "state": "Pennsylvania"},
        "statestreet.com": {"company": "State Street", "country": "United States", "city": "Boston", "state": "Massachusetts"},
        "wellsfargo.com": {"company": "Wells Fargo", "country": "United States", "city": "San Francisco", "state": "California"},
        "pnc.com": {"company": "PNC Financial Services", "country": "United States", "city": "Pittsburgh", "state": "Pennsylvania"},
        "schwab.com": {"company": "Charles Schwab", "country": "United States", "city": "San Francisco", "state": "California"},
        "tdbank.com": {"company": "TD Bank", "country": "United States", "city": "Cherry Hill", "state": "New Jersey"},
        "rbccm.com": {"company": "RBC Capital Markets", "country": "Canada", "city": "Toronto", "state": "Ontario"}
    }

# Geographic validation mappings
def get_geographic_mappings() -> Dict[str, Dict[str, str]]:
    """Return known city-state-country mappings for validation."""
    return {
        'Boston': {'state': 'Massachusetts', 'country': 'United States'},
        'Montreal': {'state': 'Quebec', 'country': 'Canada'},
        'Minneapolis': {'state': 'Minnesota', 'country': 'United States'},
        'Toronto': {'state': 'Ontario', 'country': 'Canada'},
        'New York': {'state': 'New York', 'country': 'United States'},
        'Chicago': {'state': 'Illinois', 'country': 'United States'},
        'Los Angeles': {'state': 'California', 'country': 'United States'},
        'San Francisco': {'state': 'California', 'country': 'United States'},
        'Charlotte': {'state': 'North Carolina', 'country': 'United States'},
        'Pittsburgh': {'state': 'Pennsylvania', 'country': 'United States'},
        'Cherry Hill': {'state': 'New Jersey', 'country': 'United States'},
        'Valley Forge': {'state': 'Pennsylvania', 'country': 'United States'},
        'Washington': {'state': 'District of Columbia', 'country': 'United States'},
        'Atlanta': {'state': 'Georgia', 'country': 'United States'},
        'Miami': {'state': 'Florida', 'country': 'United States'},
        'Dallas': {'state': 'Texas', 'country': 'United States'},
        'Houston': {'state': 'Texas', 'country': 'United States'},
        'Phoenix': {'state': 'Arizona', 'country': 'United States'},
        'Denver': {'state': 'Colorado', 'country': 'United States'},
        'Seattle': {'state': 'Washington', 'country': 'United States'},
        'Vancouver': {'state': 'British Columbia', 'country': 'Canada'},
        'Calgary': {'state': 'Alberta', 'country': 'Canada'},
        'Ottawa': {'state': 'Ontario', 'country': 'Canada'},
        'Arlington': {'state': 'Virginia', 'country': 'United States'},
        'Durham': {'state': 'North Carolina', 'country': 'United States'},
        'Farmington': {'state': 'Connecticut', 'country': 'United States'}
    }

def get_confidence_score(search_result: Dict[str, Any], extracted_value: str) -> float:
    """Calculate confidence score based on search results and extraction quality."""
    if not search_result or "error" in search_result:
        return 0.0
    
    confidence = 0.0
    
    # Base confidence from having search results
    if search_result.get('answer') or search_result.get('content'):
        confidence += 0.3
    
    # Higher confidence for specific, non-vague answers
    if extracted_value and len(str(extracted_value).strip()) > 2:
        confidence += 0.2
    
    # Penalty for vague answers
    vague_terms = ['unknown', 'various', 'multiple', 'unclear', 'not specified']
    if any(term in str(extracted_value).lower() for term in vague_terms):
        confidence -= 0.3
    
    return min(1.0, max(0.0, confidence))

def validate_geographic_consistency_advanced(results: Dict[str, Any], row_data: Dict[str, Any], 
                                           target_columns: List[str],
                                           confidence_scores: Dict[str, float] = None) -> Dict[str, Any]:
    """Advanced geographic validation with confidence-based decisions and override capability."""
    
    if confidence_scores is None:
        confidence_scores = {}
    
    # Get all location data (existing + newly filled)
    all_data = {**row_data, **results}
    
    # Extract current values and identify columns - BUT ONLY FOR TARGET COLUMNS
    location_data = {}
    for col, value in all_data.items():
        # ONLY process columns that are in target_columns
        if col not in target_columns:
            continue
            
        if 'city' in col.lower():
            location_data['city'] = {'value': value, 'column': col, 'confidence': confidence_scores.get(col, 0.0)}
        elif 'state' in col.lower():
            location_data['state'] = {'value': value, 'column': col, 'confidence': confidence_scores.get(col, 0.0)}
        elif 'country' in col.lower():
            location_data['country'] = {'value': value, 'column': col, 'confidence': confidence_scores.get(col, 0.0)}
    
    # Clean up None/null values
    for loc_type in location_data:
        val = location_data[loc_type]['value']
        if not val or str(val).strip() in ['None', 'null', '']:
            location_data[loc_type]['value'] = None
            location_data[loc_type]['confidence'] = 0.0
    
    # Get known mappings for validation
    known_mappings = get_geographic_mappings()
    
    # Strategy 1: Use known financial domains first (highest confidence)
    email_domain = None
    for col, value in row_data.items():
        if 'email' in col.lower() and value and '@' in str(value):
            email_domain = str(value).split('@')[1]
            break
    
    financial_domains = get_financial_domain_info()
    if email_domain and email_domain in financial_domains:
        domain_info = financial_domains[email_domain]
        st.debug(f"Using known financial domain data for {email_domain}")
        
        # Override with high-confidence domain data
        for geo_type in ['country', 'state', 'city']:
            if geo_type in location_data and geo_type in domain_info:
                if location_data[geo_type]['confidence'] < 0.9:  # Only override if not very confident
                    st.debug(f"Overriding {geo_type} with domain data: {domain_info[geo_type]}")
                    results[location_data[geo_type]['column']] = domain_info[geo_type]
                    location_data[geo_type]['value'] = domain_info[geo_type]
                    location_data[geo_type]['confidence'] = 0.95
    
    # Strategy 2: Use geographic validation rules
    city_value = location_data.get('city', {}).get('value')
    if city_value and city_value in known_mappings:
        expected = known_mappings[city_value]
        st.debug(f"Found geographic mapping for city: {city_value}")
        
        # Handle country
        if 'country' in location_data:
            country_col = location_data['country']['column']
            current_country = location_data['country']['value']
            current_confidence = location_data['country']['confidence']
            expected_country = expected['country']
            
            should_override = (
                not current_country or  # Missing
                current_confidence < 0.7 or  # Low confidence
                (current_country != expected_country and current_confidence < 0.8)  # Wrong but not very confident
            )
            
            if should_override:
                st.debug(f"Setting country to {expected_country} for city {city_value} (confidence override)")
                results[country_col] = expected_country
                confidence_scores[country_col] = 0.8  # Geographic validation confidence
                location_data['country']['value'] = expected_country
        
        # Handle state
        if 'state' in location_data:
            state_col = location_data['state']['column']
            current_state = location_data['state']['value']
            current_confidence = location_data['state']['confidence']
            expected_state = expected['state']
            
            should_override = (
                not current_state or  # Missing
                current_confidence < 0.7 or  # Low confidence
                (current_state != expected_state and current_confidence < 0.8)  # Wrong but not very confident
            )
            
            if should_override:
                st.debug(f"Setting state to {expected_state} for city {city_value} (confidence override)")
                results[state_col] = expected_state
                location_data['state']['value'] = expected_state
    
    # Strategy 3: Cross-validate between fields
    # If we have high-confidence country and city, validate state
    if (location_data.get('country', {}).get('confidence', 0) > 0.8 and 
        location_data.get('city', {}).get('confidence', 0) > 0.8 and
        'state' in location_data):
        
        city_val = location_data['city']['value']
        if city_val and city_val in known_mappings:
            expected_state = known_mappings[city_val]['state']
            current_state = location_data['state']['value']
            
            if (not current_state or 
                location_data['state']['confidence'] < 0.6 or
                current_state != expected_state):
                
                st.debug(f"Cross-validating state: setting to {expected_state} based on high-confidence city/country")
                results[location_data['state']['column']] = expected_state
    
    return results

def get_processing_order_optimized(target_columns: List[str]) -> List[str]:
    """Return columns in optimal processing order: country -> state -> city -> others."""
    # Start with country (most general, easiest to determine)
    priority_order = ['country', 'state', 'province', 'region', 'city']
    ordered_columns = []
    remaining_columns = []
    
    # Add geographic columns in order of decreasing generality
    for priority in priority_order:
        for col in target_columns:
            if priority in col.lower() and col not in ordered_columns:
                ordered_columns.append(col)
    
    # Add non-geographic columns
    for col in target_columns:
        if col not in ordered_columns:
            remaining_columns.append(col)
    
    return ordered_columns + remaining_columns

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
            "search_depth": "basic",
            "include_answer": True,
            "include_domains": ["wikipedia.org", "crunchbase.com", "linkedin.com", "bloomberg.com", "forbes.com", 
                               "marketwatch.com", "investing.com", "ft.com", "wsj.com", "reuters.com"],
            "max_results": 3
        }
        
        st.debug(f"Sending query to Tavily: {query}")
        
        response = requests.post(url, json=payload, headers=headers)
        
        if response.status_code == 432 or response.status_code == 401:
            st.warning(f"Authentication issue with Tavily API (Status Code: {response.status_code}). Check your API key.")
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

# Function to search with Perplexity
def search_with_perplexity(query: str, api_key: str) -> Dict[str, Any]:
    """Use Perplexity AI to search for information."""
    try:
        st.debug(f"Sending query to Perplexity: {query}")
        
        client = OpenAI(api_key=api_key, base_url="https://api.perplexity.ai")
        
        messages = [
            {
                "role": "system",
                "content": (
                    "You are a helpful research assistant. Provide accurate, factual information "
                    "based on reliable sources. Focus on providing specific, verifiable details."
                ),
            },
            {
                "role": "user",
                "content": query,
            },
        ]
        
        response = client.chat.completions.create(
            model="sonar",
            messages=messages,
        )
        
        result = {
            "answer": response.choices[0].message.content,
            "content": response.choices[0].message.content,
            "results": [{"content": response.choices[0].message.content}]
        }
        
        st.debug(f"Perplexity search successful for: {query}")
        return result
        
    except Exception as e:
        st.error(f"Error with Perplexity search: {str(e)}")
        return {
            "error": "perplexity_error",
            "answer": "",
            "content": "",
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

def get_improved_system_prompt(target_column: str, using_web_search: bool = True) -> str:
    """Generate improved system prompts with geographic validation."""
    
    # Base format
    base_format = f'''
Return a JSON with a single key "{target_column}" containing the extracted value. If the information is not available, return null.

Return format:
{{
    "{target_column}": "Extracted Value or null"
}}
'''
    
    if using_web_search:
        # For web search results
        if 'country' in target_column.lower():
            return f'''
Extract the {target_column} for the entity from the provided search results.

GEOGRAPHIC VALIDATION RULES:
- If you see a city mentioned in the entity data, ensure the country matches that city's location
- For example: Montreal = Canada, Boston = United States, London = United Kingdom
- Cross-reference city and state information to validate country accuracy
- Prioritize official headquarters information over branch locations

{base_format}

Focus on extracting accurate, specific information. Be precise and avoid speculation.
If multiple locations are mentioned, choose the headquarters or primary location.
'''
            
        elif 'state' in target_column.lower():
            return f'''
Extract the {target_column} (state/province/region) for the entity from the provided search results.

GEOGRAPHIC VALIDATION RULES:
- If you see a city in the entity data, the state MUST match that city's actual location
- Examples: Boston = Massachusetts, Montreal = Quebec, Minneapolis = Minnesota
- Do not guess or use random states - only return a state if you're certain it matches the city
- For international locations, use appropriate regional divisions (provinces, states, etc.)

{base_format}

CRITICAL: If the city doesn't match common knowledge of state locations, return null rather than guessing.
'''
            
        else:
            return f'''
Extract the {target_column} for the entity from the provided search results.
{base_format}
Focus on extracting accurate, specific information. Be precise and avoid speculation.
'''
    
    else:
        # For direct extraction without web search
        if 'country' in target_column.lower():
            return f'''
Based on the entity information provided, determine the most likely value for {target_column}.

GEOGRAPHIC VALIDATION RULES:
- Use city information to determine country (Montreal = Canada, Boston = USA, etc.)
- Use email domains for company headquarters (bmo.com = Canada, usbank.com = USA)
- Cross-validate city, state, and country for consistency
- Known patterns:
  * bmo.com → Canada (Bank of Montreal)
  * usbank.com → United States  
  * fmr.com → United States (Fidelity, Boston)
  * nepc.com → United States (New England Pension Consultants, Boston)

{base_format}

Consider all geographic clues in the data. If city says "Boston", country should be "United States".
Only return null if you genuinely cannot make a reasonable determination.
'''
            
        elif 'state' in target_column.lower():
            return f'''
Based on the entity information provided, determine the most likely value for {target_column}.

GEOGRAPHIC VALIDATION RULES - CRITICAL:
- If city is provided, state MUST match the city's actual location
- Common city-state mappings:
  * Boston = Massachusetts
  * Montreal = Quebec  
  * Minneapolis = Minnesota
  * New York = New York
  * Chicago = Illinois
  * Los Angeles = California
  * Toronto = Ontario
- Do NOT guess states - only return if you're confident about the city-state relationship
- For non-US locations, use appropriate regional divisions (provinces, länder, etc.)

{base_format}

IMPORTANT: Return null rather than guessing if you're not certain about the geographic relationship.
Skip determination on individuals with personal email domains.
'''
        else:
            return f'''
Based on the entity information provided, determine the most likely value for {target_column}.
{base_format}
Consider typical patterns in the data and use logical reasoning.
Only return null if you genuinely cannot make a reasonable determination.
'''

def generate_user_content(row_data: Dict[str, Any], context: str, target_column: str, using_web_search: bool = True) -> str:
    """Generate improved user content with geographic context."""
    
    # Extract key geographic info from row
    city_info = ""
    state_info = ""
    country_info = ""
    
    for col, value in row_data.items():
        if 'city' in col.lower() and value and str(value).strip() and str(value).strip() != 'None':
            city_info = f"City: {value}"
        elif 'state' in col.lower() and value and str(value).strip() and str(value).strip() != 'None':
            state_info = f"State: {value}"
        elif 'country' in col.lower() and value and str(value).strip() and str(value).strip() != 'None':
            country_info = f"Country: {value}"
    
    geographic_context = f"{city_info} {state_info} {country_info}".strip()
    
    if using_web_search:
        return f'''
Entity Information: {json.dumps(row_data)}

Geographic Context: {geographic_context}

Search Results: {context}

Extract the {target_column} value for this entity.

IMPORTANT: Ensure geographic consistency - if extracting state and city is "Boston", 
the state must be "Massachusetts". Cross-validate your answer against known geography.
'''
    else:
        return f'''
Entity Information: {json.dumps(row_data)}

Geographic Context: {geographic_context}

Determine the {target_column} value for this entity.

CRITICAL: Use the geographic context above to ensure consistency. 
For example, if City is "Boston", State must be "Massachusetts" and Country must be "United States".
'''

def extract_data_with_claude_confidence(row_data: Dict[str, Any], context: str, target_column: str, 
                                      claude_api_key: str, using_web_search: bool = True, 
                                      search_result: Dict[str, Any] = None) -> Tuple[Any, float]:
    """Extract data with Claude and return both value and confidence score."""
    
    # Enhanced system prompt with confidence requirements
    if using_web_search and search_result:
        confidence_instruction = """
        CONFIDENCE REQUIREMENTS:
        - Only return a value if you are highly confident (80%+ certainty)
        - If search results are vague, contradictory, or incomplete, return null
        - Prefer returning null over guessing
        - Look for multiple confirming sources in the search results
        """
    else:
        confidence_instruction = """
        CONFIDENCE REQUIREMENTS:
        - Only return a value if you can determine it with high confidence from the entity data
        - Use email domains and existing geographic data as strong indicators
        - If you cannot make a confident determination, return null
        - Do not guess or make assumptions
        """
    
    # Get standard prompts and add confidence instruction
    system_prompt = get_improved_system_prompt(target_column, using_web_search)
    system_prompt += confidence_instruction
    
    user_content = generate_user_content(row_data, context, target_column, using_web_search)
    
    try:
        from anthropic import Anthropic
        client = Anthropic(api_key=claude_api_key)
        
        response = client.messages.create(
            model="claude-3-haiku-20240307",
            max_tokens=1000,
            system=system_prompt,
            messages=[{"role": "user", "content": user_content}]
        )
        
        json_str = extract_json_from_response(response.content[0].text)
        result = json.loads(json_str)
        extracted_value = result.get(target_column)
        
        # Calculate confidence score
        confidence = 0.5  # Base confidence for Claude extraction
        
        if using_web_search and search_result:
            confidence = get_confidence_score(search_result, extracted_value)
        else:
            # Direct extraction confidence
            if extracted_value:
                # Higher confidence for email domain matches
                if any('email' in col.lower() for col in row_data):
                    confidence += 0.3
                # Higher confidence for geographic consistency
                if target_column.lower() in ['country', 'state'] and any('city' in col.lower() for col in row_data):
                    confidence += 0.2
        
        return extracted_value, confidence
        
    except Exception as e:
        st.debug(f"Error extracting data with Claude: {str(e)}")
        return None, 0.0

# Process a single row (for concurrent processing) - OPTIMIZED VERSION
def process_row_concurrent(args: Tuple) -> Tuple[int, Dict[str, Any], Dict[str, float]]:
    """Optimized row processing with confidence-based decisions."""
    (
        row_data, idx, target_columns, search_contexts,
        claude_api_key, search_api_key, search_provider, use_web_search, use_claude_direct,
        overwrite_existing, skip_non_company_emails
    ) = args
    
    results = {}
    confidence_scores = {}
    
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
        if '@' not in email_value:
            return idx, {}, {}
        if not is_company_email(email_value):
            return idx, {}, {}
    
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
    
    # Extract existing location data for context
    existing_city = None
    existing_state = None
    existing_country = None
    
    for col in row_data:
        if 'city' in col.lower() and pd.notna(row_data[col]) and str(row_data[col]).strip() and str(row_data[col]).strip() != 'None':
            existing_city = str(row_data[col]).strip()
        elif 'state' in col.lower() and pd.notna(row_data[col]) and str(row_data[col]).strip() and str(row_data[col]).strip() != 'None':
            existing_state = str(row_data[col]).strip()
        elif 'country' in col.lower() and pd.notna(row_data[col]) and str(row_data[col]).strip() and str(row_data[col]).strip() != 'None':
            existing_country = str(row_data[col]).strip()
    
    # Process columns in optimized order (country first)
    ordered_columns = get_processing_order_optimized(target_columns)
    
    for target_column in ordered_columns:
        # Skip if high-confidence value exists and overwrite is disabled
        current_value = row_data.get(target_column)
        if (not overwrite_existing and current_value and 
            str(current_value).strip() not in ['None', 'null', '']):
            continue
        
        # Check if we already have the value from our known domains
        if known_company_info:
            domain_key = target_column.lower()
            if domain_key in known_company_info:
                results[target_column] = known_company_info[domain_key]
                confidence_scores[target_column] = 0.95  # High confidence for known domains
                st.debug(f"Using known domain data: {target_column}={known_company_info[domain_key]}")
                continue
        
        # Generate search query with enhanced context
        search_context = search_contexts.get(target_column, "")
        
        # Enhanced context generation based on target column and existing data
        enhanced_context = search_context
        if email_domain:
            if 'country' in target_column.lower():
                enhanced_context = f"{search_context} {email_domain} headquarters country"
            elif 'state' in target_column.lower():
                enhanced_context = f"{search_context} {email_domain} state location"
            else:
                enhanced_context = f"{search_context} {email_domain}"
        
        # Add existing geographic data to context
        if existing_city:
            if 'country' in target_column.lower():
                enhanced_context = f"{enhanced_context} {existing_city} country location"
            elif 'state' in target_column.lower():
                enhanced_context = f"{enhanced_context} {existing_city} state province region"
        
        if existing_state and 'country' in target_column.lower():
            enhanced_context = f"{enhanced_context} {existing_state}"
        
        # Generate the search query
        search_query = generate_search_query(row_data, target_column, enhanced_context)
        
        # Perform search and extraction
        web_context = ""
        search_result = {}
        
        if use_web_search:
            if search_provider == "perplexity":
                search_result = search_with_perplexity(search_query, search_api_key)
            else:  # tavily
                search_result = search_with_tavily(search_query, search_api_key)
                
            if "error" not in search_result:
                web_context = search_result.get('answer', '') or search_result.get('content', '')
        
        if web_context or use_claude_direct:
            # Create enhanced row data with domain info and current results
            enhanced_row_data = row_data.copy()
            enhanced_row_data.update(results)  # Include results from previous columns
            
            if email_domain and company_from_email:
                enhanced_row_data['_derived_company_domain'] = email_domain
                enhanced_row_data['_derived_company_name'] = company_from_email
            if existing_city:
                enhanced_row_data['_derived_city'] = existing_city
            if existing_state:
                enhanced_row_data['_derived_state'] = existing_state
            if existing_country:
                enhanced_row_data['_derived_country'] = existing_country
            
            context_for_claude = web_context if web_context else json.dumps(enhanced_row_data)
            
            # Extract with confidence scoring
            extracted_value, confidence = extract_data_with_claude_confidence(
                enhanced_row_data, context_for_claude, target_column, 
                claude_api_key, bool(web_context), search_result
            )
            
            # Only store high-confidence results
            CONFIDENCE_THRESHOLD = 0.6 # Adjustable threshold
            if (extracted_value and confidence >= CONFIDENCE_THRESHOLD and
                str(extracted_value).strip().lower() not in ['null', 'none', '']):
                results[target_column] = extracted_value
                confidence_scores[target_column] = confidence
                st.debug(f"Stored {target_column}={extracted_value} (confidence: {confidence:.2f})")
            else:
                st.debug(f"Rejected {target_column}={extracted_value} (confidence: {confidence:.2f} < {CONFIDENCE_THRESHOLD})")
    
    # Apply advanced geographic validation with confidence scores
    results = validate_geographic_consistency_advanced(results, row_data, target_columns, confidence_scores)
    
    return idx, results, confidence_scores

# Function to process a batch of rows
def process_batch(
    df: pd.DataFrame, 
    start_idx: int, 
    end_idx: int, 
    target_columns: List[str],
    search_contexts: Dict[str, str],
    claude_api_key: str, 
    search_api_key: str,
    search_provider: str,
    use_web_search: bool,
    use_claude_direct: bool,
    overwrite_existing: bool,
    skip_non_company_emails: bool = True,
    progress_bar: Any = None,
    auto_save: bool = False,
    auto_save_interval: int = 50,
    input_filename: str = "data",
    auto_save_directory: str = "./data_exports",
    use_concurrent: bool = True,
    max_workers: int = 4,
    add_confidence_columns: bool = False
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
                search_api_key,
                search_provider,
                use_web_search,
                use_claude_direct,
                overwrite_existing,
                skip_non_company_emails
            )
            process_args.append(args)
        
        # Calculate appropriate number of workers
        actual_workers = min(max_workers, total_rows)
        
        # Counter for auto-saving
        rows_processed = 0
        last_save_idx = 0
        
        # Process in chunks to avoid memory issues with very large files
        chunk_size = min(100, total_rows)
        for chunk_start in range(0, total_rows, chunk_size):
            chunk_end = min(chunk_start + chunk_size, total_rows)
            chunk_args = process_args[chunk_start:chunk_end]
            
            # Process chunk concurrently
            with concurrent.futures.ThreadPoolExecutor(max_workers=actual_workers) as executor:
                # Submit all tasks
                future_to_idx = {executor.submit(process_row_concurrent, args): i for i, args in enumerate(chunk_args)}
                
                # Process as they complete
                for future in concurrent.futures.as_completed(future_to_idx):
                    idx, results, confidence_scores = future.result()
                    
                    # Update the dataframe with results
                    for col, value in results.items():
                        df.at[idx, col] = value
                        
                        # ADD CONFIDENCE COLUMNS if requested
                        if add_confidence_columns and col in confidence_scores:
                            confidence_col = f"{col}_confidence"
                            if confidence_col not in df.columns:
                                df[confidence_col] = None
                            df.at[idx, confidence_col] = round(confidence_scores[col], 2)
                    
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
            row_data = df.iloc[idx].to_dict()
            
            _, results, confidence_scores = process_row_concurrent((
                row_data, 
                idx, 
                target_columns,
                search_contexts,
                claude_api_key, 
                search_api_key,
                search_provider,
                use_web_search,
                use_claude_direct,
                overwrite_existing,
                skip_non_company_emails
            ))
            
            # Update the dataframe with results
            for col, value in results.items():
                df.at[idx, col] = value
                
                # ADD CONFIDENCE COLUMNS if requested
                if add_confidence_columns and col in confidence_scores:
                    confidence_col = f"{col}_confidence"
                    if confidence_col not in df.columns:
                        df[confidence_col] = None
                    df.at[idx, confidence_col] = round(confidence_scores[col], 2)
            
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
        "state": "headquarters state province region",
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

def calculate_confidence_statistics(df: pd.DataFrame, target_columns: List[str]) -> Dict[str, Any]:
    """Calculate confidence statistics for processed data."""
    stats = {}
    
    for col in target_columns:
        confidence_col = f"{col}_confidence"
        if confidence_col in df.columns:
            confidence_values = df[confidence_col].dropna()
            if len(confidence_values) > 0:
                stats[col] = {
                    'mean': round(confidence_values.mean(), 2),
                    'min': round(confidence_values.min(), 2),
                    'max': round(confidence_values.max(), 2),
                    'count': len(confidence_values),
                    'high_confidence': len(confidence_values[confidence_values >= 0.8]),
                    'low_confidence': len(confidence_values[confidence_values < 0.6])
                }
    
    return stats

def main():
    st.title("CatenaryLM - Excel Assistant")
    st.markdown("### Enhanced with Geographic Validation & Confidence Scoring")
    
    # Sidebar for API keys
    st.sidebar.header("API Configuration")
    
    # Try to get API keys from Streamlit secrets first, then fall back to user input
    try:
        claude_api_key = st.secrets["CLAUDE_API_KEY"]
        st.sidebar.success("Claude API Key loaded from secrets")
    except:
        claude_api_key = st.sidebar.text_input("Claude API Key", type="password", 
                                            help="Enter your Anthropic Claude API key")
        if not claude_api_key:
            st.sidebar.warning("Claude API Key required")

    # Search provider selection
    search_provider = st.sidebar.radio(
        "Search Provider",
        ["perplexity", "tavily"],
        help="Choose between Perplexity AI or Tavily for web search"
    )
    
    if search_provider == "perplexity":
        try:
            search_api_key = st.secrets["PERPLEXITY_API_KEY"]
            st.sidebar.success("Perplexity API Key loaded from secrets")
        except:
            search_api_key = st.sidebar.text_input("Perplexity API Key", type="password",
                                                help="Enter your Perplexity AI API key")
            if not search_api_key:
                st.sidebar.warning("Perplexity API Key required")
    else:
        try:
            search_api_key = st.secrets["TAVILY_API_KEY"]
            st.sidebar.success("Tavily API Key loaded from secrets")
        except:
            search_api_key = st.sidebar.text_input("Tavily API Key", type="password",
                                                help="Enter your Tavily search API key")
            if not search_api_key:
                st.sidebar.warning("Tavily API Key required")
    
    # Add debug mode toggle
    st.session_state.debug_mode = st.sidebar.checkbox("Enable Debug Mode", value=False)
    st.debug = setup_debug()
    
    # Sidebar for API options
    st.sidebar.header("API Options")
    use_web_search = st.sidebar.checkbox("Use web search", value=True, 
                                       help="Disable if you're experiencing API errors")
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
            st.markdown("Choose columns you want to fill with AI-powered searches. The system will process **Country → State → City** in optimal order.")
            
            # Dynamically create multiselect for all columns
            target_columns = st.multiselect(
                "Select columns to augment with AI searches",
                options=list(df.columns),
                help="Choose which columns you want the AI to fill with data"
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
                test_mode = st.checkbox("Test Mode (process small batch)", value=True)
                test_batch_size = st.number_input("Test Batch Size", min_value=1, max_value=20, value=5)
            
            with col2:
                create_if_missing = st.checkbox("Create columns if they don't exist", value=True)
                overwrite_existing = st.checkbox("Overwrite existing values", value=True,
                                               help="Override existing data if confidence is low")
                skip_non_company_emails = st.checkbox("Skip personal emails", value=True, 
                                                    help="Skip gmail.com, yahoo.com, etc.")
                add_confidence_scores = st.checkbox("Add confidence score columns", value=False,
                                                  help="Add columns showing AI confidence (0.0-1.0) for each filled value")
            
            # Ensure columns exist if needed
            if create_if_missing:
                for col in target_columns:
                    if col not in df.columns:
                        df[col] = None
            
            # Show sample of data
            st.subheader("Data Preview")
            st.dataframe(df.head())
            
            # Enhanced features info
            if any(geo_term in col.lower() for col in target_columns for geo_term in ['city', 'state', 'country']):
                st.info("Geographic validation enabled - Automatic correction of inconsistent city/state/country combinations with confidence scoring")
            
            if any(email_col for email_col in df.columns if 'email' in email_col.lower()):
                financial_domains = get_financial_domain_info()
                domain_count = 0
                for _, row in df.head(10).iterrows():
                    for col in df.columns:
                        if 'email' in col.lower() and pd.notna(row[col]) and '@' in str(row[col]):
                            domain = str(row[col]).split('@')[1]
                            if domain in financial_domains:
                                domain_count += 1
                                break
                
                if domain_count > 0:
                    st.info(f"Known financial domains detected - High-confidence data available for {domain_count} entries in preview")
            
            if add_confidence_scores:
                st.info("📊 Confidence scoring enabled - Additional columns will show AI certainty levels (0.0-1.0)")
            
            # Processing button
            process_button = st.button("Start AI Processing", type="primary")
            
            if process_button:
                if not claude_api_key:
                    st.error("Please provide a Claude API Key")
                elif not search_api_key and use_web_search:
                    st.warning(f"No {search_provider.title()} API Key provided. Disabling web search.")
                    use_web_search = False
                elif not target_columns:
                    st.error("Please select at least one column to augment")
                elif not (use_web_search or use_claude_direct):
                    st.error("Please enable at least one data source")
                else:
                    # Check if all target columns exist
                    missing_columns = [col for col in target_columns if col not in df.columns]
                    if missing_columns and not create_if_missing:
                        st.error(f"The following columns don't exist: {', '.join(missing_columns)}. Enable 'Create columns if they don't exist' or select different columns.")
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
                            os.makedirs(auto_save_directory, exist_ok=True)
                        
                        # Show concurrency status
                        if use_concurrent:
                            st.info(f"⚡ Concurrent processing enabled with {max_workers} workers.")
                        
                        # Show processing strategy
                        ordered_cols = get_processing_order_optimized(target_columns)
                        if len(ordered_cols) > 1:
                            st.info(f"🔄 Processing order: {' → '.join(ordered_cols)}")
                        
                        with st.spinner(f"Processing {'test batch' if test_mode else 'entire dataset'}..."):
                            start_time = time.time()
                            
                            # Process the data
                            processed_df = process_batch(
                                df=df.copy(), 
                                start_idx=0, 
                                end_idx=batch_size, 
                                target_columns=target_columns,
                                search_contexts=search_contexts,
                                claude_api_key=claude_api_key, 
                                search_api_key=search_api_key,
                                search_provider=search_provider,
                                use_web_search=use_web_search,
                                use_claude_direct=use_claude_direct,
                                overwrite_existing=overwrite_existing,
                                skip_non_company_emails=skip_non_company_emails,
                                progress_bar=progress_bar,
                                auto_save=enable_auto_save,
                                auto_save_interval=auto_save_interval,
                                input_filename=input_filename,
                                auto_save_directory=auto_save_directory,
                                use_concurrent=use_concurrent,
                                max_workers=max_workers,
                                add_confidence_columns=add_confidence_scores
                            )
                            
                            end_time = time.time()
                            duration = end_time - start_time
                            
                            st.success(f"✅ Processing completed in {duration:.2f} seconds!")
                        
                        # Store in session state
                        st.session_state.processed_df = processed_df
                        st.session_state.test_batch_completed = test_mode
                        st.session_state.processing_complete = not test_mode
                        
                        # Store these for the full processing
                        st.session_state.original_df = original_df
                        st.session_state.batch_size = batch_size
                        st.session_state.target_columns = target_columns
                        st.session_state.search_contexts = search_contexts
                        st.session_state.input_filename = input_filename
                        st.session_state.processing_settings = {
                            'claude_api_key': claude_api_key,
                            'search_api_key': search_api_key,
                            'search_provider': search_provider,
                            'use_web_search': use_web_search,
                            'use_claude_direct': use_claude_direct,
                            'overwrite_existing': overwrite_existing,
                            'skip_non_company_emails': skip_non_company_emails,
                            'enable_auto_save': enable_auto_save,
                            'auto_save_interval': auto_save_interval,
                            'auto_save_directory': auto_save_directory,
                            'use_concurrent': use_concurrent,
                            'max_workers': max_workers,
                            'add_confidence_scores': add_confidence_scores
                        }

            # Show results if we have processed data
            if st.session_state.processed_df is not None:
                st.subheader("Results")
                
                # Show confidence statistics if enabled
                if st.session_state.processing_settings.get('add_confidence_scores', False):
                    confidence_stats = calculate_confidence_statistics(st.session_state.processed_df, st.session_state.target_columns)
                    if confidence_stats:
                        st.subheader("📊 Confidence Statistics")
                        stats_cols = st.columns(len(confidence_stats))
                        for i, (col, stats) in enumerate(confidence_stats.items()):
                            with stats_cols[i]:
                                st.metric(f"{col}", f"{stats['mean']:.2f}", 
                                        f"{stats['count']} values")
                                st.caption(f"High confidence: {stats['high_confidence']}")
                                st.caption(f"Low confidence: {stats['low_confidence']}")
                
                if st.session_state.test_batch_completed and not st.session_state.processing_complete:
                    # Show comparison of before and after
                    st.markdown("### Before Processing")
                    st.dataframe(st.session_state.original_df.head(st.session_state.batch_size))
                    
                    st.markdown("### After Processing")
                    st.dataframe(st.session_state.processed_df.head(st.session_state.batch_size))
                    
                    # Show changes
                    st.markdown("### Changes Made")
                    changes_count = 0
                    for col in st.session_state.target_columns:
                        if col in st.session_state.processed_df.columns and col in st.session_state.original_df.columns:
                            col_changes = (st.session_state.processed_df.iloc[:st.session_state.batch_size][col] != st.session_state.original_df.iloc[:st.session_state.batch_size][col]).sum()
                            changes_count += col_changes
                            st.text(f"Changes in {col}: {col_changes} rows")
                    
                    st.success(f"Total changes made: {changes_count} cells")
                    
                    # Calculate remaining rows
                    remaining_rows = len(st.session_state.original_df) - st.session_state.batch_size
                    
                    if remaining_rows > 0:
                        # Option to process entire file
                        st.markdown("### Process Remaining Data")
                        st.info(f"Ready to process {remaining_rows} additional rows (from row {st.session_state.batch_size + 1} to {len(st.session_state.original_df)})")
                        
                        # Show preview of remaining data
                        st.write("**Preview of remaining data:**")
                        st.dataframe(st.session_state.original_df.iloc[st.session_state.batch_size:st.session_state.batch_size+3])
                        
                        # Process entire file button
                        if st.button("Process Entire File", type="primary", key="process_entire_file"):
                            st.subheader("Processing Complete File")
                            
                            full_progress_bar = st.progress(0)
                            settings = st.session_state.processing_settings
                            
                            if settings['enable_auto_save']:
                                st.info(f"Auto-saving enabled. Files will be saved to {settings['auto_save_directory']} every {settings['auto_save_interval']} rows.")
                            
                            with st.spinner(f"Processing remaining {remaining_rows} rows..."):
                                start_time = time.time()
                                
                                try:
                                    final_df = process_batch(
                                        df=st.session_state.processed_df,
                                        start_idx=st.session_state.batch_size, 
                                        end_idx=len(st.session_state.original_df), 
                                        target_columns=st.session_state.target_columns,
                                        search_contexts=st.session_state.search_contexts,
                                        claude_api_key=settings['claude_api_key'],
                                        search_api_key=settings['search_api_key'],
                                        search_provider=settings['search_provider'],
                                        use_web_search=settings['use_web_search'],
                                        use_claude_direct=settings['use_claude_direct'],
                                        overwrite_existing=settings['overwrite_existing'],
                                        skip_non_company_emails=settings['skip_non_company_emails'],
                                        progress_bar=full_progress_bar,
                                        auto_save=settings['enable_auto_save'],
                                        auto_save_interval=settings['auto_save_interval'],
                                        input_filename=st.session_state.input_filename,
                                        auto_save_directory=settings['auto_save_directory'],
                                        use_concurrent=settings['use_concurrent'],
                                        max_workers=settings['max_workers'],
                                        add_confidence_columns=settings['add_confidence_scores']
                                    )
                                    
                                    end_time = time.time()
                                    duration = end_time - start_time
                                    
                                    st.success(f"✅ Full processing completed in {duration:.2f} seconds!")
                                    st.success(f"✅ Processed {remaining_rows} additional rows!")
                                    
                                    # Update session state
                                    st.session_state.processed_df = final_df
                                    st.session_state.processing_complete = True
                                    
                                    # Force a rerun to show final results
                                    st.rerun()
                                    
                                except Exception as e:
                                    st.error(f"Error during full processing: {str(e)}")
                                    if st.session_state.debug_mode:
                                        st.error(f"Exception details: {traceback.format_exc()}")
                    else:
                        st.info("All rows have already been processed in the test batch!")
                        st.session_state.processing_complete = True
                
                # Show final results
                if st.session_state.processing_complete:
                    st.markdown("### Final Results")
                    
                    # Show final confidence statistics
                    if st.session_state.processing_settings.get('add_confidence_scores', False):
                        final_confidence_stats = calculate_confidence_statistics(st.session_state.processed_df, st.session_state.target_columns)
                        if final_confidence_stats:
                            st.subheader("📊 Final Confidence Statistics")
                            final_stats_cols = st.columns(len(final_confidence_stats))
                            for i, (col, stats) in enumerate(final_confidence_stats.items()):
                                with final_stats_cols[i]:
                                    st.metric(f"{col}", f"{stats['mean']:.2f}", 
                                            f"{stats['count']} values")
                                    st.caption(f"High confidence: {stats['high_confidence']}")
                                    st.caption(f"Low confidence: {stats['low_confidence']}")
                    
                    st.success("Full file processed!")
                    st.dataframe(st.session_state.processed_df)
                    
                    # Show processing summary
                    st.markdown("### Processing Summary")
                    col1, col2, col3 = st.columns(3)
                    col1.metric("Total Rows Processed", len(st.session_state.processed_df))
                    if hasattr(st.session_state, 'batch_size'):
                        col2.metric("Test Batch", st.session_state.batch_size)
                        col3.metric("Full Processing", len(st.session_state.processed_df) - st.session_state.batch_size)
                    
                    settings = st.session_state.processing_settings
                    if settings['enable_auto_save']:
                        auto_save_directory = settings['auto_save_directory']
                        st.success(f"💾 Final results auto-saved to {auto_save_directory}")
                        if os.path.exists(auto_save_directory):
                            files = [f for f in os.listdir(auto_save_directory) if f.startswith(st.session_state.input_filename)]
                            files.sort(reverse=True)
                            if files:
                                st.info(f"Most recent auto-saved files:")
                                for i, file in enumerate(files[:3]):
                                    st.code(os.path.join(auto_save_directory, file))
                    
                    offer_download_options(st.session_state.processed_df)
                
                # Reset button
                if st.button("Start New Processing", type="secondary"):
                    st.session_state.processed_df = None
                    st.session_state.test_batch_completed = False
                    st.session_state.processing_complete = False
                    st.rerun()
                        
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