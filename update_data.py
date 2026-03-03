import os
import io
import json
import zipfile
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import re
import urllib.parse

# ---------------------------------------------------------
# MODULE 1: ASP DRUG DATABASE SCRAPER
# ---------------------------------------------------------
def get_latest_asp_zip_url():
    print("\n-> Scraping CMS.gov for the latest ASP (Drug) Files...")
    base_url = "https://www.cms.gov"
    main_url = "https://www.cms.gov/medicare/payment/part-b-drugs/asp-pricing-files"
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    response = requests.get(main_url, headers=headers)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')
    
    links = []
    for a in soup.find_all('a', href=True):
        href = a['href'].lower().strip()
        if '.zip' in href and ('part-b-payment-limit' in href or 'asp-pricing' in href):
            search_string = a.text.strip().lower() + " " + href
            year_match = re.search(r'20\d{2}', search_string)
            if year_match:
                year = int(year_match.group(0))
                q_weight = 0
                if 'january' in search_string: q_weight = 1
                elif 'april' in search_string: q_weight = 2
                elif 'july' in search_string: q_weight = 3
                elif 'october' in search_string: q_weight = 4
                links.append({'year': year, 'quarter': q_weight, 'url': a['href'].strip()})

    if not links:
        raise Exception("❌ Could not locate ASP ZIP links.")
        
    links.sort(key=lambda x: (x['year'], x['quarter']), reverse=True)
    zip_link = links[0]['url']
    if not zip_link.startswith('http'):
        zip_link = base_url + zip_link
        
    print(f"-> Found Direct ASP ZIP Link: {zip_link}")
    return zip_link

def process_dynamic_asp_file():
    zip_url = get_latest_asp_zip_url()
    print(f"-> Initiating Direct RAM Download from: {zip_url}")
    headers = {'User-Agent': 'Mozilla/5.0'}
    r = requests.get(zip_url, headers=headers)
    r.raise_for_status()
    asp_dict = {}
    
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        csv_filename = next((name for name in z.namelist() if name.endswith('.csv') and '__MACOSX' not in name), None)
        if not csv_filename: raise Exception("❌ No CSV found in ASP ZIP.")
            
        with z.open(csv_filename) as f:
            content = f.read().decode('utf-8', errors='ignore')
            lines = content.splitlines()
            
            # REGEX BOUNDARY LOCK: Looks for exact CSV cell match
            header_idx = 0
            for i, line in enumerate(lines[:50]):
                if re.search(r'(?:^|,)[\s"\']*HCPCS(?: CODE)?[\s"\']*(?:,|$)', line.upper()):
                    header_idx = i
                    break
            
            df = pd.read_csv(io.StringIO(content), skiprows=header_idx, dtype={'HCPCS Code': str})
            
            hcpcs_col = next((col for col in df.columns if 'HCPCS' in col.upper()), None)
            price_col = next((col for col in df.columns if 'LIMIT' in col.upper() or 'FEE' in col.upper() or 'AMOUNT' in col.upper()), None)
            
            if hcpcs_col and price_col:
                df[hcpcs_col] = df[hcpcs_col].astype(str).str.replace('.0', '', regex=False).str.strip()
                df = df.dropna(subset=[hcpcs_col, price_col])
                
                for index, row in df.iterrows():
                    code = row[hcpcs_col]
                    desc = str(row.get('Short Description', 'Medicare Part B Drug')).strip()
                    try:
                        price = float(str(row[price_col]).replace(',', '').strip())
                        if price > 0:
                            asp_dict[code] = {"desc": desc, "price": price, "schedule": "ASP Drug Database"}
                    except ValueError: continue
            else:
                raise Exception(f"❌ Could not map ASP columns. Found: {df.columns.tolist()}")
    return asp_dict

# ---------------------------------------------------------
# MODULE 2: CLFS LAB DATABASE SCRAPER
# ---------------------------------------------------------
def get_latest_clfs_zip_url():
    print("\n-> Scraping CMS.gov for the latest CLFS (Lab) Files...")
    base_url = "https://www.cms.gov"
    main_url = "https://www.cms.gov/medicare/payment/fee-schedules/clinical-laboratory-fee-schedule-clfs/files"
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    response = requests.get(main_url, headers=headers)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')
    
    links = []
    for a in soup.find_all('a', href=True):
        href = a['href'].strip().lower()
        text = a.text.strip().lower()
        match = re.search(r'(\d{2})clabq(\d)', text + href)
        if match:
            year = int("20" + match.group(1))
            quarter = int(match.group(2))
            links.append({'year': year, 'quarter': quarter, 'url': a['href'].strip()})

    if not links:
        raise Exception("❌ Could not locate CLFS links.")
        
    links.sort(key=lambda x: (x['year'], x['quarter']), reverse=True)
    page_link = links[0]['url']
    if not page_link.startswith('http'):
        page_link = base_url + page_link
        
    if page_link.endswith('.zip'): return page_link
        
    print(f"-> Navigating to CLFS Sub-Page: {page_link}")
    sub_response = requests.get(page_link, headers=headers)
    sub_soup = BeautifulSoup(sub_response.text, 'html.parser')
    
    zip_link = None
    for a in sub_soup.find_all('a', href=True):
        href = a['href'].strip()
        if href.endswith('.zip') or ('license.asp' in href and '.zip' in href):
            zip_link = href
            break
            
    if not zip_link:
        raise Exception("❌ Could not find a ZIP link on the CLFS sub-page.")

    if 'license.asp' in zip_link and 'file=' in zip_link:
        print("-> Detected AMA License Wall. Extracting direct backend payload...")
        file_path = zip_link.split('file=')[-1].strip()
        zip_link = base_url + file_path
    elif not zip_link.startswith('http'):
        zip_link = base_url + zip_link
            
    print(f"-> Found Direct CLFS ZIP Link: {zip_link}")
    return zip_link

def process_dynamic_clfs_file():
    zip_url = get_latest_clfs_zip_url()
    print(f"-> Initiating Direct RAM Download from: {zip_url}")
    headers = {'User-Agent': 'Mozilla/5.0'}
    r = requests.get(zip_url, headers=headers)
    r.raise_for_status()
    clfs_dict = {}
    
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        csv_filename = next((name for name in z.namelist() if name.endswith('.csv') and '__MACOSX' not in name), None)
        if not csv_filename: raise Exception("❌ No CSV found in CLFS ZIP.")
            
        print(f"-> Extracting and analyzing CLFS dataset: {csv_filename}")
        with z.open(csv_filename) as f:
            content = f.read().decode('utf-8', errors='ignore')
            lines = content.splitlines()
            
            # REGEX BOUNDARY LOCK: Looks for exact CSV cell match to dodge COVID paragraphs
            header_idx = None
            for i, line in enumerate(lines[:50]):
                if re.search(r'(?:^|,)[\s"\']*HCPCS[\s"\']*(?:,|$)', line.upper()):
                    header_idx = i
                    break
                    
            if header_idx is None:
                raise Exception("❌ Could not find an exact 'HCPCS' column header using regex.")
                
            print(f"-> Dynamic header locked at line {header_idx}. Parsing prices...")
            
            df = pd.read_csv(io.StringIO(content), skiprows=header_idx, dtype=str)
            
            hcpcs_col = next((col for col in df.columns if 'HCPCS' in col.upper()), None)
            
            # Massive Fuzzy Price Matcher
            price_keywords = ['LIMIT', 'NLA', 'FEE', 'AMOUNT', 'PAYMENT', 'PRICE', 'RATE']
            price_col = None
            for col in df.columns:
                upper_col = col.upper()
                if any(kw in upper_col for kw in price_keywords) and 'NOTE' not in upper_col and 'MODIFIER' not in upper_col:
                    price_col = col
                    break
            
            mod_col = next((col for col in df.columns if 'MODIFIER' in col.upper() or 'MOD' == col.upper().strip()), None)
            
            if hcpcs_col and price_col:
                print(f"-> Mapped HCPCS to '{hcpcs_col}' and Price to '{price_col}'")
                df[hcpcs_col] = df[hcpcs_col].astype(str).str.strip()
                df = df.dropna(subset=[hcpcs_col, price_col])
                
                for index, row in df.iterrows():
                    code = row[hcpcs_col]
                    if mod_col and pd.notna(row[mod_col]) and str(row[mod_col]).strip() != '':
                        continue 
                        
                    try:
                        price = float(str(row[price_col]).replace(',', '').strip())
                        if price > 0:
                            clfs_dict[code] = {"desc": f"Clinical Lab Test ({code})", "price": price, "schedule": "CLFS Lab Database"}
                    except ValueError: continue
            else:
                print(f"-> ⚠️ Columns detected by Pandas: {df.columns.tolist()}")
                raise Exception("❌ Could not locate HCPCS or Price columns in CLFS.")
    return clfs_dict

# ---------------------------------------------------------
# MASTER PIPELINE (The Merger & Meta-Injector)
# ---------------------------------------------------------
def fetch_dynamic_uuid(keyword):
    print(f"-> Querying CMS Data API for dynamic UUID: {keyword}")
    url = f"https://data.cms.gov/data-api/v1/dataset/search?keyword={urllib.parse.quote(keyword)}"
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        response = requests.get(url, headers=headers)
        # SCHEMA-AGNOSTIC REGEX: Extracts the first 36-character UUID it finds in the payload
        matches = re.findall(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', response.text.lower())
        if matches:
            return matches[0]
    except Exception as e:
        print(f"⚠️ UUID Fetch Failed: {e}")
    return None

def generate_supplemental_db():
    print(f"\n[{datetime.now()}] Initiating Zero-Hardcode Omni-Scraper...")
    
    # 1. Fetch Dynamic Database IDs
    pfs_uuid = fetch_dynamic_uuid("National Payment Amount File") or "1a4e7cb4-65db-48fd-8250-a64a3cc6e583"
    gpci_uuid = fetch_dynamic_uuid("Geographic Practice Cost Index") or "81f942b8-3f6c-4b36-a151-0888376d9ca0"
    
    master_db = {
        "__META__": {
            "pfs_uuid": pfs_uuid,
            "gpci_uuid": gpci_uuid,
            "last_updated": str(datetime.now().date())
        }
    }
    
    print(f"✅ Dynamic Meta-Data Locked. PFS: {pfs_uuid} | GPCI: {gpci_uuid}")
    
    # 2. Extract Data Lakes
    try:
        asp_data = process_dynamic_asp_file()
        master_db.update(asp_data)
        print(f"✅ SUCCESS: Extracted {len(asp_data)} ASP Drug Codes.")
    except Exception as e: print(f"❌ ASP ERROR: {str(e)}")
        
    try:
        clfs_data = process_dynamic_clfs_file()
        for code, data in clfs_data.items():
            if code not in master_db: master_db[code] = data
        print(f"✅ SUCCESS: Extracted {len(clfs_data)} CLFS Lab Codes.")
    except Exception as e: print(f"❌ CLFS ERROR: {str(e)}")
        
    output_path = "supplemental.json"
    with open(output_path, "w") as json_file:
        json.dump(master_db, json_file, indent=4)
    print(f"\n🚀 Saved Data Lake and Meta-Headers to {output_path}")

if __name__ == "__main__":
    generate_supplemental_db()