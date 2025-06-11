"""

**Author:** Christina Konstantopoulou  
**Version:** 1.2 

---------------------------------------------------------------
This script:

- automatically downloads the latest list of certified growers 
(as a CSV) from the RSPO PRISMA website,  
- filters the ones with license start dates in a specific time range,
- then downloads their Audit Reports one by one.
- Finally it saves a log of what was downloaded.

"""
import os
import asyncio
import pandas as pd
from datetime import datetime
import shutil
from playwright.async_api import async_playwright
import nest_asyncio

VERSION = "1.2"
RUN_TIMESTAMP = datetime.now().strftime("%Y-%m-%d %H:%M")

print(f"Script Version: {VERSION} | Run at: {RUN_TIMESTAMP}")

# USER CONFIGURATION - Specify a range for dates and file paths

nest_asyncio.apply()

# === # Define the range for the license start date ===
START_DATE = datetime(2025, 4, 1) # Specifz the licence start date
END_DATE = datetime(2025, 4, 10) 

DOWNLOADS_DIR = "...."
os.makedirs(DOWNLOADS_DIR, exist_ok=True)

# Define the download directory for the webs
DOWNLOADS_DIR_CSV = "...."
os.makedirs(DOWNLOADS_DIR_CSV, exist_ok=True)

# Automatically download all the members list as CSV from the RSPO website

async def download_csv():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)  # set headless=True for background mode, headless=False will popup the browser
        context = await browser.new_context(accept_downloads=True)
        page = await context.new_page()

        await page.goto("https://platform.prismabyrspo.org/certificate-registry/certified-growers", timeout=60000)
        await page.wait_for_selector("div:text('Download as CSV')", timeout=10000)

        async with page.expect_download() as download_info:
            await page.click("div:text('Download as CSV')")
        download = await download_info.value

        csv_filename = download.suggested_filename
        csv_path = os.path.join(DOWNLOADS_DIR_CSV, csv_filename)
        await download.save_as(csv_path)
        print(f" CSV downloaded to: {csv_path}")

        await browser.close()
        return csv_path

csv_file_path = await download_csv()

# Prepare for download
# Delete any existing files in the download folder
for filename in os.listdir(DOWNLOADS_DIR):
    file_path = os.path.join(DOWNLOADS_DIR, filename)
    try:
        if os.path.isfile(file_path):
            os.remove(file_path)
    except Exception as e:
        print(f"Could not delete {file_path}: {e}")

# === Load and  Filter RSPO members from the CSV file within date range ===
df = pd.read_csv(csv_file_path, header=1)
df['License Start Date'] = pd.to_datetime(df['License Start Date'], format="%d-%b-%Y", errors='coerce')
filtered_df = df[
    (df['License Start Date'] >= START_DATE) &
    (df['License Start Date'] <= END_DATE)
]
target_records = filtered_df[['Prisma Trading Account ID', 'License Start Date']].dropna().drop_duplicates()

# Track downloaded files
download_log = []

# === Main download function ===
async def download_audit_reports():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless= True)  # set headless=True for background mode, headless=False will popup the browser 
        context = await browser.new_context(accept_downloads=True)
        page = await context.new_page()

        for _, row in target_records.iterrows():
            prisma_id = row['Prisma Trading Account ID'].strip()
            license_start = row['License Start Date']

            print(f"\n Searching for {prisma_id}...")
            await page.goto("https://platform.prismabyrspo.org/certificate-registry/certified-growers", timeout=60000)
            await page.wait_for_selector(".MuiDataGrid-root", timeout=30000)

            try:
                # Find and use search bar
                await page.wait_for_selector("input#search", timeout=10000)
                search_box = await page.query_selector("input#search")
                if not search_box:
                    print(f" Search bar not found for {prisma_id}")
                    continue

                # Clear and enter PO ID
                await search_box.click()
                await search_box.fill("")
                await search_box.type(prisma_id)
                await page.keyboard.press("Enter")
                await page.evaluate("document.activeElement.blur()")  # trigger search
                await page.wait_for_timeout(2000)

                # Verify that top row matches the searched PO ID
                rows = await page.query_selector_all(".MuiDataGrid-row")
                if not rows:
                    print(f"  No search results for {prisma_id}")
                    continue

                first_row_cells = await rows[0].query_selector_all(".MuiDataGrid-cell")
                if len(first_row_cells) < 6:
                    print(f"  Unexpected row format for {prisma_id}")
                    continue

                visible_po_id = (await first_row_cells[5].inner_text()).strip()
                print(f"  PO ID: {visible_po_id}")
                if visible_po_id != prisma_id:
                    print(f"  Mismatch — skipping {prisma_id}")
                    continue

                # Click and download audit reports
                await rows[0].click()
                await page.wait_for_timeout(1500)

                elements = await page.query_selector_all("text=Audit Report")
                if not elements:
                    print(f"  No Audit Reports for {prisma_id}")
                    continue

                for j, el in enumerate(elements):
                    try:
                        async with page.expect_download(timeout=5000) as download_info:
                            await el.click()
                        download = await download_info.value
                        filename = f"{prisma_id}_{license_start.strftime('%Y-%m-%d')}_audit_{j+1}.pdf"
                        save_path = os.path.join(DOWNLOADS_DIR, filename)
                        await download.save_as(save_path)
                        print(f"  Saved: {save_path}")
                        download_log.append({
                            'Prisma ID': prisma_id,
                            'License Start Date': license_start.strftime('%Y-%m-%d'),
                            'File Type': 'Audit Report',
                            'Filename': filename
                        })
                    except Exception as e:
                        print(f"  Download failed for {prisma_id}: {e}")

            except Exception as e:
                print(f"  Error processing {prisma_id}: {e}")
                continue

        await browser.close()

        # Save download log
        df_log = pd.DataFrame(download_log)
        log_path = os.path.join(DOWNLOADS_DIR, "download_log.csv")
        df_log.to_csv(log_path, index=False)
        print(f"\n Download log saved to: {log_path}")

# === Run script ===
asyncio.run(download_audit_reports())
