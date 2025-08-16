import pandas as pd
import os
import asyncio
from dotenv import load_dotenv
from rich import print

# Import your classes
from utils.data_cleaner import DataPreprocessor
from utils.salesforce_data_dowloader import SalesforceReportDownloader, remove_existing_companies
from utils.data_downloader import GoogleDriveDownloader
from utils.dropbox_uploader import DropboxUploader
from ai_filters.jd_qualifier import JobClassifier
from ai_filters.web_search import CompanySizeFiller
from ai_filters.company_classifier import CompanyClassifier

load_dotenv()

username = 'oas@salariasales.com'
report_id = "00OPF000006VAVt2AO"
password = os.getenv("SF_password")
security_token = os.getenv("SF_security_token")

# All the orchestration logic should be within a single async function
async def orchestrate():
    gdrive_folder_name = "Apify Uploads"
    config_file = "config/cleaner.yml"
    cleaned_file_name = "/tmp/cleaned_data.csv"
    my_dropbox_folder = "/ApifyCleaned"
    service_account_credentials = "credentials.json"
    salesforce_file_name = "/tmp/salesforce_report.csv"
    filtered_data_file_path = "/tmp/filtered_data.csv"
    jd_classified_path = "/tmp/classified_data.csv"
    filled_path = "/tmp/filled_data.csv"
    final_data_path = "/tmp/Completed.csv" # Changed name to avoid conflict

    print("[bold blue]Starting Orchestration Pipeline[/bold blue]")

    # Step 1: Download files from Google Drive (Synchronous)
    # This part seems to be synchronous, so we'll just call it directly.
    downloader = GoogleDriveDownloader(
        folder_name=gdrive_folder_name,
        service_account_file=service_account_credentials
    )
    downloaded_files = downloader.runner()
    print("Downloaded files:", downloaded_files)

    # Step 2: Preprocess file (Synchronous)
    preprocessor = DataPreprocessor(config_file, downloaded_files)
    is_not_empty = preprocessor.runner(output_path=cleaned_file_name)

    if not is_not_empty:
        print("[yellow]No new files to process. Exiting.[/yellow]")
        return # Use return instead of else block for cleaner code

    # Step 3: Remove existing companies (Synchronous)
    print("[cyan]Downloading Salesforce report...[/cyan]")
    downloader = SalesforceReportDownloader(
        username=username,
        password=password,
        security_token=security_token
    )
    downloader.authenticate()
    report_file_path = downloader.download_report(report_id=report_id, output_path=salesforce_file_name)
    print("File available at:", report_file_path)

    print("[cyan]Filtering existing companies...[/cyan]")
    remove_existing_companies(
        cleaned_file=cleaned_file_name,
        salesforce_file=salesforce_file_name,
        output_file=filtered_data_file_path
    )

    # Step 4: Job Classification (ASYNC)
    # The dataframe is loaded here.
    try:
        df = pd.read_csv(filtered_data_file_path)
    except FileNotFoundError:
        print(f"[red]Could not find filtered data at {filtered_data_file_path}. Exiting.[/red]")
        return

    print("[cyan]Starting Job Classification...[/cyan]")
    job_classifier = JobClassifier(limit_rows=None) # Increased limit for a more realistic scenario
    # Await the async method call
    processed_df = await job_classifier.process_dataframe(df)
    processed_df.to_csv(jd_classified_path, index=False)
    print("[green]Job Classification Complete![/green]")

    # Step 5: Company Data Enrichment (Synchronous)
    # This seems to be a synchronous process, so no await is needed.
    print("[cyan]Starting Company Data Enrichment...[/cyan]")
    filler = CompanySizeFiller()
    filler.fill_missing_sizes(file_path=jd_classified_path, output_path=filled_path)
    print("[green]Company Data Enrichment Complete![/green]")

    # Step 6: Company Classification (ASYNC)
    print("[cyan]Starting Company Classification...[/cyan]")
    company_classifier = CompanyClassifier(max_concurrency=10)
    # Await the async method call
    await company_classifier.process_dataset(
        input_csv_path=filled_path,
        output_csv_path=final_data_path
    )
    print("[green]Company Classification Complete![/green]")

    # Step 7: Upload to Dropbox (Synchronous)
    print("[cyan]Uploading final data to Dropbox...[/cyan]")
    uploader = DropboxUploader(dropbox_folder=my_dropbox_folder)
    link = uploader.upload(final_data_path)
    print(f"[bold green]Final pipeline complete! File available at: {link}[/bold green]")


# The single entry point to the async world
if __name__ == '__main__':
    # Use asyncio.run() once to start the event loop
    try:
        asyncio.run(orchestrate())
    except Exception as e:
        print(f"[bold red]An error occurred during orchestration:[/bold red] {e}")