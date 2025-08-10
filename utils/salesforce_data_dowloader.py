import requests
from simple_salesforce import Salesforce
import pandas as pd
import os


class SalesforceReportDownloader:
    def __init__(self, username, password, security_token):
        """
        Initialize the SalesforceReportDownloader with credentials.
        """
        self.username = username
        self.password = password
        self.security_token = security_token
        self.sf = None
        self.access_token = None
        self.instance_url = None

    def authenticate(self):
        """
        Authenticate using simple-salesforce and store session details.
        """
        self.sf = Salesforce(
            username=self.username,
            password=self.password,
            security_token=self.security_token
        )
        self.access_token = self.sf.session_id
        self.instance_url = f"https://{self.sf.sf_instance}"
        print("✅ Authentication successful")

    def download_report(self, report_id, output_path="/tmp/salesforce_report.csv"):
        """
        Download a Salesforce report as CSV and return the file path.
        """
        if not self.sf:
            raise Exception("Not authenticated. Call authenticate() first.")

        # Build API URL
        url = f"{self.instance_url}/services/data/v60.0/analytics/reports/{report_id}"
        headers = {"Authorization": f"Bearer {self.access_token}"}

        # Fetch report data
        response = requests.get(url, headers=headers).json()

        # Extract columns
        column_keys = response['reportMetadata']['detailColumns']
        column_labels = [
            response['reportExtendedMetadata']['detailColumnInfo'][key]['label']
            for key in column_keys
        ]

        # Extract data rows
        factMap = response['factMap']['T!T']['rows']
        data = []
        for row in factMap:
            data.append([cell.get('label') for cell in row['dataCells']])

        # Save to CSV
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df = pd.DataFrame(data, columns=column_labels)
        df.to_csv(output_path, index=False)

        print(f"✅ Report saved to {output_path}")
        return output_path



# Removing prospected leads
def remove_existing_companies(cleaned_file="/tmp/cleaned_data.csv",
                              salesforce_file="/tmp/salesforce_report.csv",
                              output_file="/tmp/filtered_data.csv"):
    """
    Removes rows from cleaned_file where company_name exists in the Salesforce report.
    Saves the filtered data back to output_file (overwrites by default).
    """
    # Load both CSVs
    cleaned_df = pd.read_csv(cleaned_file)
    sf_df = pd.read_csv(salesforce_file)

    # Extract company names from Salesforce
    sf_companies = sf_df["Company / Account"].dropna().unique()

    # Filter cleaned_df
    filtered_df = cleaned_df[~cleaned_df["company_name"].isin(sf_companies)]

    # Save filtered data back to cleaned_data.csv
    filtered_df.to_csv(output_file, index=False)
    print(f"✅ Filtered data saved to {output_file} (removed {len(cleaned_df) - len(filtered_df)} rows)")

    return output_file