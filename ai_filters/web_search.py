from typing import Optional
import pandas as pd
from rich import print
from langchain_tavily import TavilySearch
from langgraph.prebuilt import create_react_agent
from langchain_google_genai import ChatGoogleGenerativeAI
from dotenv import load_dotenv
from pathlib import Path



class CompanySizeFiller:
    def __init__(self, model_name: str = "gemini-2.5-flash-preview-05-20", temperature: float = 0.1):
        load_dotenv()
        self.llm = ChatGoogleGenerativeAI(
            model=model_name,
            temperature=temperature,
            max_retries=2
        )

        # Initialize Tavily Search Tool
        self.tavily_search_tool = TavilySearch(
            max_results=5,
            topic="general",
        )

        # Create the agent
        self.agent = create_react_agent(self.llm, [self.tavily_search_tool])

    def _fetch_company_size(self, company_name: str, industry: Optional[str] = None) -> Optional[str]:
        """
        Uses the agent to fetch the employee size for a company from the web.
        """
        prompt = f"""
        Search the web and get me the employee count range for the company below.

        - Primary source: LinkedIn  
        - Secondary sources: Indeed, Glassdoor  
        - If none are available, fallback to any other credible source

        company_name: {company_name}
        """

        if industry:
            prompt += f"\nindustry: {industry}"

        prompt += "\n\nOnly return the employee range."

        try:
            inputs = {"messages": [("user", prompt)]}
            response = self.agent.invoke(inputs)
            return response.get("messages", [])[-1].content
        except Exception as e:
            print(f"[red]Error fetching size for {company_name}: {e}[/red]")
            return None

    def fill_missing_sizes(
            self,
            file_path: str,
            output_path: str,
            company_col: str = "company_name",
            industry_col: str = "industry",
            size_col: str = "size",
    ) -> None:
        """
        Fill missing size values in the CSV file using AI agent and save to /tmp/filled_data.csv.

        Parameters:
            file_path (str): Path to the CSV file.
            output_path (str): Where to store preprocessed file
            company_col (str): Column name for the company name.
            industry_col (str): Column name for the industry.
            size_col (str): Column name for the size.

        Returns:
            pd.DataFrame: DataFrame with filled size values.
        """
        if not Path(file_path).is_file():
            raise FileNotFoundError(f"File not found: {file_path}")

        df = pd.read_csv(file_path)

        for col in [company_col, industry_col, size_col]:
            if col not in df.columns:
                raise ValueError(f"Missing column: '{col}'")

        missing_rows = df[df[size_col].isnull()].copy()

        # Only process rows that are not Disqualified
        process_rows = missing_rows[missing_rows["label"] != "Disqualified"]

        print(f"[bold yellow]Found {len(process_rows)} rows with missing size info.[/bold yellow]")

        for idx, row in process_rows.iterrows():
            company_name = row[company_col]
            industry = row[industry_col]

            if pd.isnull(company_name) or not str(company_name).strip():
                print(f"[blue]Skipping row {idx} due to empty company name.[/blue]")
                continue

            size = self._fetch_company_size(company_name, industry)
            if size:
                df.at[idx, size_col] = size
                print(f"[green]Filled size for {company_name} ({industry}): {size}[/green]")
            else:
                print(f"[red]Could not find size for {company_name} ({industry})[/red]")

        df.to_csv(output_path, index=False)
        print(f"[bold green]✅ Saved filled dataset to: {output_path}[/bold green]")

