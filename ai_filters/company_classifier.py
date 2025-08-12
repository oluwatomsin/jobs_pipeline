import yaml
import json
import asyncio
import pandas as pd
from dotenv import load_dotenv
from rich import print
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.prompts import PromptTemplate
from langchain.output_parsers import PydanticOutputParser
from data_schema.schema import CompanyQualifier


class CompanyClassifier:
    def __init__(
        self,
        config_path: str = "config/company_requirements.yml",
        model_name: str = "gemini-2.5-flash-preview-05-20",
        temperature: float = 0.1
    ):
        load_dotenv()

        # Load config from YAML
        with open(config_path, "r") as file:
            data = yaml.safe_load(file)

        self.instruction_data = data["prompt_v2"]["company_requirements"]
        self.instruction_json = json.dumps(self.instruction_data, indent=2)

        # Initialize model
        self.llm = ChatGoogleGenerativeAI(
            model=model_name,
            temperature=temperature,
            max_retries=2
        )

        # Output parser
        self.parser = PydanticOutputParser(pydantic_object=CompanyQualifier)

        # Prompt template
        self.prompt = PromptTemplate(
            template="""
You are an AI agent classifying companies as either "Qualified" or "Disqualified".

Instructions for classification:
{instruction_json}

Here is the company info:
{query}

{format_instructions}

Only return the JSON output.
""",
            input_variables=["query"],
            partial_variables={
                "instruction_json": self.instruction_json,
                "format_instructions": self.parser.get_format_instructions(),
            },
        )

    async def classify(self, n_employees: str, company_industry: str, company_location: str) -> str:
        """
        Classify a single company as Qualified or Disqualified.

        Parameters:
        - n_employees: Company size (employee count)
        - company_industry: Industry the company operates in
        - company_location: Geographic location of the company

        Returns:
        - str: "Qualified" or "Disqualified"
        """
        company_info = (
            f"company's Industry info: {company_industry}\n"
            f"company's employee head count info: {n_employees}\n"
            f"company's location info: {company_location}"
        )

        prompt_str = self.prompt.format(query=company_info)
        output = self.llm.invoke([{"role": "user", "content": prompt_str}])
        content = output.content.strip() if hasattr(output, "content") else str(output).strip()

        # Clean markdown/codeblock wrappers
        if content.startswith("```json"):
            content = content[7:]
        if content.startswith("```"):
            content = content[3:]
        if content.endswith("```"):
            content = content[:-3]
        content = content.strip()

        try:
            parsed = self.parser.parse(content)
            return parsed.label  # Return the label string only
        except Exception as e:
            print(
                f"[yellow]Error parsing result for [{company_industry}, {n_employees}, {company_location}]:[/yellow] {e}")
            return "Disqualified"  # Fail-safe default

    async def process_dataset(self, input_csv_path: str, output_csv_path: str) -> None:
        """
        Load CSV, classify companies, drop disqualified ones, and save the cleaned file.
        """
        df = pd.read_csv(input_csv_path)

        # Clean column names just in case
        df.columns = df.columns.str.strip().str.lower()

        required_columns = ["industry", "size", "company_location"]
        for col in required_columns:
            if col not in df.columns:
                raise ValueError(f"Missing required column: '{col}'")

        print(f"[blue]Loaded {len(df)} records from {input_csv_path}[/blue]")

        # Filter rows to process (skip Disqualified)
        to_process = df[df["label"] != "Disqualified"]

        print(f"[blue]Loaded {len(df)} records from {input_csv_path}[/blue]")
        print(f"[yellow]Skipping {len(df) - len(to_process)} disqualified rows during processing.[/yellow]")

        # Apply classification only to rows to process
        results = {}
        for idx, row in to_process.iterrows():
            industry = str(row["industry"])
            size = str(row["size"])
            location = str(row["company_location"])
            result = await self.classify(size, industry, location)
            results[idx] = result

        # Update original df with new classifications
        for idx, label in results.items():
            df.at[idx, "Is_company_qualified"] = label

        # Save full dataset with disqualified rows still present
        df.to_csv(output_csv_path, index=False)
        print(f"[bold blue]Saved full dataset (including skipped disqualified rows) to {output_csv_path}[/bold blue]")
