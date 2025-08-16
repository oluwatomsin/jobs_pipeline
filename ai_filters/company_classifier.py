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


new_model = "gemini-2.5-flash-lite"
old_model = "gemini-2.5-flash-preview-05-20"

class CompanyClassifier:
    def __init__(
            self,
            config_path: str = "config/company_requirements.yml",
            model_name: str = new_model,
            temperature: float = 0.1,
            max_concurrency: int = 60
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

        # Initialize semaphore for concurrency control
        self.semaphore = asyncio.Semaphore(max_concurrency)

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
        Classify a single company as Qualified or Disqualified with concurrency control.
        """
        async with self.semaphore:
            print(
                f"[yellow]Starting classification for: {company_industry}, {n_employees}, {company_location}[/yellow]")
            company_info = (
                f"company's Industry info: {company_industry}\n"
                f"company's employee head count info: {n_employees}\n"
                f"company's location info: {company_location}"
            )

            prompt_str = self.prompt.format(query=company_info)
            # Use ainvoke for asynchronous model calls
            output = await self.llm.ainvoke([{"role": "user", "content": prompt_str}])
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
                label = parsed.label
                print(
                    f"[green]Finished classification for: {company_industry}, {n_employees}, {company_location}. Result: {label}[/green]")
                return label  # Return the label string only
            except Exception as e:
                print(
                    f"[red]Error parsing result for [{company_industry}, {n_employees}, {company_location}]:[/red] {e}")
                return "Disqualified"  # Fail-safe default

    async def process_dataset(self, input_csv_path: str, output_csv_path: str) -> None:
        """
        Load CSV, classify companies concurrently, and save the updated file.
        """
        df = pd.read_csv(input_csv_path)

        # Clean column names just in case
        df.columns = df.columns.str.strip().str.lower()

        required_columns = ["industry", "size", "company_location"]
        for col in required_columns:
            if col not in df.columns:
                raise ValueError(f"Missing required column: '{col}'")

        print(f"[blue]Loaded {len(df)} records from {input_csv_path}[/blue]")

        # Prepare a list of tasks for asyncio.gather
        tasks = []
        for idx, row in df.iterrows():
            industry = str(row["industry"])
            size = str(row["size"])
            location = str(row["company_location"])
            tasks.append(self.classify(size, industry, location))

        print(f"[cyan]Starting concurrent classification tasks on {len(df)} rows...[/cyan]")

        # Run all tasks concurrently and await all results
        results = await asyncio.gather(*tasks)

        # Update the original DataFrame with the results
        df["Is_company_qualified"] = results

        # Optional: You can filter the DataFrame here if needed
        # df = df[df["Is_company_qualified"] != "Disqualified"]

        # Save the full dataset with new classifications
        df.to_csv(output_csv_path, index=False)
        print(f"[bold blue]Saved full dataset to {output_csv_path}[/bold blue]")
