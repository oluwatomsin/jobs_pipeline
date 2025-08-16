import json
import yaml
import asyncio
import pandas as pd
from rich import print
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.prompts import PromptTemplate
from langchain.output_parsers import PydanticOutputParser
from dotenv import load_dotenv
from data_schema.schema import JobQualifier

load_dotenv()

new_model = "gemini-2.5-flash-lite"
old_model = "gemini-2.5-flash-preview-05-20"

class JobClassifier:
    def __init__(self, yaml_path: str = "config/job_qualification.yml", limit_rows: int | None = 10,
                 max_concurrency: int = 60):
        # ✅ limit_rows is configurable for testing
        self.limit_rows = limit_rows
        self.semaphore = asyncio.Semaphore(max_concurrency)

        # Load YAML configuration
        with open(yaml_path, "r") as file:
            data = yaml.safe_load(file)

        job_post_analysis = data["prompt_v2"]["job_post_analysis"]
        instruction_json = json.dumps(job_post_analysis, indent=2)

        # LLM Model setup
        self.llm = ChatGoogleGenerativeAI(
            model=new_model,
            temperature=0.1,
            max_retries=2
        )

        # Output parser
        self.parser = PydanticOutputParser(pydantic_object=JobQualifier)

        # Prompt template
        self.prompt = PromptTemplate(
            template=(
                "You are an AI agent. Use the following JSON configuration to analyze job posts.\n"
                "{instruction_json}\n\nHere is the job post:\n{query}\n\n{format_instructions}\n"
            ),
            input_variables=["query"],
            partial_variables={
                "instruction_json": instruction_json,
                "format_instructions": self.parser.get_format_instructions(),
            },
        )

    async def classify_job(self, job_post: str) -> str:
        """Classifies a single job post into ['SDR Strategy', 'AE Strategy', 'Disqualified'] with concurrency control."""
        async with self.semaphore:
            print(f"[yellow]Starting classification for a job post of length {len(job_post)}...[/yellow]")
            prompt_str = self.prompt.format(query=job_post)

            # Use ainvoke for asynchronous model calls
            output = await self.llm.ainvoke([{"role": "user", "content": prompt_str}])

            content = output.content if hasattr(output, "content") else output
            content = content.strip()

            # Clean up JSON markers if present
            if content.startswith("```json"):
                content = content[7:]
            if content.startswith("```"):
                content = content[3:]
            if content.endswith("```"):
                content = content[:-3]
            content = content.strip()

            try:
                parsed = self.parser.parse(content)
                print(f"[green]Finished classification. Result: {parsed.label}[/green]")
                return parsed.label  # JobQualifier must have 'label'
            except Exception as e:
                print("[yellow]Could not parse output as JobQualifier. Raw output:[/yellow]")
                print("[red]Error:[/red]", e)
                return "Disqualified"  # fallback if parsing fails

    def _format_job_post(self, title: str, description: str, job_type: str, salary: str) -> str:
        """Formats job details into a structured prompt text."""
        return f"""
        Job Description: \n
        {description.strip()}\n\n
        Other job info: \n\n
        Job Title: {title}\n
        Job Type: {job_type}\n
        Salary: {salary}
        """

    async def process_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Processes a dataframe:
        1. Combines job info into formatted text
        2. Classifies each job concurrently
        3. Adds 'label' column
        4. Removes 'job_post' column
        """
        required_columns = ["job_title", "job_description", "job_type", "salary"]
        for col in required_columns:
            if col not in df.columns:
                raise ValueError(f"Missing required column: {col}")

        # ✅ Limit rows for testing
        if self.limit_rows:
            df = df.head(self.limit_rows)

        print(f"[cyan]Job Qualification in progress. Processing {len(df)} rows...[/cyan]")

        # Create formatted job_post column
        df["job_post"] = df.apply(
            lambda row: self._format_job_post(
                row["job_title"], row["job_description"], row["job_type"], row["salary"]
            ),
            axis=1
        )

        # ✅ Run all job classifications concurrently using a gather
        tasks = [self.classify_job(post) for post in df["job_post"]]
        labels = await asyncio.gather(*tasks)

        # Add labels
        df["label"] = labels


        df.drop(columns=["job_post"], inplace=True)

        removed_count = len(df[df["label"] == "Disqualified"])

        print(f"[green]Job qualification done,[/green] [bold]{removed_count}[/bold] unqualified jobs were found.")
        return df
