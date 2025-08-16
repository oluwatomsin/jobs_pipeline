import asyncio
import time
from langchain_google_genai import ChatGoogleGenerativeAI
from dotenv import load_dotenv

load_dotenv()

# Define the semaphore with a limit of 2
semaphore = asyncio.Semaphore(5)

async def send_request(prompt):
    async with semaphore:
        print(f"Starting request for: {prompt}")
        llm = ChatGoogleGenerativeAI(model="gemini-2.5-flash")
        response = await llm.ainvoke(prompt)
        print(f"Finished request for: {prompt}")
        return response


async def main(prompts):
    tasks = [send_request(prompt) for prompt in prompts]
    results = await asyncio.gather(*tasks)
    return results


if __name__ == "__main__":
    prompts = [
        "Tell me a joke.",
        "Write a short poem.",
        "Summarize the news.",
        "What is the capital of France?",
        "What is 2+2?",
        "Write a one-sentence story.",
        "What is the color of the sky?",
        "Tell me a fun fact about dogs.",
        "What is the boiling point of water?",
        "Write a simple riddle."
    ]
    results = asyncio.run(main(prompts))
    for result in results:
        print(result.content)
