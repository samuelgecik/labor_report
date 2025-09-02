import pandas as pd
from openrouter_client.client import OpenRouterClient


def send_table_to_llm(df: pd.DataFrame) -> str:
    """
    Send a pandas DataFrame to an LLM via OpenRouter.

    Args:
        df (pd.DataFrame): The DataFrame to send.

    Returns:
        str: The response from the LLM.
    """
    # Instantiate the OpenRouter client
    client = OpenRouterClient()

    # Convert DataFrame to CSV format
    table_str = df.to_csv(index=False)

    # Create a basic prompt
    prompt = f"Here is the data: {table_str}"

    # Prepare messages for the chat completion
    messages = [{"role": "user", "content": prompt}]

    # Send the request to the LLM
    response = client.create_chat_completion(messages)

    # Extract and return the response content
    return response['choices'][0]['message']['content']