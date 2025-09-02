import os
import requests


class OpenRouterClient:
    """
    A client for interacting with the OpenRouter API.
    """

    def __init__(self, api_key=None):
        """
        Initialize the OpenRouter client.

        Args:
            api_key (str, optional): The API key for authentication. If not provided,
                                     it will be read from the OPENROUTER_API_KEY environment variable.

        Raises:
            ValueError: If no API key is provided and cannot be found in environment variables.
        """
        self.api_key = api_key or os.getenv('OPENROUTER_API_KEY')
        if not self.api_key:
            raise ValueError("API key must be provided or set in OPENROUTER_API_KEY environment variable")

    def create_chat_completion(self, messages, model="openai/gpt-3.5-turbo", **kwargs):
        """
        Create a chat completion using the OpenRouter API.

        Args:
            messages (list): List of message dictionaries for the chat completion.
            model (str, optional): The model to use for completion. Defaults to "openai/gpt-3.5-turbo".
            **kwargs: Additional parameters for the API request.

        Returns:
            dict: The API response as a dictionary.

        Raises:
            Exception: If the API request fails (network issues, invalid key, etc.).
        """
        url = "https://openrouter.ai/api/v1/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        data = {
            "model": model,
            "messages": messages,
            **kwargs
        }
        try:
            response = requests.post(url, headers=headers, json=data)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            raise Exception(f"API request failed: {str(e)}")