from typing import List, Union, Generator, Iterator, Optional
from pprint import pprint
import requests, json, warnings
import meilisearch

# Uncomment to disable SSL verification warnings if needed.
# warnings.filterwarnings('ignore', message='Unverified HTTPS request')


class Pipeline:
    def __init__(self):
        self.name = "Meilisearch Pipeline"
        self.api_url = (
            "https://meilisearch.dealwallet.com"  # Set your Meilisearch instance URL
        )
        self.api_key = "tsssadmin"  # Insert your Meilisearch API key here
        self.verify_ssl = True
        self.debug = False
        self.meilisearch_client = meilisearch.Client(self.api_url, self.api_key)

    async def on_startup(self):
        # This function is called when the server is started.
        print(f"on_startup: {__name__}")
        pass

    async def on_shutdown(self):
        # This function is called when the server is shutdown.
        print(f"on_shutdown: {__name__}")
        pass

    async def inlet(self, body: dict, user: Optional[dict] = None) -> dict:
        # This function is called before the Meilisearch request is made. You can modify the form data before sending it.
        print(f"inlet: {__name__}")
        if self.debug:
            print(f"inlet: {__name__} - body:")
            pprint(body)
            print(f"inlet: {__name__} - user:")
            pprint(user)
        return body

    async def outlet(self, body: dict, user: Optional[dict] = None) -> dict:
        # This function is called after the Meilisearch API response is completed. You can modify the messages after receiving them.
        print(f"outlet: {__name__}")
        if self.debug:
            print(f"outlet: {__name__} - body:")
            pprint(body)
            print(f"outlet: {__name__} - user:")
            pprint(user)
        return body

    def pipe(
        self, user_message: str, model_id: str, messages: List[dict], body: dict
    ) -> Union[str, Generator, Iterator]:
        # This function triggers Meilisearch search or other operations.
        print(f"pipe: {__name__}")

        if self.debug:
            print(f"pipe: {__name__} - received message from user: {user_message}")

        # Assume we want to search for user_message in a specific Meilisearch index
        index_name = "my_index"  # Set the Meilisearch index you want to query
        options = {}  # Additional options for the search if needed

        try:
            # Perform the Meilisearch search
            search_results = self.meilisearch_client.index(index_name).search(
                user_message, options
            )

            # Check if results were found
            if search_results["hits"]:
                for hit in search_results["hits"]:
                    # Yield search result one by one, assuming each hit contains a field like 'text' that we need
                    yield hit.get("text", "No text found in this hit")
            else:
                yield "No results found."
        except Exception as e:
            yield f"An error occurred: {str(e)}"
