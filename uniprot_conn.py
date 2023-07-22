import re
from io import StringIO
from typing import Any, Generator, Tuple, Optional, List

import requests
from requests.adapters import HTTPAdapter
from streamlit.connections import ExperimentalBaseConnection
from streamlit.runtime.caching import cache_data
import pandas as pd
from urllib3 import Retry


class UniProtKBAPIConnection(ExperimentalBaseConnection[requests.Session]):
    """Basic st.experimental_connection implementation for UniProt API"""

    LINK_REGEX = re.compile(r'<(.+)>; rel="next"')

    def __init__(self, connection_name: str,
                 base_url: str = 'https://rest.uniprot.org/uniprotkb/search',
                 total_retries: int = 5,
                 backoff_factor: float = 0.25,
                 status_forcelist: List[int] = None,
                 format: str = 'tsv',
                 batch_size: int = 500, **kwargs):

        self.base_url = base_url

        if status_forcelist is None:
            status_forcelist = [500, 502, 503, 504]

        self.retries = Retry(total=total_retries, backoff_factor=backoff_factor, status_forcelist=status_forcelist)

        self.format = format
        self.batch_size = batch_size

        super().__init__(connection_name, **kwargs)

    def _connect(self, **kwargs: Any) -> requests.Session:
        """Connects to the Session

        :returns: requests.Session
        """
        session = requests.Session()
        session.mount("https://", HTTPAdapter(max_retries=self.retries))
        return session

    def get_next_link(self, headers: dict) -> Optional[str]:
        """Extracts next link from headers if present.

        :returns: The next link url
        """
        if "Link" in headers:
            match = self.LINK_REGEX.match(headers["Link"])
            if match:
                return match.group(1)

    def get_batch(self, batch_url: str) -> Generator[Tuple[requests.Response, str], None, None]:
        """Yields batches of data from the API.

        :returns: A generator yielding responses and total count.
        """
        while batch_url:
            try:
                response = self._instance.get(batch_url)
                response.raise_for_status()
                total = response.headers["x-total-results"]
                yield response, total
                batch_url = self.get_next_link(response.headers)
            except Exception as e:
                print(f"An error occurred: {e}")
                break

    def query(self, query: str, cache_time: int = 3600, **kwargs: Any) -> pd.DataFrame:
        """Queries the API and returns a DataFrame.

        :param query: query string
        :param cache_time: time to cache the result
        :param kwargs: other optional parameters
        :returns: results as a DataFrame
        """

        @cache_data(ttl=cache_time)
        def _query(query: str, **kwargs: Any) -> pd.DataFrame:
            params = {'query': query, 'format': self.format, 'size': self.batch_size, **kwargs}
            url = self.base_url + '?' + '&'.join([f'{k}={v}' for k, v in params.items()])

            data = []
            for batch, total in self.get_batch(url):
                # Convert the batch to a list of dictionaries
                batch_data = pd.read_csv(StringIO(batch.text), sep='\t').to_dict('records')
                data.extend(batch_data)

            # Create the DataFrame once all data has been gathered
            result = pd.DataFrame(data)

            return result

        return _query(query, **kwargs)
