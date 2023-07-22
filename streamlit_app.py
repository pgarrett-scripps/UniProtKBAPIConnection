import streamlit as st
from uniprot_conn import UniProtKBAPIConnection

FIELDS = {
    'Entry': 'accession',
    'Entry Name': 'id',
    'Gene Names': 'gene_names',
    'Gene Names (primary)': 'gene_primary',
    'Gene Names (synonym)': 'gene_synonym',
    'Gene Names (ordered locus)': 'gene_oln',
    'Gene Names (ORF)': 'gene_orf',
    'Organism': 'organism_name',
    'Organism ID': 'organism_id',
    'Protein names': 'protein_name',
    'Proteomes': 'xref_proteomes',
    'Taxonomic lineage': 'lineage',
    'Taxonomic lineage (IDs)': 'lineage_ids',
    'Virus hosts': 'virus_hosts'
}

ORGANISM_IDS = {
    'Mouse': '10090',
    'Rat': '10116'
}

st.title("UniProtKB API Connection Example")

st.markdown("""
This app allows you to query the UniProt API and view the results.

To use the app, select the organism, choose the fields you are interested in, and enter the mass range.

The app will then query the UniProt API and display the results in a table below.

Please not that this is a very basic example of what the uniprot api can do, there are many many more query options
available, please see the [UniProt API User Manual](https://www.uniprot.org/help/api_queries) for more information. 
""")


with st.expander('Show UniProtKBAPIConnection class'):
    st.code("""
    
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
        session = requests.Session()
        session.mount("https://", HTTPAdapter(max_retries=self.retries))
        return session

    def get_next_link(self, headers: dict) -> Optional[str]:
        if "Link" in headers:
            match = self.LINK_REGEX.match(headers["Link"])
            if match:
                return match.group(1)

    def get_batch(self, batch_url: str) -> Generator[Tuple[requests.Response, str], None, None]:
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
    """)



# create an instance of UniProtAPIConnection
uniprot_conn = UniProtKBAPIConnection("UniProt Connection")

organism_id = st.selectbox("organism:", list(ORGANISM_IDS.keys()), 1)
organism_id = ORGANISM_IDS[organism_id]

is_reviewed = True
selected_fields = st.multiselect('Select fields', options=list(FIELDS.keys()), default=list(FIELDS.keys()))


c1, c2 = st.columns(2)
min_mass = c1.number_input('Minimum mass', value=0)
max_mass = c2.number_input('Maximum mass', value=5_000, max_value=10_000)
st.caption('Mass is in Daltons (Range is intentionally limited to 10,000 for demo purposes))')


fields = ','.join(FIELDS[field] for field in selected_fields)
query = f'(reviewed:{str(is_reviewed).lower()}) AND (organism_id:{organism_id}) AND (mass:[{min_mass} TO {max_mass}])&fields={fields}'
st.markdown(f'**Your query:** `{query}`')
if st.button('Search'):
    try:
        # Get results and display them
        df = uniprot_conn.query(query)
        st.subheader(f"Results: {len(df)}")
        st.dataframe(df)
    except Exception as e:
        st.error(f'Error querying the API: {e}')

