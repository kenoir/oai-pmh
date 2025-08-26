from datetime import datetime
from pathlib import Path

import pytest
from pytest_httpx import HTTPXMock

from oai_pmh_client.client import OAIClient
from oai_pmh_client.exceptions import BadArgumentError

# Using arXiv as the test endpoint for integration tests.
BASE_URL = "https://oaipmh.arxiv.org/oai"

@pytest.fixture
def client():
    """
    Returns an OAIClient instance for testing.
    """
    return OAIClient(BASE_URL)

@pytest.fixture
def mock_client(httpx_mock: HTTPXMock):
    """
    Returns an OAIClient instance with a mocked HTTPX client.
    """
    return OAIClient(BASE_URL)

def load_test_data(filename: str) -> bytes:
    """
    Loads test data from the tests/data directory.
    """
    return (Path(__file__).parent / "data" / filename).read_bytes()

def test_identify(client):
    """
    Tests the identify method.
    """
    response = client.identify()
    assert response is not None
    ns = {"oai": "http://www.openarchives.org/OAI/2.0/"}
    repository_name = response.findtext(".//oai:repositoryName", namespaces=ns)
    assert repository_name == "arXiv"

def test_list_metadata_formats(client):
    """
    Tests the list_metadata_formats method.
    """
    response = client.list_metadata_formats()
    assert response is not None
    ns = {"oai": "http://www.openarchives.org/OAI/2.0/"}
    prefixes = [
        elem.text
        for elem in response.findall(".//oai:metadataPrefix", namespaces=ns)
    ]
    assert "oai_dc" in prefixes

def test_list_sets(client):
    """
    Tests the list_sets method.
    """
    count = 0
    for _ in client.list_sets():
        count += 1
        if count > 0:
            break
    assert count > 0

def test_get_record(client):
    """
    Tests the get_record method.
    """
    identifier = "oai:arXiv.org:cs/0012001"
    response = client.get_record(identifier, "oai_dc")
    assert response is not None
    ns = {"oai": "http://www.openarchives.org/OAI/2.0/"}
    identifier_element = response.findtext(".//oai:identifier", namespaces=ns)
    assert identifier_element == identifier

def test_list_identifiers(client):
    """
    Tests the list_identifiers method.
    """
    count = 0
    for _ in client.list_identifiers(metadata_prefix="oai_dc"):
        count += 1
        if count > 0:
            break
    assert count > 0

def test_list_records(client):
    """
    Tests the list_records method.
    """
    count = 0
    for _ in client.list_records(metadata_prefix="oai_dc"):
        count += 1
        if count > 0:
            break
    assert count > 0

def test_oai_error(mock_client: OAIClient, httpx_mock: HTTPXMock):
    """
    Tests that the client raises the correct exception for an OAI error.
    """
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE_URL}?verb=ListRecords&metadataPrefix=invalid",
        content=load_test_data("error_bad_argument.xml"),
    )
    with pytest.raises(BadArgumentError, match="The request includes illegal arguments."):
        list(mock_client.list_records(metadata_prefix="invalid"))

def test_list_records_with_datetime(mock_client: OAIClient, httpx_mock: HTTPXMock):
    """
    Tests that the client correctly formats datetime objects.
    """
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE_URL}?verb=ListRecords&metadataPrefix=oai_dc&from=2024-01-01T12%3A00%3A00Z",
        content=load_test_data("list_records_final.xml"),
    )
    from_date = datetime(2024, 1, 1, 12, 0, 0)
    records = list(mock_client.list_records(metadata_prefix="oai_dc", from_date=from_date))
    assert len(records) == 1

def test_list_records_with_resumption(mock_client: OAIClient, httpx_mock: HTTPXMock):
    """
    Tests that the client correctly handles resumption tokens.
    """
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE_URL}?verb=ListRecords&metadataPrefix=oai_dc",
        content=load_test_data("list_records_resumption.xml"),
    )
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE_URL}?verb=ListRecords&resumptionToken=token123",
        content=load_test_data("list_records_final.xml"),
    )
    records = list(mock_client.list_records(metadata_prefix="oai_dc"))
    assert len(records) == 2
    assert records[0].findtext(".//oai:identifier", namespaces={"oai": "http://www.openarchives.org/OAI/2.0/"}) == "oai:example.org:1"
    assert records[1].findtext(".//oai:identifier", namespaces={"oai": "http://www.openarchives.org/OAI/2.0/"}) == "oai:example.org:2"
