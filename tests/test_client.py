from datetime import datetime
from pathlib import Path

import pytest
from pytest_httpx import HTTPXMock

from oai_pmh_client import (
    OAIClient,
    BadArgumentError,
    Identify,
    Header,
    MetadataFormat,
    Set,
    Record,
)

# Using arXiv as the test endpoint for integration tests.
BASE_URL = "https://export.arxiv.org/oai2"
CANONICAL_BASE_URL = "https://oaipmh.arxiv.org/oai"


@pytest.fixture
def client():
    """
    Returns an OAIClient instance for testing.
    """
    return OAIClient(BASE_URL)

@pytest.fixture
def mock_client_get(httpx_mock: HTTPXMock):
    """
    Returns an OAIClient instance with a mocked HTTPX client using GET.
    """
    return OAIClient(BASE_URL, use_post=False)

@pytest.fixture
def mock_client_post(httpx_mock: HTTPXMock):
    """
    Returns an OAIClient instance with a mocked HTTPX client using POST.
    """
    return OAIClient(BASE_URL, use_post=True)

def load_test_data(filename: str) -> bytes:
    """
    Loads test data from the tests/data directory.
    """
    return (Path(__file__).parent / "data" / filename).read_bytes()

# The following tests are integration tests and will make live HTTP requests.
# They are marked with 'integration' and can be skipped with `pytest -m "not integration"`.

@pytest.mark.integration
def test_identify(client: OAIClient):
    """
    Tests the identify method against a live endpoint.
    """
    response = client.identify()
    assert isinstance(response, Identify)
    assert response.repository_name == "arXiv"
    assert response.base_url == CANONICAL_BASE_URL
    assert response.protocol_version == "2.0"

@pytest.mark.integration
def test_list_metadata_formats(client: OAIClient):
    """
    Tests the list_metadata_formats method against a live endpoint.
    """
    formats = list(client.list_metadata_formats())
    assert len(formats) > 0
    assert all(isinstance(f, MetadataFormat) for f in formats)
    prefixes = [f.prefix for f in formats]
    assert "oai_dc" in prefixes

@pytest.mark.integration
def test_list_sets(client: OAIClient):
    """
    Tests the list_sets method against a live endpoint.
    """
    sets = list(client.list_sets())
    assert len(sets) > 0
    assert all(isinstance(s, Set) for s in sets)

@pytest.mark.integration
def test_get_record(client: OAIClient):
    """
    Tests the get_record method against a live endpoint.
    """
    identifier = "oai:arXiv.org:cs/0012001"
    record = client.get_record(identifier, "oai_dc")
    assert isinstance(record, Record)
    assert record.header.identifier == identifier
    assert not record.header.is_deleted
    assert record.metadata is not None

@pytest.mark.integration
def test_list_identifiers(client: OAIClient):
    """
    Tests the list_identifiers method against a live endpoint.
    """
    # Take just a few items to avoid fetching the whole list
    from itertools import islice
    identifiers = list(islice(client.list_identifiers(metadata_prefix="oai_dc", set_spec="cs"), 5))
    assert len(identifiers) > 0
    assert all(isinstance(i, Header) for i in identifiers)

@pytest.mark.integration
def test_list_records(client: OAIClient):
    """
    Tests the list_records method against a live endpoint.
    """
    # Take just a few items to avoid fetching the whole list
    from itertools import islice
    records = list(islice(client.list_records(metadata_prefix="oai_dc", set_spec="cs"), 5))
    assert len(records) > 0
    assert all(isinstance(r, Record) for r in records)

# The following tests are unit tests using mocked responses.

def test_oai_error(mock_client_get: OAIClient, httpx_mock: HTTPXMock):
    """
    Tests that the client raises the correct exception for an OAI error.
    """
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE_URL}?verb=ListRecords&metadataPrefix=invalid",
        content=load_test_data("error_bad_argument.xml"),
    )
    with pytest.raises(BadArgumentError):
        list(mock_client_get.list_records(metadata_prefix="invalid"))

def test_list_records_with_datetime(mock_client_get: OAIClient, httpx_mock: HTTPXMock):
    """
    Tests that the client correctly formats datetime objects.
    """
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE_URL}?verb=ListRecords&metadataPrefix=oai_dc&from=2024-01-01T12%3A00%3A00Z",
        content=load_test_data("list_records_final.xml"),
    )
    from_date = datetime(2024, 1, 1, 12, 0, 0)
    records = list(mock_client_get.list_records(metadata_prefix="oai_dc", from_date=from_date))
    assert len(records) == 1
    assert isinstance(records[0], Record)

def test_list_records_with_resumption(mock_client_get: OAIClient, httpx_mock: HTTPXMock):
    """
    Tests that the client correctly handles resumption tokens with GET.
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
    records = list(mock_client_get.list_records(metadata_prefix="oai_dc"))
    assert len(records) == 2
    assert records[0].header.identifier == "oai:example.org:1"
    assert records[1].header.identifier == "oai:example.org:2"

def test_list_records_with_resumption_post(mock_client_post: OAIClient, httpx_mock: HTTPXMock):
    """
    Tests that the client correctly handles resumption tokens with POST.
    """
    httpx_mock.add_response(
        method="POST",
        url=BASE_URL,
        content=load_test_data("list_records_resumption.xml"),
        match_content=b"verb=ListRecords&metadataPrefix=oai_dc"
    )
    httpx_mock.add_response(
        method="POST",
        url=BASE_URL,
        content=load_test_data("list_records_final.xml"),
        match_content=b"verb=ListRecords&resumptionToken=token123"
    )
    records = list(mock_client_post.list_records(metadata_prefix="oai_dc"))
    assert len(records) == 2
    assert records[0].header.identifier == "oai:example.org:1"
    assert records[1].header.identifier == "oai:example.org:2"
