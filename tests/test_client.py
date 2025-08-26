import pytest
from oai_pmh_client.client import OAIClient

# Using arXiv as the test endpoint as the LOC one seems to be timing out.
BASE_URL = "https://oaipmh.arxiv.org/oai"

@pytest.fixture
def client():
    """
    Returns an OAIClient instance for testing.
    """
    return OAIClient(BASE_URL)

def test_identify(client):
    """
    Tests the identify method.
    """
    response = client.identify()
    assert response is not None
    # A simple check to see if we got a valid response.
    # The namespace is http://www.openarchives.org/OAI/2.0/
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
    response = client.list_sets()
    assert response is not None
    ns = {"oai": "http://www.openarchives.org/OAI/2.0/"}
    sets = response.findall(".//oai:set", namespaces=ns)
    assert len(sets) > 0

def test_get_record(client):
    """
    Tests the get_record method.
    """
    # This is a valid record from the arXiv repository.
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
    response = client.list_identifiers(metadata_prefix="oai_dc")
    assert response is not None
    ns = {"oai": "http://www.openarchives.org/OAI/2.0/"}
    identifiers = response.findall(".//oai:identifier", namespaces=ns)
    assert len(identifiers) > 0

def test_list_records(client):
    """
    Tests the list_records method.
    """
    response = client.list_records(metadata_prefix="oai_dc")
    assert response is not None
    ns = {"oai": "http://www.openarchives.org/OAI/2.0/"}
    records = response.findall(".//oai:record", namespaces=ns)
    assert len(records) > 0
