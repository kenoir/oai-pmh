# OAI-PMH Client

A modern Python client for OAI-PMH (Open Archives Initiative Protocol for Metadata Harvesting).

## Installation

This project uses `uv` for package management. To install the client and its dependencies, you can use the following commands:

```bash
uv venv
source .venv/bin/activate
uv pip install .
```

## Documentation

Full documentation is available at [https://kenoir.github.io/oai-pmh/](https://kenoir.github.io/oai-pmh/).

## Usage

Here is a simple example of how to use the client:

```python
from oai_pmh_client.client import OAIClient

# Create a client for the arXiv OAI-PMH endpoint.
client = OAIClient("https://oaipmh.arxiv.org/oai")

# Get the repository's identity.
identity = client.identify()
print(identity)

# List the available metadata formats.
formats = client.list_metadata_formats()
print(formats)

# List the sets in the repository.
sets = client.list_sets()
print(sets)
```

### More Examples

#### Listing Records

You can list records with optional `from_date`, `until_date`, and `set_spec` filters.

```python
from datetime import datetime

# List all records updated since the start of 2024 in the "cs.AI" set
records = client.list_records(
    metadata_prefix="oai_dc",
    from_date=datetime(2024, 1, 1),
    set_spec="cs.AI"
)
for record in records:
    print(record.header.identifier, record.header.datestamp)
```

#### Getting a Single Record

Retrieve a single record by its identifier and a metadata prefix.

```python
record = client.get_record("oai:arXiv.org:2401.00001", "oai_dc")
print(record.metadata)
```

#### Error Handling

The client will raise an `OAIError` subclass for errors returned by the OAI-PMH server.

```python
from oai_pmh_client.exceptions import IdDoesNotExistError

try:
    record = client.get_record("oai:arXiv.org:this-id-does-not-exist", "oai_dc")
except IdDoesNotExistError as e:
    print(f"Caught expected error: {e}")
```

## Testing

To run the tests, you will need to install the development dependencies:

```bash
uv pip install -e ".[dev]"
```

Then, you can run the tests using `pytest`:

```bash
pytest
```
