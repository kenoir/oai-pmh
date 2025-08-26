# OAI-PMH Client

A modern Python client for OAI-PMH (Open Archives Initiative Protocol for Metadata Harvesting).

## Installation

This project uses `uv` for package management. To install the client and its dependencies, you can use the following commands:

```bash
uv venv
source .venv/bin/activate
uv pip install .
```

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

## Testing

To run the tests, you will need to install the development dependencies:

```bash
uv pip install -e ".[dev]"
```

Then, you can run the tests using `pytest`:

```bash
pytest
```
