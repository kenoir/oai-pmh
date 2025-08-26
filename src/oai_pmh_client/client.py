import httpx
from lxml import etree

class OAIClient:
    """
    A client for interacting with an OAI-PMH repository.
    """
    def __init__(self, base_url: str, client: httpx.Client | None = None, timeout: int = 20):
        """
        Initializes the OAIClient.

        :param base_url: The base URL of the OAI-PMH repository.
        :param client: An optional httpx.Client instance.
        :param timeout: The timeout for HTTP requests in seconds.
        """
        self.base_url = base_url
        self._client = client or httpx.Client(timeout=timeout, follow_redirects=True)

    def _request(self, verb: str, **kwargs) -> etree._Element:
        """
        Makes a request to the OAI-PMH repository and returns the parsed XML.

        :param verb: The OAI-PMH verb.
        :param kwargs: Additional request parameters.
        :return: The parsed XML response.
        """
        params = {"verb": verb, **kwargs}
        response = self._client.get(self.base_url, params=params)
        response.raise_for_status()
        return etree.fromstring(response.content)

    def identify(self) -> etree._Element:
        """
        Performs the Identify request.
        """
        return self._request("Identify")

    def list_metadata_formats(self, identifier: str | None = None) -> etree._Element:
        """
        Performs the ListMetadataFormats request.

        :param identifier: An optional identifier to retrieve formats for a specific item.
        """
        params = {}
        if identifier:
            params["identifier"] = identifier
        return self._request("ListMetadataFormats", **params)

    def list_sets(self, resumption_token: str | None = None) -> etree._Element:
        """
        Performs the ListSets request.

        :param resumption_token: An optional resumption token for fetching the next chunk of sets.
        """
        params = {}
        if resumption_token:
            params["resumptionToken"] = resumption_token
        return self._request("ListSets", **params)

    def get_record(self, identifier: str, metadata_prefix: str) -> etree._Element:
        """
        Performs the GetRecord request.

        :param identifier: The identifier of the item.
        :param metadata_prefix: The metadata prefix for the requested format.
        """
        params = {"identifier": identifier, "metadataPrefix": metadata_prefix}
        return self._request("GetRecord", **params)

    def list_identifiers(
        self,
        metadata_prefix: str,
        from_date: str | None = None,
        until_date: str | None = None,
        set_spec: str | None = None,
        resumption_token: str | None = None,
    ) -> etree._Element:
        """
        Performs the ListIdentifiers request.

        :param metadata_prefix: The metadata prefix for the requested format.
        :param from_date: An optional start date for selective harvesting.
        :param until_date: An optional end date for selective harvesting.
        :param set_spec: An optional set specification for selective harvesting.
        :param resumption_token: An optional resumption token.
        """
        params = {"metadataPrefix": metadata_prefix}
        if from_date:
            params["from"] = from_date
        if until_date:
            params["until"] = until_date
        if set_spec:
            params["set"] = set_spec
        if resumption_token:
            params["resumptionToken"] = resumption_token
        return self._request("ListIdentifiers", **params)

    def list_records(
        self,
        metadata_prefix: str,
        from_date: str | None = None,
        until_date: str | None = None,
        set_spec: str | None = None,
        resumption_token: str | None = None,
    ) -> etree._Element:
        """
        Performs the ListRecords request.

        :param metadata_prefix: The metadata prefix for the requested format.
        :param from_date: An optional start date for selective harvesting.
        :param until_date: An optional end date for selective harvesting.
        :param set_spec: An optional set specification for selective harvesting.
        :param resumption_token: An optional resumption token.
        """
        params = {"metadataPrefix": metadata_prefix}
        if from_date:
            params["from"] = from_date
        if until_date:
            params["until"] = until_date
        if set_spec:
            params["set"] = set_spec
        if resumption_token:
            params["resumptionToken"] = resumption_token
        return self._request("ListRecords", **params)
