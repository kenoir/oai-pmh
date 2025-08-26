from datetime import datetime, timezone
from typing import Union, Iterator

import httpx
from lxml import etree

from .exceptions import (
    OAIError,
    BadArgumentError,
    BadResumptionTokenError,
    BadVerbError,
    CannotDisseminateFormatError,
    IdDoesNotExistError,
    NoRecordsMatchError,
    NoMetadataFormatsError,
    NoSetHierarchyError,
)
from .models import (
    Identify,
    Header,
    MetadataFormat,
    Set,
    Record,
    ResumptionToken,
    NS,
)

OAI_ERROR_MAP = {
    "badArgument": BadArgumentError,
    "badResumptionToken": BadResumptionTokenError,
    "badVerb": BadVerbError,
    "cannotDisseminateFormat": CannotDisseminateFormatError,
    "idDoesNotExist": IdDoesNotExistError,
    "noRecordsMatch": NoRecordsMatchError,
    "noMetadataFormats": NoMetadataFormatsError,
    "noSetHierarchy": NoSetHierarchyError,
}

Datestamp = Union[datetime, str]


class OAIClient:
    """
    A client for interacting with an OAI-PMH repository.
    """

    def __init__(
        self,
        base_url: str,
        client: httpx.Client | None = None,
        timeout: int = 20,
        use_post: bool = False,
    ):
        """
        Initializes the OAIClient.

        :param base_url: The base URL of the OAI-PMH repository.
        :param client: An optional httpx.Client instance.
        :param timeout: The timeout for HTTP requests in seconds.
        :param use_post: Whether to use POST requests instead of GET.
        """
        self.base_url = base_url
        self._client = client or httpx.Client(timeout=timeout, follow_redirects=True)
        self.use_post = use_post

    def _format_datestamp(self, dt: Datestamp) -> str:
        """
        Formats a datetime object into an OAI-PMH datestamp string.
        """
        if isinstance(dt, str):
            return dt
        if dt.tzinfo is None:
            # If the datetime object is naive, assume it's in UTC.
            dt = dt.replace(tzinfo=timezone.utc)
        # If the datetime object is aware, convert it to UTC.
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    def _request(self, verb: str, **kwargs) -> etree._Element:
        """
        Makes a request to the OAI-PMH repository and returns the parsed XML.

        :param verb: The OAI-PMH verb.
        :param kwargs: Additional request parameters.
        :return: The parsed XML response.
        """
        params = {"verb": verb}
        # Filter out None values so they aren't included in the query
        for key, value in kwargs.items():
            if value is not None:
                params[key] = value

        if self.use_post:
            response = self._client.post(self.base_url, data=params)
        else:
            response = self._client.get(self.base_url, params=params)

        response.raise_for_status()
        xml = etree.fromstring(response.content)

        error = xml.find("oai:error", namespaces=NS)
        if error is not None:
            code = error.get("code", "")
            message = error.text or ""
            exception_class = OAI_ERROR_MAP.get(code, OAIError)
            raise exception_class(message)

        return xml

    def identify(self) -> Identify:
        """
        Performs the Identify request and returns a parsed Identify object.
        """
        xml = self._request("Identify")
        identify_element = xml.find("oai:Identify", namespaces=NS)
        if identify_element is None:
            raise OAIError("Invalid response: missing Identify element")
        return Identify.from_xml(identify_element)

    def list_metadata_formats(
        self, identifier: str | None = None
    ) -> Iterator[MetadataFormat]:
        """
        Performs the ListMetadataFormats request and yields MetadataFormat objects.

        :param identifier: An optional identifier to retrieve formats for a specific item.
        """
        params = {}
        if identifier:
            params["identifier"] = identifier
        xml = self._request("ListMetadataFormats", **params)
        for element in xml.findall(".//oai:metadataFormat", namespaces=NS):
            yield MetadataFormat.from_xml(element)

    def list_sets(self) -> Iterator[Set]:
        """
        Performs the ListSets request, handles resumption tokens, and yields Set objects.
        """
        params = {}
        verb = "ListSets"
        while True:
            xml = self._request(verb, **params)
            for element in xml.findall(".//oai:set", namespaces=NS):
                yield Set.from_xml(element)

            token_element = xml.find(".//oai:resumptionToken", namespaces=NS)
            if token_element is None or not token_element.text:
                break

            token = ResumptionToken.from_xml(token_element)
            params = {"resumptionToken": token.value}

    def get_record(self, identifier: str, metadata_prefix: str) -> Record:
        """
        Performs the GetRecord request and returns a Record object.

        :param identifier: The identifier of the item.
        :param metadata_prefix: The metadata prefix for the requested format.
        """
        params = {"identifier": identifier, "metadataPrefix": metadata_prefix}
        xml = self._request("GetRecord", **params)
        record_element = xml.find(".//oai:record", namespaces=NS)
        if record_element is None:
            raise OAIError("Invalid response: missing record element")
        return Record.from_xml(record_element)

    def list_identifiers(
        self,
        metadata_prefix: str,
        from_date: Datestamp | None = None,
        until_date: Datestamp | None = None,
        set_spec: str | None = None,
    ) -> Iterator[Header]:
        """
        Performs the ListIdentifiers request, handles resumption tokens, and yields Header objects.

        :param metadata_prefix: The metadata prefix for the requested format.
        :param from_date: An optional start date for selective harvesting.
        :param until_date: An optional end date for selective harvesting.
        :param set_spec: An optional set specification for selective harvesting.
        """
        params = {
            "metadataPrefix": metadata_prefix,
            "from": self._format_datestamp(from_date) if from_date else None,
            "until": self._format_datestamp(until_date) if until_date else None,
            "set": set_spec,
        }
        verb = "ListIdentifiers"

        while True:
            xml = self._request(verb, **params)
            for element in xml.findall(".//oai:header", namespaces=NS):
                yield Header.from_xml(element)

            token_element = xml.find(".//oai:resumptionToken", namespaces=NS)
            if token_element is None or not token_element.text:
                break

            token = ResumptionToken.from_xml(token_element)
            # When using a resumption token, the original parameters must be omitted
            params = {"resumptionToken": token.value}


    def list_records(
        self,
        metadata_prefix: str,
        from_date: Datestamp | None = None,
        until_date: Datestamp | None = None,
        set_spec: str | None = None,
    ) -> Iterator[Record]:
        """
        Performs the ListRecords request, handles resumption tokens, and yields Record objects.

        :param metadata_prefix: The metadata prefix for the requested format.
        :param from_date: An optional start date for selective harvesting.
        :param until_date: An optional end date for selective harvesting.
        :param set_spec: An optional set specification for selective harvesting.
        """
        params = {
            "metadataPrefix": metadata_prefix,
            "from": self._format_datestamp(from_date) if from_date else None,
            "until": self._format_datestamp(until_date) if until_date else None,
            "set": set_spec,
        }
        verb = "ListRecords"

        while True:
            xml = self._request(verb, **params)
            for element in xml.findall(".//oai:record", namespaces=NS):
                yield Record.from_xml(element)

            token_element = xml.find(".//oai:resumptionToken", namespaces=NS)
            if token_element is None or not token_element.text:
                break

            token = ResumptionToken.from_xml(token_element)
            # When using a resumption token, the original parameters must be omitted
            params = {"resumptionToken": token.value}
