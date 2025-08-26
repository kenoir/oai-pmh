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
    def __init__(self, base_url: str, client: httpx.Client | None = None, timeout: int = 20):
        """
        Initializes the OAIClient.

        :param base_url: The base URL of the OAI-PMH repository.
        :param client: An optional httpx.Client instance.
        :param timeout: The timeout for HTTP requests in seconds.
        """
        self.base_url = base_url
        self._client = client or httpx.Client(timeout=timeout, follow_redirects=True)

    def _format_datestamp(self, dt: Datestamp) -> str:
        """
        Formats a datetime object into an OAI-PMH datestamp string.
        """
        if isinstance(dt, str):
            return dt
        if dt.tzinfo is None:
            # If the datetime object is naive, assume it's in UTC.
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        # If the datetime object is aware, convert it to UTC.
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

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
        xml = etree.fromstring(response.content)

        # The namespace for OAI-PMH v2.0
        ns = {"oai": "http://www.openarchives.org/OAI/2.0/"}
        error = xml.find("oai:error", namespaces=ns)

        if error is not None:
            code = error.get("code", "")
            message = error.text or ""
            exception_class = OAI_ERROR_MAP.get(code, OAIError)
            raise exception_class(message)

        return xml

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

    def list_sets(self) -> Iterator[etree._Element]:
        """
        Performs the ListSets request and handles resumption tokens.
        """
        params = {}
        ns = {"oai": "http://www.openarchives.org/OAI/2.0/"}

        while True:
            xml = self._request("ListSets", **params)

            for record in xml.findall(".//oai:set", namespaces=ns):
                yield record

            token_element = xml.find(".//oai:resumptionToken", namespaces=ns)
            if token_element is None or not token_element.text:
                break

            params = {"resumptionToken": token_element.text}

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
        from_date: Datestamp | None = None,
        until_date: Datestamp | None = None,
        set_spec: str | None = None,
    ) -> Iterator[etree._Element]:
        """
        Performs the ListIdentifiers request and handles resumption tokens.

        :param metadata_prefix: The metadata prefix for the requested format.
        :param from_date: An optional start date for selective harvesting.
        :param until_date: An optional end date for selective harvesting.
        :param set_spec: An optional set specification for selective harvesting.
        """
        params = {"metadataPrefix": metadata_prefix}
        if from_date:
            params["from"] = self._format_datestamp(from_date)
        if until_date:
            params["until"] = self._format_datestamp(until_date)
        if set_spec:
            params["set"] = set_spec

        ns = {"oai": "http://www.openarchives.org/OAI/2.0/"}

        while True:
            xml = self._request("ListIdentifiers", **params)

            for header in xml.findall(".//oai:header", namespaces=ns):
                yield header

            token_element = xml.find(".//oai:resumptionToken", namespaces=ns)
            if token_element is None or not token_element.text:
                break

            params = {"resumptionToken": token_element.text}

    def list_records(
        self,
        metadata_prefix: str,
        from_date: Datestamp | None = None,
        until_date: Datestamp | None = None,
        set_spec: str | None = None,
    ) -> Iterator[etree._Element]:
        """
        Performs the ListRecords request and handles resumption tokens.

        :param metadata_prefix: The metadata prefix for the requested format.
        :param from_date: An optional start date for selective harvesting.
        :param until_date: An optional end date for selective harvesting.
        :param set_spec: An optional set specification for selective harvesting.
        """
        params = {"metadataPrefix": metadata_prefix}
        if from_date:
            params["from"] = self._format_datestamp(from_date)
        if until_date:
            params["until"] = self._format_datestamp(until_date)
        if set_spec:
            params["set"] = set_spec

        ns = {"oai": "http://www.openarchives.org/OAI/2.0/"}

        while True:
            xml = self._request("ListRecords", **params)

            for record in xml.findall(".//oai:record", namespaces=ns):
                yield record

            token_element = xml.find(".//oai:resumptionToken", namespaces=ns)
            if token_element is None or not token_element.text:
                break

            params = {"resumptionToken": token_element.text}
