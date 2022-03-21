#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#

from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
import uuid
from datetime import timedelta, date
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator
from airbyte_cdk.models import SyncMode

class KyribaClient:
    def __init__(self, username: str, password: str, gateway_url: str):
        self.username = username
        self.password = password
        self.url = f"{gateway_url}/oauth/token"

    def login(self) -> TokenAuthenticator:
        data = {"grant_type": "client_credentials"}
        auth = requests.auth.HTTPBasicAuth(self.username, self.password)
        response = requests.post(self.url, auth=auth, data=data)
        response.raise_for_status()
        access_token = response.json()["access_token"]
        return TokenAuthenticator(access_token)


# Basic full refresh stream
class KyribaStream(HttpStream):
    def __init__(
            self,
            gateway_url: str,
            client: KyribaClient,
            version: str = 1,
            start_date: str = None,
    ):
        self.gateway_url = gateway_url
        self.version = version
        self.start_date = start_date
        self.client = client
        super().__init__(self.client.login())

    primary_key = "uuid"
    max_retries = 5

    @property
    def url_base(self) -> str:
        return f"{self.gateway_url}/api/v{self.version}/"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        metadata = response.json()["metadata"]
        next_page = metadata["links"].get("next")
        next_offset = metadata["pageOffset"] + metadata["pageLimit"]
        return { "page.offset": next_offset } if next_page else None

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return next_page_token

    def should_retry(self, response: requests.Response) -> bool:
        # Kyriba uses basic auth to generate an expiring bearer token
        # There is no refresh token, so users need to log in again when the token expires
        if response.status_code == 401:
            old_tokens = self._session.auth
            self._session.auth = self.client.login()
            new_tokens = self._session.auth._tokens
            print(f"Token comparison: {old_tokens == new_tokens}")
            # change the response status code to 429, so should_give_up in rate_limiting.py
            # does not evaluate to true
            response.status_code = 429
            return True
        return response.status_code == 429 or 500 <= response.status_code < 600

    def unnest(self, key: str, data: Mapping[str, Any]) -> Mapping[str, Any]:
        '''
        Kyriba loves to nest fields, but nested fields cannot be used in an
        incremental cursor. This method grabs the hash where the increment field
        is nested and puts it at the top level
        '''
        nested = data.pop(key)
        return {**data, **nested}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        return response.json().get("results")


# Basic incremental stream
class IncrementalKyribaStream(KyribaStream, ABC):
    cursor_field = "updateDateTime"

    # Checkpoint stream reads after N records. This prevents re-reading of data if the stream fails for any reason.
    @property
    def state_checkpoint_interval(self) -> int:
        # 100 is the default page size
        return 100

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        latest_cursor = latest_record.get(self.cursor_field) or ""
        current_cursor = current_stream_state.get(self.cursor_field) or ""
        return {self.cursor_field: max(current_cursor, latest_cursor)}

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = { "sort": self.cursor_field }
        latest_cursor = stream_state.get(self.cursor_field) or self.start_date + " 00:00:00"
        if latest_cursor:
            # the Kyriba atetime output contains T and Z, but the input has a space and no time zone
            fmt_cursor = latest_cursor.replace("T", " ").replace("Z", "")
            filter = f"{self.cursor_field}=gt='{fmt_cursor}'"
            params["filter"] = filter
        if next_page_token:
            params = {**params, **next_page_token}
        return params


class Accounts(KyribaStream):
    def path(self, **kwargs) -> str:
        return "accounts"


class AccountSubStream(HttpSubStream):
    def __init__(self, **kwargs):
        super().__init__(Accounts, **kwargs)
        self.parent = Accounts(**kwargs)

    def get_account_uuids(self) -> Iterable[Optional[Mapping[str, str]]]:
        return [{"account_uuid": a["uuid"]} for a in self.parent.read_records(sync_mode = SyncMode.full_refresh)]

    def next_page_token(self, response: requests.Response):
        pass

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        return [response.json()]


class CashBalancesStream(AccountSubStream):
    def normalize_balance(self, base: Mapping[str, Any], balance: Mapping[str, Any]) -> Mapping[str, Any]:
        date_info = balance.pop("balanceDate")
        return { **base,  **date_info, **balance}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        '''
        cash balalances are returned as an array over a date range. We need to increment
        based on the date range and Airbyte does not support incrementing based on nested
        values, so we need to normalize the data in transit instead of relying on Airbyte's
        normalization.
        '''
        resp = response.json()
        cash_balances = resp.pop("cashBalance")
        return [self.normalize_balance(resp, b) for b in cash_balances]


class CashBalancesEod(CashBalancesStream, IncrementalKyribaStream):
    cursor_field = "date"

    # Checkpoint stream reads after N records. This prevents re-reading of data if the stream fails for any reason.
    @property
    def state_checkpoint_interval(self) -> int:
        return 100

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        latest_cursor = latest_record.get(self.cursor_field) or ""
        current_cursor = current_stream_state.get(self.cursor_field) or ""
        return {self.cursor_field: max(current_cursor, latest_cursor)}

    def stream_slices(self, stream_state: Mapping[str, Any], **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        slices = []
        account_uuids = self.get_account_uuids()
        # we can query a max of 31 days at a time
        days_inc = 31
        start_str = stream_state.get(self.cursor_field) or self.start_date
        start_date = date.fromisoformat(start_str)
        yesterday = date.today() - timedelta(days=1)
        while start_date <= yesterday:
            end_date = start_date + timedelta(days=days_inc)
            end_date = end_date if end_date < date.today() else date.today()
            date_params = {
                "startDate": start_date.isoformat(),
                "endDate": end_date.isoformat(),
            }
            slices.extend([{**u, **date_params} for u in account_uuids])
            # ensure the next start date is never greater than today since we are getting EOD balances
            start_date = end_date + timedelta(days=1)
        return slices

    def path(self, stream_slice: Mapping[str, Any], **kwargs) -> str:
        account_uuid = stream_slice['account_uuid']
        return f"cash-balances/accounts/{account_uuid}/balances"

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {
            "endDate": stream_slice["endDate"],
            "startDate": stream_slice["startDate"],
            "intraday": False,
            "actual": True,
            "estimatedForecasts": False,
            "confirmedForecasts": False,
            "dateType": "VALUE",
        }


class CashBalancesIntraday(CashBalancesStream, KyribaStream):
    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        return self.get_account_uuids()

    def path(self, stream_slice: Mapping[str, Any], **kwargs) -> str:
        account_uuid = stream_slice['account_uuid']
        return f"cash-balances/accounts/{account_uuid}/balances"

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {
            "endDate": date.today().isoformat(),
            "startDate": date.today().isoformat(),
            "intraday": True,
            "actual": True,
            "estimatedForecasts": False,
            "confirmedForecasts": False,
            "dateType": "VALUE",
        }


class BankBalancesStream(AccountSubStream):
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        # the updatedDateTime is unnecessarily nested under date
        # Airbyte cannot accomodate nested cursors, so this needs to be fixed
        results = response.json()
        return [self.unnest("bankBalance", results)]


class BankBalancesEod(BankBalancesStream, IncrementalKyribaStream):
    cursor_field = "balanceDate"

    # Checkpoint stream reads after N records. This prevents re-reading of data if the stream fails for any reason.
    @property
    def state_checkpoint_interval(self) -> int:
        return 100

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        latest_cursor = latest_record.get(self.cursor_field) or ""
        current_cursor = current_stream_state.get(self.cursor_field) or ""
        return {self.cursor_field: max(current_cursor, latest_cursor)}

    def stream_slices(self, stream_state: Mapping[str, Any], **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        slices = []
        account_uuids = self.get_account_uuids()
        # bank balances require the date to be specified
        bal_date_str = stream_state.get(self.cursor_field) or self.start_date
        bal_date = date.fromisoformat(bal_date_str)
        yesterday = date.today() - timedelta(days=1)
        while bal_date <= yesterday:
            slices.extend([{**u, self.cursor_field: bal_date.isoformat()} for u in account_uuids])
            bal_date = bal_date + timedelta(days=1)
        return slices

    def path(self, stream_slice: Mapping[str, Any], **kwargs) -> str:
        account_uuid = stream_slice['account_uuid']
        return f"bank-balances/accounts/{account_uuid}/balances"

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {
            "date": stream_slice[self.cursor_field],
            "type": "END_OF_DAY",
        }


class BankBalancesIntraday(BankBalancesStream, KyribaStream):
    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        return self.get_account_uuids()

    def path(self, stream_slice: Mapping[str, Any], **kwargs) -> str:
        account_uuid = stream_slice['account_uuid']
        return f"bank-balances/accounts/{account_uuid}/balances"

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {
            "date": date.today().isoformat(),
            "type": "INTRADAY",
        }


class CashFlows(IncrementalKyribaStream):
    def path(self, **kwargs) -> str:
        return "cash-flows"

    def stream_slices(self, stream_state: Mapping[str, Any], **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        # cash flow date range has to be less than a year
        year_ago = (date.today() - timedelta(days=365))
        start = date.fromisoformat(self.start_date)
        slices = []
        while start < year_ago:
            end = start + timedelta(days=365)
            slices.append({"startDate": start.isoformat(), "endDate": end.isoformat()})
            start = end + timedelta(days=1)
        # endDate will default to the current date
        slices.append({"startDate": start.isoformat()})
        return slices

    def request_params(self, stream_slice: Optional[Mapping[str, Any]], **kwargs) -> MutableMapping[str, Any]:
        params = super().request_params(**kwargs) or {}
        params["dateType"] = "UPDATE"
        params = {**params, **stream_slice}
        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        # the updatedDateTime is unnecessarily nested under date
        # Airbyte cannot accomodate nested cursors, so this needs to be fixed
        results = response.json().get("results")
        return [self.unnest("date", r) for r in results]


# Source
class SourceKyriba(AbstractSource):
    def gateway_url(self, config: Mapping[str, Any]) -> str:
        return f"https://{config['domain']}/gateway"

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        client = KyribaClient(config["username"], config["password"], self.gateway_url(config))
        client.login()
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        gateway_url = self.gateway_url(config)
        client = KyribaClient(config["username"], config["password"], gateway_url)
        kwargs = {
            "gateway_url": gateway_url,
            "version": config.get("version"),
            "client": client,
            "start_date": config.get("start_date"),
        }
        return [
            Accounts(**kwargs),
            CashFlows(**kwargs),
            CashBalancesEod(**kwargs),
            CashBalancesIntraday(**kwargs),
            BankBalancesEod(**kwargs),
            BankBalancesIntraday(**kwargs),
        ]
