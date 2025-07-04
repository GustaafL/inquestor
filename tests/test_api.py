import responses
import requests
from datetime import datetime, timedelta
from pytest import mark, raises
from src.inquestor.inquestor import ingest, update_args, update_arg, validate_keys
from dataclasses import dataclass
from requests import Response
from responses import matchers
from urllib3.util import Url, Retry
import time


@dataclass
class ResponsesData:
    json: dict
    status_code: int


@dataclass
class RequestData:
    url: str | Url
    params: dict


@dataclass
class ExchangeData:
    response_data: ResponsesData
    request_data: RequestData


def test_update_arg():
    args_dict = {"params": {"param1": 10, "param2": 0}, "url": "https://api.test"}
    _, value = update_arg(("params", {"param1": 0}), args_dict)
    assert value == {"param1": 10, "param2": 0}
    args_dict = {"params": {"param1": 0, "param2": 0}, "url": "https://api.test_new"}
    _, value = update_arg(("url", "https://api.test"), args_dict)
    assert value == "https://api.test_new"


def test_update_args():
    local_args = {"params": {"param1": 0, "param2": 0}, "url": "https://api.test"}
    local_args = update_args({"url": "https://api.test_new"}, local_args)
    assert local_args["url"] == "https://api.test_new"
    assert local_args["params"] == {"param1": 0, "param2": 0}
    new_args_dict = update_args({"params": {"param1": 10}}, local_args)
    assert isinstance(local_args["params"], dict)
    local_args["params"] |= {"param1": 10}
    assert new_args_dict["params"] == {"param1": 10, "param2": 0}


def test_validate_keys():
    local_args = {"params": {"param1": 0, "param2": 0}, "url": "https://api.test"}
    new_local_args = validate_keys(local_args)
    assert new_local_args == local_args
    with raises(ValueError):
        validate_keys(
            {"params": {"param1": 0, "param2": 0}, "url": 10, "not_a_key": "value"}
        )


exchange_data_params = [
    ExchangeData(
        ResponsesData(json={"data": "response1"}, status_code=200),
        RequestData(url="https://api.test", params={"param1": 0, "param2": 0}),
    ),
    ExchangeData(
        ResponsesData(json={"data": "response2"}, status_code=200),
        RequestData(url="https://api.test", params={"param1": 10, "param2": 0}),
    ),
    ExchangeData(
        ResponsesData(json={"data": "response3"}, status_code=200),
        RequestData(url="https://api.test", params={"param1": 20, "param2": 0}),
    ),
]


exchange_data_url = [
    ExchangeData(
        ResponsesData(json={"data": "response1"}, status_code=200),
        RequestData(url="https://api.test/0", params={"param2": 0}),
    ),
    ExchangeData(
        ResponsesData(json={"data": "response2"}, status_code=200),
        RequestData(url="https://api.test/1", params={"param2": 0}),
    ),
    ExchangeData(
        ResponsesData(json={"data": "response3"}, status_code=200),
        RequestData(url="https://api.test/2", params={"param2": 0}),
    ),
]


exchange_data_from_response = [
    ExchangeData(
        ResponsesData(
            json={"data": "response1", "next_url": "https://api.test/a"},
            status_code=200,
        ),
        RequestData(url="https://api.test", params={"param2": 0}),
    ),
    ExchangeData(
        ResponsesData(
            json={"data": "response2", "next_url": "https://api.test/b"},
            status_code=200,
        ),
        RequestData(url="https://api.test/a", params={"param2": 0}),
    ),
    ExchangeData(
        ResponsesData(json={"data": "response3"}, status_code=200),
        RequestData(url="https://api.test/b", params={"param2": 0}),
    ),
]

exchange_data_url_as_object = [
    ExchangeData(
        ResponsesData(json={"data": "response1"}, status_code=200),
        RequestData(
            url=Url(scheme="https", host="api.test", port=None, path="/0"),
            params={"param2": 0},
        ),
    ),
    ExchangeData(
        ResponsesData(json={"data": "response2"}, status_code=200),
        RequestData(
            url=Url(scheme="https", host="api.test", port=None, path="/1"),
            params={"param2": 0},
        ),
    ),
    ExchangeData(
        ResponsesData(json={"data": "response3"}, status_code=200),
        RequestData(
            url=Url(scheme="https", host="api.test", port=None, path="/2"),
            params={"param2": 0},
        ),
    ),
]


def next_page_params(keyword_arg_dict=None, response: Response | None = None):
    if keyword_arg_dict is None:
        return {"params": {"param1": 0}}
    else:
        arg_value = keyword_arg_dict["params"]
        arg_value["param1"] += 10
    if arg_value["param1"] > 20:
        return False
    return {"params": arg_value}


def next_page_url(keyword_arg_dict=None, response: Response | None = None):
    if keyword_arg_dict is None:
        return {"url": "https://api.test/0"}
    else:
        arg_value = keyword_arg_dict["url"]
        url_page_value = arg_value.split("/")[-1]
        url_page_value = int(url_page_value) + 1
        arg_value = f"https://api.test/{url_page_value}"
    if url_page_value > 2:
        return False
    return {"url": arg_value}


def next_page_from_response(keyword_arg_dict=None, response: Response | None = None):
    if keyword_arg_dict is None:
        return {"url": "https://api.test"}
    else:
        if response is None:
            raise TypeError("response is None")
        next_url = response.json().get("next_url", False)
    return {"url": next_url} if next_url else False


def next_page_url_as_object(keyword_arg_dict=None, response: Response | None = None):
    if keyword_arg_dict is None:
        url_page_value = 0
        path = "/0"
        arg_value = Url(scheme="https", host="api.test", port=None, path=path)
    else:
        arg_value = keyword_arg_dict["url"]
        path = arg_value.path
        if isinstance(path, str):
            url_page_value = int(path.removeprefix("/")) + 1
            arg_value = Url(
                scheme="https", host="api.test", port=None, path=f"/{url_page_value}"
            )
            if url_page_value > 2:
                return False
    return {"url": arg_value}


@mark.parametrize(
    "exchange_data, next_page",
    [
        (exchange_data_params, next_page_params),
        (exchange_data_url, next_page_url),
        (exchange_data_from_response, next_page_from_response),
        (exchange_data_url_as_object, next_page_url_as_object),
    ],
)
@responses.activate
def test_ingest(exchange_data, next_page):
    for item in exchange_data:
        responses.add(
            responses.GET,
            str(item.request_data.url),
            json=item.response_data.json,
            status=200,
            match=[matchers.query_param_matcher(item.request_data.params)],
        )
    data = ingest(
        method="GET", url="https://api.test", next_page=next_page, params={"param2": 0}
    )

    for i, item in enumerate(data):
        assert item["data"] == exchange_data[i].response_data.json["data"]


@mark.parametrize(
    "exchange_data",
    [
        (exchange_data_params),
    ],
)
@responses.activate
def test_next_page_function_correct(exchange_data):
    for item in exchange_data:
        responses.add(
            responses.GET,
            str(item.request_data.url),
            json=item.response_data.json,
            status=200,
            match=[matchers.query_param_matcher(item.request_data.params)],
        )
    data = ingest(
        method="GET",
        url="https://api.test",
        next_page="not a function",
        params={"param2": 0},
    )
    with raises(TypeError):
        for _i, item in enumerate(data):
            pass


@responses.activate
def test_authenticate():
    responses.add(
        responses.GET,
        "https://api.test",
        json={"data": "response1"},
        status=200,
        match=[matchers.header_matcher({"authorization": "Bearer token"})],
    )

    def next_page(keyword_arg_dict=None, response: Response | None = None):
        if keyword_arg_dict is None:
            return {"url": "https://api.test"}
        else:
            return False

    def authenticate(reauth_dict=None, response: Response | None = None):
        return {"headers": {"authorization": "Bearer token"}}, reauth_dict

    data = ingest(
        method="GET",
        url="https://api.test",
        next_page=next_page,
        params={"param2": 0},
        authenticate=authenticate,
    )
    for _i, item in enumerate(data):
        assert item["data"] == "response1"


@responses.activate
def test_reauthenticate():
    responses.add(
        responses.GET,
        "https://api.test",
        json={"data": "response1"},
        status=200,
        match=[matchers.header_matcher({"authorization": "Bearer token"})],
    )
    responses.add(
        responses.GET,
        "https://api.test2",
        json={"data": "response2"},
        status=200,
        match=[matchers.header_matcher({"authorization": "Bearer token2"})],
    )

    def next_page(keyword_arg_dict=None, response: Response | None = None):
        if keyword_arg_dict is None:
            return {"url": "https://api.test"}
        if keyword_arg_dict["url"] == "https://api.test":
            return {"url": "https://api.test2"}
        else:
            return False

    def authenticate(reauth_dict=None, response: Response | None = None):
        if reauth_dict is None:
            return {"headers": {"authorization": "Bearer token"}}, {"reauth": True}
        else:
            return {"headers": {"authorization": "Bearer token2"}}, reauth_dict

    data = ingest(
        method="GET",
        url="https://api.test",
        next_page=next_page,
        params={"param2": 0},
        authenticate=authenticate,
    )
    response_data = [
        {"data": "response1"},
        {"data": "response2"},
    ]
    for i, item in enumerate(data):
        assert item["data"] == response_data[i]["data"]


@responses.activate
def test_reautheticate_time_condition():
    responses.add(
        responses.GET,
        "https://api.test",
        json={"data": "response1"},
        status=200,
        match=[matchers.header_matcher({"authorization": "Bearer auth_token_initial"})],
    )
    responses.add(
        responses.GET,
        "https://api.test2",
        json={"data": "response2"},
        status=200,
        match=[matchers.header_matcher({"authorization": "Bearer auth_token_updated"})],
    )
    responses.add(
        responses.GET,
        "https://api.authenticate",
        json={"token": "auth_token_initial"},
        status=200,
        match=[matchers.header_matcher({"refresh_token": "refresh_token"})],
    )
    responses.add(
        responses.GET,
        "https://api.authenticate",
        json={"token": "auth_token_updated"},
        status=200,
        match=[matchers.header_matcher({"refresh_token": "refresh_token"})],
    )

    def next_page(keyword_arg_dict=None, response: Response | None = None):
        if keyword_arg_dict is None:
            return {"url": "https://api.test"}
        if keyword_arg_dict["url"] == "https://api.test":
            return {"url": "https://api.test2"}
        else:
            return False

    def authenticate(reauth_dict=None, response: Response | None = None):
        if reauth_dict is None:
            response = requests.get(
                "https://api.authenticate",
                headers={"refresh_token": "refresh_token"},
            )
            token = response.json()["token"]
            now = datetime.now()
            expiry_time = now + timedelta(seconds=2)
            reauth = {"expiry_time": expiry_time}
            return {"headers": {"authorization": f"Bearer {token}"}}, reauth
        else:
            now = datetime.now()
            if now >= reauth_dict["expiry_time"]:
                response = requests.get(
                    "https://api.authenticate",
                    headers={"refresh_token": "refresh_token"},
                )
                token = response.json()["token"]
                expiry_time = now + timedelta(seconds=1)
                reauth_dict["expiry_time"] = expiry_time
                return {"headers": {"authorization": f"Bearer {token}"}}, reauth_dict
            return None, reauth_dict

    data = ingest(
        method="GET",
        url="https://api.test",
        next_page=next_page,
        params={"param2": 0},
        authenticate=authenticate,
    )
    response_data = [
        {"data": "response1"},
        {"data": "response2"},
    ]
    for i, item in enumerate(data):
        assert item["data"] == response_data[i]["data"]
        time.sleep(2)


@responses.activate
def test_retry():
    responses.add(
        responses.GET,
        "https://api.test",
        json={"error": "errorresponse"},
        status=500,
    )
    responses.add(
        responses.GET,
        "https://api.test",
        json={"data": "response_success"},
        status=200,
    )

    def next_page(keyword_arg_dict=None, response: Response | None = None):
        return False

    data = ingest(
        method="GET",
        url="https://api.test",
        next_page=next_page,
        params={"param2": 0},
        retries=Retry(total=3, backoff_factor=1, status_forcelist=[500]),
    )

    for i, item in enumerate(data):
        assert item["data"] == "response_success"


@responses.activate
def test_rate_limit(mocker):
    responses.add(
        responses.GET,
        "https://api.test",
        json={"data": "response1"},
        status=200,
        headers={"X-RateLimit-Limit": "0"},
    )
    responses.add(
        responses.GET,
        "https://api.test2",
        json={"data": "response2"},
        status=200,
        headers={"X-RateLimit-Limit": "2"},
    )
    mock_sleep = mocker.patch("time.sleep", return_value=None)

    def next_page(keyword_arg_dict=None, response: Response | None = None):
        if keyword_arg_dict is None:
            return {"url": "https://api.test"}
        if keyword_arg_dict["url"] == "https://api.test":
            return {"url": "https://api.test2"}
        else:
            return False

    def rate_limit(ratelimit_dict=None, response: Response | None = None):
        if response is None:
            return {}
        else:
            ratelimit_dict = ratelimit_dict or {}
            ratelimit_dict["limit"] = int(response.headers.get("X-RateLimit-Limit", 0))

            if ratelimit_dict["limit"] <= 0:
                time.sleep(1)
            return {"ratelimit": ratelimit_dict}

    data = ingest(
        method="GET",
        url="https://api.test",
        next_page=next_page,
        rate_limit=rate_limit,
    )
    response_list = ["response1", "response2"]
    for i, item in enumerate(data):
        assert item["data"] == response_list[i]

    mock_sleep.assert_called_once_with(1)
