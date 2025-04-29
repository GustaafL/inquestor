import responses
from pytest import mark, raises
from src.inquestor.inquestor import ingest
from dataclasses import dataclass
from requests import Response
from responses import matchers
from urllib3.util import Url


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


def next_page_params(
    initial: bool, keyword="params", arg_value=None, response: Response | None = None
):
    if initial or arg_value is None:
        return {"param1": 0}, keyword
    else:
        arg_value["param1"] += 10
    if arg_value["param1"] > 20:
        return False, keyword
    return arg_value, keyword


def next_page_url(
    initial: bool, keyword="url", arg_value=None, response: Response | None = None
):
    if initial or arg_value is None:
        arg_value = "https://api.test/0"
        url_page_value = 0
    else:
        url_page_value = arg_value.split("/")[-1]
        url_page_value = int(url_page_value) + 1
        arg_value = f"https://api.test/{url_page_value}"
    if url_page_value > 2:
        return False, keyword
    return arg_value, keyword


def next_page_from_response(
    initial: bool, keyword="url", arg_value=None, response: Response | None = None
):
    if initial:
        return "https://api.test", keyword
    elif response:
        next_url = response.json().get("next_url", False)
    else:
        next_url = False
    return next_url, keyword


def next_page_url_as_object(
    initial: bool, keyword="url", arg_value=None, response: Response | None = None
):
    if initial or arg_value is None:
        url_page_value = 0
        path = "/0"
        arg_value = Url(scheme="https", host="api.test", port=None, path=path)
    else:
        path = arg_value.path
        if isinstance(path, str):
            url_page_value = int(path.removeprefix("/")) + 1
            arg_value = Url(
                scheme="https", host="api.test", port=None, path=f"/{url_page_value}"
            )
            if url_page_value > 2:
                return False, keyword
    return arg_value, keyword


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
