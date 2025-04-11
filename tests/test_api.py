import responses
from pytest import mark
from src.inquestor.inquestor import ingest
from dataclasses import dataclass
from requests import Response


@dataclass
class ResponsesData:
    json: dict
    status_code: int


@dataclass
class RequestData:
    url: str
    params: dict


@dataclass
class ExchangeData:
    response_data: ResponsesData
    request_data: RequestData


exchange_data_params = [
    ExchangeData(
        ResponsesData(json={"data": "response1"}, status_code=200),
        RequestData(url="https://api.test", params={"param1": 0}),
    ),
    ExchangeData(
        ResponsesData(json={"data": "response2"}, status_code=200),
        RequestData(url="https://api.test", params={"param1": 10}),
    ),
    ExchangeData(
        ResponsesData(json={"data": "response3"}, status_code=200),
        RequestData(url="https://api.test", params={"param1": 20}),
    ),
]


exchange_data_url = [
    ExchangeData(
        ResponsesData(json={"data": "response1"}, status_code=200),
        RequestData(url="https://api.test/0", params={"param1": 0}),
    ),
    ExchangeData(
        ResponsesData(json={"data": "response2"}, status_code=200),
        RequestData(url="https://api.test/1", params={"param1": 0}),
    ),
    ExchangeData(
        ResponsesData(json={"data": "response3"}, status_code=200),
        RequestData(url="https://api.test/2", params={"param1": 0}),
    ),
]


exchange_data_from_response = [
    ExchangeData(
        ResponsesData(
            json={"data": "response1", "next_url": "https://api.test/a"},
            status_code=200,
        ),
        RequestData(url="https://api.test", params={"param1": 0}),
    ),
    ExchangeData(
        ResponsesData(
            json={"data": "response2", "next_url": "https://api.test/b"},
            status_code=200,
        ),
        RequestData(url="https://api.test/a", params={"param1": 0}),
    ),
    ExchangeData(
        ResponsesData(json={"data": "response3"}, status_code=200),
        RequestData(url="https://api.test/b", params={"param1": 0}),
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


def next_page_url(initial: bool, keyword="url", arg_value=None, response: Response | None = None):
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


@mark.parametrize(
    "exchange_data, next_page",
    [
        (exchange_data_params, next_page_params),
        (exchange_data_url, next_page_url),
        (exchange_data_from_response, next_page_from_response),
    ],
)
@responses.activate
def test_ingest(exchange_data, next_page):
    for item in exchange_data:
        responses.add(
            responses.GET,
            item.request_data.url,
            json=item.response_data.json,
            status=200,
        )
    data = ingest(method="GET", url="https://api.test", next_page=next_page)

    for i, item in enumerate(data):
        assert item["data"] == exchange_data[i].response_data.json["data"]
