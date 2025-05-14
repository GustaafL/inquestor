import responses
from pytest import mark, raises
from src.inquestor.inquestor import ingest, update_args, update_arg, validate_keys
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

def test_update_arg():
    args_dict = {"params": {"param1": 10, "param2": 0},
                  "url": "https://api.test"}
    _, value = update_arg(("params" ,{"param1": 0}), args_dict)
    assert value == {"param1": 10, "param2": 0}
    args_dict = {"params": {"param1": 0, "param2": 0},
                  "url": "https://api.test_new"}
    _, value = update_arg(("url", "https://api.test"), args_dict)
    assert value == "https://api.test_new"

def test_update_args():
    local_args = {"params": {"param1": 0, "param2": 0},
                  "url": "https://api.test"}
    local_args = update_args({"url": "https://api.test_new"},local_args )
    assert local_args["url"] == "https://api.test_new"
    assert local_args["params"] == {"param1": 0, "param2": 0}
    new_args_dict = update_args( {"params": {"param1": 10}},local_args)
    assert isinstance(local_args["params"], dict)
    local_args["params"] |= {"param1": 10}
    assert new_args_dict["params"] == {"param1": 10, "param2": 0}

def test_validate_keys():
    local_args = {"params": {"param1": 0, "param2": 0},
                  "url": "https://api.test"}
    new_local_args = validate_keys(local_args)
    assert new_local_args == local_args
    with raises(ValueError):
        validate_keys({"params": {"param1": 0, "param2": 0},
                       "url": 10,
                       "not_a_key": "value"})

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
    keyword_arg_dict = None, response: Response | None = None
):
    if keyword_arg_dict is None:
        return {"params":{"param1": 0}}
    else:
        arg_value = keyword_arg_dict["params"]
        arg_value["param1"] += 10
    if arg_value["param1"] > 20:
        return False
    return {"params": arg_value}


def next_page_url(
    keyword_arg_dict = None, response: Response | None = None
):
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


def next_page_from_response(
    keyword_arg_dict = None, response: Response | None = None
):
    if keyword_arg_dict is None:
        return {"url": "https://api.test"}
    else:
        if response is None:
            raise TypeError("response is None")
        next_url = response.json().get("next_url", False)
    return {"url": next_url} if next_url else False


def next_page_url_as_object(
    keyword_arg_dict = None, response: Response | None = None
):
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
