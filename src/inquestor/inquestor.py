from typing import Any
from functools import reduce
import inspect
from requests import Response, Session
from urllib3.util import Url, Retry
from enum import Enum
from requests.adapters import HTTPAdapter


class MutableRequestInput(Enum):
    url = "url"
    params = "params"
    data = "data"
    headers = "headers"
    cookies = "cookies"
    files = "files"
    auth = "auth"
    hooks = "hooks"
    stream = "stream"
    verify = "verify"
    json = "json"


class RequestInput(Enum):
    method = "method"
    url = "url"
    params = "params"
    data = "data"
    headers = "headers"
    cookies = "cookies"
    files = "files"
    auth = "auth"
    timeout = "timeout"
    allow_redirects = "allow_redirects"
    proxies = "proxies"
    hooks = "hooks"
    stream = "stream"
    verify = "verify"
    json = "json"


type KeywordArgDict = dict[MutableRequestInput, Any]


def update_arg_value(local_args, keyword, arg_value):
    if isinstance(arg_value, dict):
        local_args[keyword] |= arg_value
    elif isinstance(arg_value, Url):
        local_args[keyword] = arg_value
    else:
        local_args[keyword] = arg_value
    return local_args


def filter_request_input(acc, item):
    key, value = item
    if key in RequestInput:
        acc[key] = value
    return acc


def check_is_function(func):
    if not inspect.isfunction(func):
        raise TypeError(f"{func} must be a function")
    return func


def next_page(
    keyword_arg_dict: KeywordArgDict | None = None, response: Response | None = None
) -> KeywordArgDict | None:
    if keyword_arg_dict is None:
        return {MutableRequestInput.url: "placeholder"}
    return keyword_arg_dict


def authenticate(
    reauth_dict, response: Response | None = None
) -> tuple[KeywordArgDict | None, dict[str, Any] | None]:
    auth_token = "your_auth_token"
    return {
        MutableRequestInput.headers: {"Authorization": f"Bearer {auth_token}"}
    }, reauth_dict


def validate_keys(args_dict):
    valid_keys = set(MutableRequestInput.__members__.keys())
    invalid_keys = set(args_dict.keys()) - valid_keys

    if invalid_keys:
        raise ValueError(
            f"Invalid keyword(s) provided: {sorted(invalid_keys)}"
            f"Valid keywords are: {sorted(valid_keys)}"
            "The authenticate or next_page return dict is not valid."
        )
    return args_dict


def update_arg(item, args_dict):
    key, value = item

    if key in args_dict:
        if isinstance(args_dict[key], dict):
            if isinstance(value, dict):
                value |= args_dict[key]
            else:
                value = args_dict[key]
            return key, value
        else:
            return key, args_dict[key]
    return key, value


def update_args(args_dict, input_dict):
    def process_item(acc, item):
        key, value = update_arg(item, args_dict)
        return {**acc, key: value}

    return reduce(process_item, input_dict.items(), {})


def validate_response(response: Response) -> bool:
    """Validates the response object.

    :param response: The Response object to validate.
    :return: True if the response is valid, False otherwise.
    """
    if response.status_code == 200:
        return True
    elif response.status_code in {401, 403}:
        print("Authentication error or forbidden access.")
        return False
    else:
        print(f"Unexpected status code: {response.status_code}")
        return False


def rate_limit(
    ratelimit_dict: dict[str, Any] | None = None, response: Response | None = None
):
    pass


def ingest(
    method,
    url,
    params=None,
    data=None,
    headers=None,
    cookies=None,
    files=None,
    auth=None,
    timeout=None,
    allow_redirects=True,
    proxies=None,
    hooks=None,
    stream=None,
    verify=None,
    cert=None,
    json=None,
    retries: Retry | None = None,
    next_page=next_page,
    authenticate=None,
    rate_limit=None,
):
    """Constructs a :class:`Request <Request>`, prepares it and sends it.
    Returns :class:`Response <Response>` object.

    :param method: method for the new :class:`Request` object.
    :param url: URL for the new :class:`Request` object.
    :param params: (optional) Dictionary or bytes to be sent in the query
        string for the :class:`Request`.
    :param data: (optional) Dictionary, list of tuples, bytes, or file-like
        object to send in the body of the :class:`Request`.
    :param json: (optional) json to send in the body of the
        :class:`Request`.
    :param headers: (optional) Dictionary of HTTP Headers to send with the
        :class:`Request`.
    :param cookies: (optional) Dict or CookieJar object to send with the
        :class:`Request`.
    :param files: (optional) Dictionary of ``'filename': file-like-objects``
        for multipart encoding upload.
    :param auth: (optional) Auth tuple or callable to enable
        Basic/Digest/Custom HTTP Auth.
    :param timeout: (optional) How long to wait for the server to send
        data before giving up, as a float, or a :ref:`(connect timeout,
        read timeout) <timeouts>` tuple.
    :type timeout: float or tuple
    :param allow_redirects: (optional) Set to True by default.
    :type allow_redirects: bool
    :param proxies: (optional) Dictionary mapping protocol or protocol and
        hostname to the URL of the proxy.
    :param hooks: (optional) Dictionary mapping hook name to one event or
        list of events, event must be callable.
    :param stream: (optional) whether to immediately download the response
        content. Defaults to ``False``.
    :param verify: (optional) Either a boolean, in which case it controls whether we verify
        the server's TLS certificate, or a string, in which case it must be a path
        to a CA bundle to use. Defaults to ``True``. When set to
        ``False``, requests will accept any TLS certificate presented by
        the server, and will ignore hostname mismatches and/or expired
        certificates, which will make your application vulnerable to
        man-in-the-middle (MitM) attacks. Setting verify to ``False``
        may be useful during local development or testing.
    :param cert: (optional) if String, path to ssl client cert file (.pem).
        If Tuple, ('cert', 'key') pair.
    :rtype: requests.Response
    """
    request_input_args = reduce(filter_request_input, locals().items(), {})

    check_is_function(next_page)
    if authenticate:
        check_is_function(authenticate)
    if rate_limit:
        check_is_function(rate_limit)
    reauth_dict = None
    ratelimit_dict = None
    response = None
    session = Session()
    if retries:
        session.mount("http://", HTTPAdapter(max_retries=retries))
    next_page_dict = next_page()
    while next_page_dict:
        request_input_args = update_args(
            validate_keys(next_page_dict), request_input_args
        )
        print(request_input_args)

        if authenticate:
            authenticate_args, reauth_dict = authenticate(
                reauth_dict=reauth_dict, response=response
            )
            if authenticate_args:
                request_input_args = update_args(
                    validate_keys(authenticate_args), request_input_args
                )
        if rate_limit:
            ratelimit_dict = rate_limit(
                ratelimit_dict=ratelimit_dict, response=response
            )

        response = session.request(
            **request_input_args,
        )

        if validate_response(response):
            # If the response is valid, we can proceed to the next page
            print("Valid response received.")
            next_page_dict = next_page(next_page_dict, response=response)

            yield response.json()
        else:
            print(f"Error: {response.status_code}")
            break
    session.close()
    # add logging options
