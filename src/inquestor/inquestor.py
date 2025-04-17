from typing import Any
from dataclasses import dataclass
from requests import Session
from urllib3.util import Url


def next_page(initial: bool, arg_value=None, keyword="") -> tuple[Any, str]:
    _ = initial
    return arg_value, keyword


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
    next_page=next_page,
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
    local_args = locals()
    local_args.pop("next_page")
    session = Session()
    arg_value, keyword = next_page(initial=True)

    while arg_value:
        local_args = update_arg_value(local_args, keyword, arg_value)
        print(local_args)
        response = session.request(
            **local_args,
        )

        arg_value, keyword = next_page(
            initial=False, arg_value=arg_value, keyword=keyword, response=response
        )
        if response.status_code == 200:
            yield response.json()
        else:
            print(f"Error: {response.status_code}")
            break


def update_arg_value(local_args, keyword, arg_value):
    if isinstance(arg_value, dict):
        local_args[keyword] |= arg_value
    elif isinstance(arg_value, Url):
        local_args[keyword] = arg_value
    else:
        local_args[keyword] = arg_value
    return local_args
