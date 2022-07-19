import logging
from typing import Any, Optional, Tuple
import orjson as json
import aiohttp
from aiohttp.client import ClientResponse
from aiohttp.client_exceptions import InvalidURL
from sentry_sdk import start_span, set_tag
from sentry_sdk.tracing import Span
from . import settings
from .data.analyze import analyze_data


_logger = logging.getLogger(__name__)


async def proxy(method, service, path, headers, params, data=None, _cache: Optional[dict] = None, _parent_span: Optional[Span] = None) -> Tuple[ClientResponse, Any]:
    try:
        base_url = settings.SERVICE_URLS[service]

    except KeyError:
        raise InvalidURL(service)

    url = f"{base_url}/{service}/{path}"
    if path in ('docs', 'redoc'):
        url = f"{base_url}/{path}"

    with (_parent_span.start_child if _parent_span else start_span)(op='proxy', description=f'{method} /{service}/{path}') as _span:
        set_tag('service', service)
        async with aiohttp.request(
            method,
            url,
            headers=headers,
            params=params,
            data=data,
        ) as response:
            if 'application/json' in response.content_type:
                data = await response.json(loads=json.loads)
                if path != 'openapi.json':
                    await analyze_data(data, headers=headers, _cache=_cache, _parent_span=_span)

                return response, data

            return response, await response.text()
