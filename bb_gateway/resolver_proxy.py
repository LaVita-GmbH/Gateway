import asyncio
import logging
from typing import Any, Optional, Tuple, Awaitable
import json
import aiohttp
from aiohttp.client import ClientResponse
from aiohttp.client_exceptions import InvalidURL
from sentry_sdk import start_span, set_tag
from sentry_sdk.tracing import Span
from . import settings
from .data.analyze import analyze_data


_logger = logging.getLogger(__name__)


async def _proxy(method: str, service: str, path: str, headers, params: str, *, data=None, timeout: Optional[float] = None, _cache: dict, _cleanup_callbacks: list, _parent_span: Optional[Span] = None) -> Tuple[ClientResponse, Any]:
    try:
        base_url = settings.SERVICE_URLS[service]

    except KeyError:
        raise InvalidURL(service)

    url = f"{base_url}/{service}/{path}"
    if path in ('docs', 'redoc'):
        url = f"{base_url}/{path}"

    is_json = False

    with (_parent_span.start_child if _parent_span else start_span)(op='proxy', description=f'{method} /{service}/{path}') as _span:
        set_tag('service', service)
        _logger.debug("REQ %s %s/%s", method, service, path)
        async with aiohttp.request(
            method,
            url,
            headers=headers,
            params=params,
            data=data,
            timeout=aiohttp.ClientTimeout(total=timeout),
        ) as response:
            _logger.debug("REQ %s %s/%s resolved in with %i", method, service, path, response.status)
            if 'application/json' in response.content_type:
                is_json = True
                data = await response.json(loads=json.loads)

            else:
                data = await response.read()

    with (_parent_span.start_child if _parent_span else start_span)(op='analyze_data', description=f'{method} /{service}/{path}') as _span:
        if is_json and path != 'openapi.json':
            _tasks: list[asyncio.Task] = list(analyze_data(data, headers=headers, _cache=_cache, _cleanup_callbacks=_cleanup_callbacks, _parent_span=_span))
            await asyncio.gather(*_tasks, return_exceptions=True)
            for task in _tasks:
                if error := task.exception():
                    _logger.error("Error in Task %s: %e", task, error)

        return response, data


def proxy(method: str, service: str, path: str, headers, params: str, *, data=None, timeout: Optional[float] = None, _cache: dict, _cleanup_callbacks: list, _parent_span: Optional[Span] = None) -> Awaitable[Tuple[ClientResponse, Any]]:
    _logger.debug("PROXY %s %s/%s", method, service, path)

    _cache_key = f'{method} {service}/{path}?{params}'
    coro = _proxy(
        method=method,
        service=service,
        path=path,
        headers=headers,
        params=params,
        data=data,
        timeout=timeout,
        _cache=_cache,
        _cleanup_callbacks=_cleanup_callbacks,
        _parent_span=_parent_span,
    )

    if method != 'GET':
        return asyncio.create_task(coro, name=_cache_key)

    if _cache_key not in _cache:
        _logger.debug("PROXY FRESH %s", _cache_key)
        _cache[_cache_key] = asyncio.create_task(coro, name=_cache_key)

    else:
        _logger.debug("PROXY PENDING %s", _cache_key)

    return _cache[_cache_key]
