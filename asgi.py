import os
import functools
import logging
import re
from typing import Any, Optional, Tuple
from datetime import datetime
from urllib.parse import urlencode
import aiohttp
from dotenv import load_dotenv
from aiohttp.client import ClientResponse
from starlette.applications import Starlette
from starlette.responses import JSONResponse, Response
from starlette.routing import Route
from starlette.requests import Request
from jsonpath_ng import parse as jsonpath_parse


load_dotenv()
_logger = logging.getLogger(__name__)
ENV_SERVICE_PREFIX = 'SERVICE_'
SERVICE_URLS = {key.removeprefix(ENV_SERVICE_PREFIX).lower(): value for key, value in os.environ.items() if key.startswith(ENV_SERVICE_PREFIX)}


async def _load_related_data(relation: list[str], curr_obj: dict, headers: dict = {}, id: Optional[str] = None, _cache: Optional[dict] = None, **lookup):
    if _cache is None:
        _cache = {}

    def get_params() -> str:
        if '$rel_params' not in curr_obj:
            return ''

        return f'?{urlencode(curr_obj["$rel_params"])}'

    cache_key = '/'.join(relation)
    if id:
        cache_key += f'/{id}{get_params()}'
        if cache_key not in _cache:
            try:
                _response, data = await _proxy(
                    method='GET',
                    service=relation[1],
                    path='/'.join([*relation[2:], id]),
                    params={},
                    headers=headers,
                )
                if not _response.ok:
                    raise ValueError({
                        'status': _response.status,
                        'data': await (_response.json() if 'application/json' in _response.content_type else _response.text()),
                    })

                _cache[cache_key] = data

            except Exception as error:
                _logger.warn("Failed to load referenced data for $rel='%s' error: %r", relation, error, exc_info=True)
                raise

    elif '$rel_params' in curr_obj:
        raise NotImplementedError
        # cache_key += get_params()
        # params = {key: resolve_placeholder(value) for key, value in curr_obj['$rel_params'].items()}
        # if curr_obj.get('$rel_is_lookup'):
        #     params['limit'] = 1

        # if cache_key not in _cache:
        #     try:
        #         _response, data = await _proxy('GET', relation[1], '/'.join(relation[2:]))
        #         _cache[cache_key] = (data, datetime.utcnow())

        #     except Exception:
        #         _logger.warn("Failed to load referenced data for $rel='%s' with tenant_id='%s', error: %r", relation, tenant_id, error, exc_info=True)
        #         return

        # else:
        #     data = _cache[cache_key][0]

        # if curr_obj.get('$rel_is_lookup'):
        #     cache_key += '[0]'
        #     if cache_key not in _cache or datetime.utcnow() - _cache[cache_key][1] > self._referenced_data_expire:
        #         try:
        #             _cache[cache_key] = (objects[loc][0], datetime.utcnow())

        #         except (KeyError, IndexError) as error:
        #             _logger.warn("Failed to get object from lookup for $rel='%s' with tenant_id='%s', error: %r", relation, tenant_id, error, exc_info=True)
        #             return

    else:
        raise NotImplementedError

    return _cache[cache_key]


def resolve_path(match, curr_obj):
    try:
        result = jsonpath_parse(match.group(1)).find(curr_obj)[0]
        return result.value

    except IndexError:
        pass


def resolve_placeholder(value, curr_obj):
    if not isinstance(value, str):
        return value

    return re.sub(
        r'\{([^\}]+)\}',
        functools.partial(resolve_path, curr_obj=curr_obj),
        value,
    )


async def load_referenced_data(values: dict, headers: dict = {}, parent: Optional[dict] = None, max_level: Optional[int] = None):
    """
    Load referenced data into `values`, performing an in-place update.
    """

    _referenced_data_cache = {}

    async def enrich_data(values, parent: Optional[dict] = None, level: int = 0):
        if isinstance(values, list):
            for item in values:
                await enrich_data(item, parent=parent, level=level + 1)

        if not isinstance(values, dict):
            return

        if '$rel_at' in values:
            return

        if max_level and level > max_level:
            return

        if parent:
            values['_parent'] = parent

        for key, value in list(values.items()):
            if key == '_parent':
                continue

            if key == '$rel':
                try:
                    rel_path = [resolve_placeholder(part, curr_obj=values) for part in value.split('/')]
                    values['$rel'] = '/'.join(rel_path)
                    related = await _load_related_data(rel_path, curr_obj=values, headers=headers, **values, _cache=_referenced_data_cache)

                except Exception as error:
                    _logger.exception(error)
                    values['$error'] = error.args[0]

                else:
                    values.update(related)

            else:
                await enrich_data(value, parent=values, level=level + 1)

        if parent:
            del values['_parent']

    await enrich_data(values, parent=parent)


async def _proxy(method, service, path, headers, params, data=None) -> Tuple[ClientResponse, Any]:
    base_url = SERVICE_URLS[service]
    url = f"{base_url}/{service}/{path}"
    if path == 'docs':
        url = f"{base_url}/docs"

    async with aiohttp.request(
        method,
        url,
        headers=headers,
        params=params,
        data=data,
    ) as response:
        if 'application/json' in response.headers.get('Content-Type'):
            data = await response.json()
            await load_referenced_data(data, headers=headers)
            return response, data

        return response, await response.text()


async def resolver(request: Request):
    response, data = await _proxy(
        method=request.method,
        service=request.path_params['service'],
        path=request.path_params['path'],
        headers=request.headers,
        params=request.query_params,
        data=await request.body(),
    )
    headers = {**response.headers}
    del headers['Content-Length']
    if 'application/json' in response.headers.get('Content-Type'):
        return JSONResponse(data, headers=headers)

    return Response(data, headers=headers)


app = Starlette(routes=[
    Route('/{service:str}/{path:path}', resolver, methods=['GET', 'POST', 'PATCH', 'PUT', 'DELETE', 'HEAD', 'OPTIONS'])
])
