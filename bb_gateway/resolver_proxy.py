from datetime import timedelta
import functools
import logging
import re
import asyncio
from typing import Any, Optional, Tuple
from urllib.parse import urlencode
import orjson as json
import aiohttp
from redis.asyncio import Redis
from redis.asyncio.cluster import RedisCluster
from redis.exceptions import RedisClusterException, RedisError
from aiohttp.client import ClientResponse
from aiohttp.client_exceptions import InvalidURL
from jsonpath_ng import parse as jsonpath_parse
from sentry_sdk import start_span
from sentry_sdk.tracing import Span
from . import settings


_logger = logging.getLogger(__name__)


try:
    redis_conn = RedisCluster.from_url(url=settings.REDIS_URL)

except RedisClusterException:
    redis_conn = Redis.from_url(settings.REDIS_URL)


def _load_related_data(relation: list[str], cache_key: str, curr_obj: dict, headers: dict = {}, id: Optional[str] = None, _cache: Optional[dict] = None, *, _parent_span: Span, **lookup) -> asyncio.Task:
    if _cache is None:
        _cache = {}

    if id:
        if cache_key not in _cache:
            _cache[cache_key] = asyncio.create_task(proxy(
                method='GET',
                service=relation[1],
                path='/'.join([*relation[2:], id]),
                params={},
                headers=headers,
                _cache=_cache,
                _parent_span=_parent_span,
            ))

    elif '$rel_params' in curr_obj:
        raise NotImplementedError
        # cache_key += get_params()
        # params = {key: resolve_placeholder(value) for key, value in curr_obj['$rel_params'].items()}
        # if curr_obj.get('$rel_is_lookup'):
        #     params['limit'] = 1

        # if cache_key not in _cache:
        #     try:
        #         _response, data = await proxy('GET', relation[1], '/'.join(relation[2:]))
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


def get_cache_key(relation, curr_obj, id: Optional[str] = None, **lookup):
    def get_params() -> str:
        if '$rel_params' not in curr_obj:
            return ''

        return f'?{urlencode(curr_obj["$rel_params"])}'

    cache_key = '/'.join(relation)
    if id:
        cache_key += f'/{id}{get_params()}'

    return cache_key


async def load_data(value, values, headers, _cache, _parent_span: Span):
    with _parent_span.start_child(op='load_data', description=value) as _span:
        rel_path = [resolve_placeholder(part, curr_obj=values) for part in value.split('/')]
        cache_key = get_cache_key(rel_path, curr_obj=values, **values)
        values['$rel'] = '/'.join(rel_path)

        with _span.start_child(op='load_data.redis_get', description=cache_key):
            try:
                data = await redis_conn.get(cache_key)

            except RedisError as error:
                _logger.exception(error)

        if not data:
            data = {}
            try:
                _response, related = await _load_related_data(rel_path, cache_key=cache_key, curr_obj=values, headers=headers, **values, _cache=_cache, _parent_span=_span)

                if not _response.ok:
                    raise ValueError({
                        'status': _response.status,
                        'data': await (_response.json() if 'application/json' in _response.content_type else _response.text()),
                    })

            except Exception as error:
                _logger.exception(error)
                data['$error'] = error.args[0]

            else:
                data.update(related)

            with _span.start_child(op='load_data.redis_set'):
                try:
                    await redis_conn.set(cache_key, json.dumps(data), ex=timedelta(seconds=60))

                except RedisError as error:
                    _logger.exception(error)

        else:
            data = json.loads(data)

        values.update(data)


async def load_referenced_data(values: dict, headers: dict = {}, parent: Optional[dict] = None, max_level: Optional[int] = None, _cache: Optional[dict] = None, _parent_span: Optional[Span] = None):
    """
    Load referenced data into `values`, performing an in-place update.
    """

    if _cache is None:
        _cache = {}

    _pending = []
    cleanup_callbacks = []

    async def enrich_data(values, parent: Optional[dict] = None, level: int = 0, _parent_span: Optional[Span] = None):
        with (_parent_span.start_child if _parent_span else start_span)(op='enrich_data') as _span:
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
                    future = asyncio.create_task(load_data(value, values, headers, _cache, _parent_span=_span))
                    _pending.append(future)

                else:
                    await enrich_data(value, parent=values, level=level + 1, _parent_span=_span)

            cleanup_callbacks.append(lambda: values.pop('_parent', None))

    with (_parent_span.start_child if _parent_span else start_span)(op='load_referenced_data') as _span:
        await enrich_data(values, parent=parent, _parent_span=_span)
        await asyncio.gather(*_pending, return_exceptions=True)

    with (_parent_span.start_child if _parent_span else start_span)(op='load_referenced_data.cleanup'):
        for callback in cleanup_callbacks:
            callback()


async def proxy(method, service, path, headers, params, data=None, _cache: Optional[dict] = None, _parent_span: Optional[Span] = None) -> Tuple[ClientResponse, Any]:
    try:
        base_url = settings.SERVICE_URLS[service]

    except KeyError:
        raise InvalidURL(service)

    url = f"{base_url}/{service}/{path}"
    if path in ('docs', 'redoc'):
        url = f"{base_url}/{path}"

    with (_parent_span.start_child if _parent_span else start_span)(op='proxy', description=f'{method} /{service}/{path}') as _span:
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
                    await load_referenced_data(data, headers=headers, _cache=_cache, _parent_span=_span)

                return response, data

            return response, await response.text()
