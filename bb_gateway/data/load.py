from datetime import timedelta
import logging
import asyncio
from typing import Any, Optional, Tuple
import orjson as json
from redis.exceptions import RedisError
from aiohttp.client import ClientResponse
from sentry_sdk.tracing import Span
from .. import settings
from .utils import resolve_placeholder, get_cache_key


_logger = logging.getLogger(__name__)


async def load_data(value, values, headers, _cache, _parent_span: Span):
    with _parent_span.start_child(op='load_data', description=value) as _span:
        rel_path = [resolve_placeholder(part, curr_obj=values) for part in value.split('/')]
        cache_key = get_cache_key(rel_path, curr_obj=values, **values)
        values['$rel'] = '/'.join(rel_path)
        data = {}

        with _span.start_child(op='load_data.redis_get', description=cache_key):
            try:
                data = await settings.REDIS_CONN.get(cache_key)

            except Exception as error:
                _logger.warning("Redis get error: %r", error)

        if not data:
            data = {}
            try:
                with _span.start_child(op='load_data.fetch') as __span:
                    _response, related = await _load_related_data(rel_path, cache_key=cache_key, curr_obj=values, headers=headers, **values, _cache=_cache, _parent_span=__span)

                    _span.set_tag('data.source', 'upstream')
                    _span.set_http_status(_response.status)

                if not _response.ok:
                    raise ValueError({
                        'status': _response.status,
                        'data': await (_response.json() if 'application/json' in _response.content_type else _response.text()),
                    })

            except Exception as error:
                _logger.warning("Cannot load data from %s", value)
                data['$error'] = error.args[0]

            else:
                data.update(related)

            _span.set_tag('cache_control', _response.headers.get('cache-control'))

            if _response.headers.get('cache-control') != 'no-cache':
                with _span.start_child(op='load_data.redis_set'):
                    try:
                        await settings.REDIS_CONN.set(cache_key, json.dumps(data), ex=timedelta(seconds=60))

                    except Exception as error:
                        _logger.warning("Redis set error: %r", error)

        else:
            _span.set_tag('data.source', 'redis')
            data = json.loads(data)

        values.update(data)
        return values


def _load_related_data(
    relation: list[str],
    cache_key: str,
    curr_obj: dict,
    headers: dict = {},
    id: Optional[str] = None,
    _cache: Optional[dict] = None,
    *,
    _parent_span: Span,
    **lookup,
) -> asyncio.Task[Tuple[ClientResponse, Any]]:
    if _cache is None:
        _cache = {}

    from ..resolver_proxy import proxy

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
