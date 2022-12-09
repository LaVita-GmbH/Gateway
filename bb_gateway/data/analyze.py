import asyncio
import logging
from typing import Coroutine, Iterable, Optional
from sentry_sdk import start_span
from sentry_sdk.tracing import Span
from .load import load_data


_logger = logging.getLogger(__name__)


def analyze_data(values: dict, *, headers: dict = {}, parent: Optional[dict] = None, max_level: Optional[int] = None, _cache: dict, _cleanup_callbacks: list, _parent_span: Optional[Span] = None):
    """
    Load referenced data into `values`, performing an in-place update.
    """

    def enrich_data(values, key: Optional[str] = None, parent: Optional[dict] = None, *, level: int = 0, _parent_span: Span) -> Iterable[Coroutine]:
        with _parent_span.start_child(op='enrich_data', description=key) as _span:
            if isinstance(values, list):
                for i, item in enumerate(values):
                    yield from enrich_data(item, key=f'{key}[{i}]', parent=parent, level=level + 1, _parent_span=_span)

            if not isinstance(values, dict):
                return

            if '$rel_at' in values:
                return

            if max_level and level > max_level:
                _logger.debug("MAXIMUM RECURSION DEPTH REACHED %s", values)
                return

            keys = list(values.keys())
            if parent:
                values['_parent'] = parent
                _cleanup_callbacks.append(lambda: values.pop('_parent', None))

            def process_value(key, value):
                if key == '$rel':
                    yield asyncio.create_task(load_data(value, values, headers, _cache, _cleanup_callbacks, _parent_span=_span))

                else:
                    yield from enrich_data(value, key=key, parent=values, level=level + 1, _parent_span=_span)

            for key in keys:
                yield from process_value(key, values[key])

    with (_parent_span.start_child if _parent_span else start_span)(op='load_referenced_data') as _span:
        yield from enrich_data(values, parent=parent, _parent_span=_span)
