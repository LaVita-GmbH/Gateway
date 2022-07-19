import functools
import re
from typing import Optional
from urllib.parse import urlencode
from jsonpath_ng import parse as jsonpath_parse


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
