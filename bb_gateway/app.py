from starlette.applications import Starlette
from starlette.responses import JSONResponse, Response
from starlette.routing import Route
from starlette.requests import Request
from aiohttp.client_exceptions import ClientError, InvalidURL
from sentry_sdk.integrations.asgi import SentryAsgiMiddleware
from sentry_sdk import Hub
import orjson as json
from . import settings
from .resolver_proxy import proxy


async def resolver(request: Request):
    request_headers = {**request.headers}

    cors_headers = {
        'Access-Control-Allow-Origin': request_headers.get('origin'),
        'Access-Control-Allow-Credentials': 'true',
        'Access-Control-Allow-Headers': 'Authorization, Content-Type, sentry-trace',
        'Access-Control-Allow-Methods': 'GET, HEAD, POST, PUT, PATCH, DELETE, OPTIONS',
    }

    if request.method == "OPTIONS":
        return Response(status_code=204, headers=cors_headers)

    if not request_headers.get('sentry-trace'):
        request_headers['sentry-trace'] = Hub.current.scope.transaction.to_traceparent()

    try:
        response, data = await proxy(
            method=request.method,
            service=request.path_params['service'],
            path=request.path_params['path'],
            headers=request_headers,
            params=str(request.query_params),
            data=await request.body(),
        )

    except InvalidURL as error:
        return Response(status_code=404)

    except ClientError as error:
        return JSONResponse(
            content={
                'error': error.__class__.__name__,
            },
            status_code=502,
            headers=cors_headers,
        )

    response_headers = {**response.headers, **cors_headers}
    try:
        del response_headers['Content-Length']

    except KeyError:
        pass

    if 'application/json' in response.content_type:
        return Response(json.dumps(data), headers=response_headers, status_code=response.status)

    return Response(data, headers=response_headers, status_code=response.status)



async def healthcheck(request: Request):
    return Response(json.dumps({
        'services': list(settings.SERVICE_URLS.keys()),
    }), headers={'Content-Type': 'application/json'})


star = Starlette(routes=[
    Route('/{service:str}/{path:path}', resolver, methods=['GET', 'POST', 'PATCH', 'PUT', 'DELETE', 'HEAD', 'OPTIONS']),
    Route('/', healthcheck)
])
app = SentryAsgiMiddleware(star, transaction_style='url')
