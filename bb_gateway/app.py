from starlette.applications import Starlette
from starlette.responses import JSONResponse, Response
from starlette.routing import Route
from starlette.requests import Request
from aiohttp.client_exceptions import ClientError, InvalidURL
from sentry_sdk.integrations.asgi import SentryAsgiMiddleware
from . import settings
from .resolver_proxy import proxy


async def resolver(request: Request):
    try:
        response, data = await proxy(
            method=request.method,
            service=request.path_params['service'],
            path=request.path_params['path'],
            headers=request.headers,
            params=request.query_params,
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
        )

    headers = {**response.headers}
    del headers['Content-Length']
    if 'application/json' in response.content_type:
        return JSONResponse(data, headers=headers)

    return Response(data, headers=headers)



async def healthcheck(request: Request):
    return JSONResponse({
        'services': list(settings.SERVICE_URLS.keys()),
    })


star = Starlette(routes=[
    Route('/{service:str}/{path:path}', resolver, methods=['GET', 'POST', 'PATCH', 'PUT', 'DELETE', 'HEAD', 'OPTIONS']),
    Route('/', healthcheck)
])
app = SentryAsgiMiddleware(star, transaction_style='url')
