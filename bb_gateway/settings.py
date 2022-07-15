import os
import sentry_sdk
from dotenv import load_dotenv


load_dotenv()


ENV_SERVICE_PREFIX = 'SERVICE_'
SERVICE_URLS = {key.removeprefix(ENV_SERVICE_PREFIX).lower().replace('_', '-'): value for key, value in os.environ.items() if key.startswith(ENV_SERVICE_PREFIX)}
SENTRY_TRACES_SAMPLE_RATE = float(os.getenv('SENTRY_TRACES_SAMPLE_RATE', 1.0))


def sentry_traces_sampler(context):
    if 'asgi_scope' in context and context['asgi_scope']['path'] == '/':
        return 0

    return SENTRY_TRACES_SAMPLE_RATE


sentry_sdk.init(
    environment=os.getenv('SENTRY_ENVIRONMENT', 'development'),
    dsn=os.getenv('SENTRY_DSN'),
    integrations=[],
    traces_sampler=sentry_traces_sampler,
    send_default_pii=True,
    auto_session_tracking=True,
)
