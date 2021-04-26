import os

LOG_ERROR_CONFIG = {
    'logging': {
        'log_level': 'ERROR',
    }
}


def skip_external_http_request():
    return 'WFR_SKIP_EXTERNAL_REQUEST_TEST' in os.environ


def skip_on_github_actions():
    return 'WFR_SKIP_ON_GITHUB_ACTIONS' in os.environ
