import os

LOG_ERROR_CONFIG = {
    'logging': { 'log_level': 'ERROR' }
}

def skip_external_http_request():
    return 'WFR_SKIP_EXTERNAL_REQUEST_TEST' in os.environ
