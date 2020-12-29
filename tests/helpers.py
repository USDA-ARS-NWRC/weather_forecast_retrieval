import os


def skip_external_http_request():
    return 'WFR_SKIP_EXTERNAL_REQUEST_TEST' in os.environ
