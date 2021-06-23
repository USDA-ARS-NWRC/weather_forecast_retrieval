import os

LOG_ERROR_CONFIG = {
    'logging': {
        'log_level': 'ERROR',
    }
}


def skip_on_github_actions():
    return 'WFR_SKIP_ON_GITHUB_ACTIONS' in os.environ


class MockResponse:
    def __init__(self, url, text, status_code):
        self.url = url
        self.text = text
        self.status_code = status_code
        self.content = text

    def text(self):
        return self.text


def mocked_requests_get(*args, **kwargs):

    if 'grib2' in args[0]:
        return MockResponse(args[0], b'mock response text', 200)

    if 'nomads' in args[0]:
        with open('../nomads/nomads_response.html') as f:
            html_string = f.read()
        return MockResponse(args[0], html_string, 200)

    return MockResponse(args[0], None, 404)
