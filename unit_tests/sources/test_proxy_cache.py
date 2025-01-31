import time

import pytest
import requests
from pytest_httpserver import HTTPServer


def test_mitmproxy_caching(mitmproxy_cache, httpserver: HTTPServer):
    """Test that HTTP requests are intercepted and cached by mitmproxy."""
    httpserver.expect_request("/test").respond_with_data("test response", status=200)
    url = httpserver.url_for("/test")
    
    # First request should go through the proxy
    response1 = requests.get(url)
    assert response1.status_code == 200
    assert response1.text == "test response"
    assert len(httpserver.log) == 1
    
    # Give mitmproxy time to process the response
    time.sleep(0.1)
    
    # Second request should be served from cache
    response2 = requests.get(url)
    assert response2.status_code == 200
    assert response2.text == "test response"
    # Server should not receive second request if it was cached
    assert len(httpserver.log) == 1
