#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2017 Bitergia
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, 51 Franklin Street, Fifth Floor, Boston, MA 02110-1335, USA.
#
# Authors:
#     Valerio Cosentino <valcos@bitergia.com>
#

import os
import sys
import time
import unittest

import httpretty
import pkg_resources
import requests

# Hack to make sure that tests import the right packages
# due to setuptools behaviour
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
pkg_resources.declare_namespace('perceval.backends')

from perceval.client import HttpClient, RateLimitHandler


CLIENT_API_URL = "https://gateway.marvel.com/v1/"
CLIENT_SPIDERMAN_URL = "https://gateway.marvel.com/v1/public/characters/1"
CLIENT_SUPERMAN_URL = "https://gateway.marvel.com/v1/public/characters/2"
CLIENT_BATMAN_URL = "https://gateway.marvel.com/v1/public/characters/3"
CLIENT_IRONMAN_URL = "https://gateway.marvel.com/v1/public/characters/4"


class MockedClient(HttpClient, RateLimitHandler):

    def __init__(self, base_url, default_sleep_time=HttpClient.DEFAULT_SLEEP_TIME,
                 max_retries=HttpClient.MAX_RETRIES,
                 extra_status_forcelist=None,
                 extra_retry_after_status=None,
                 extra_headers=HttpClient.DEFAULT_HEADERS, sleep_for_rate=False,
                 min_rate_to_sleep=RateLimitHandler.MIN_RATE_LIMIT,
                 rate_limit_header=RateLimitHandler.RATE_LIMIT_HEADER,
                 rate_limit_reset_header=RateLimitHandler.RATE_LIMIT_RESET_HEADER):

        super().__init__(base_url, default_sleep_time=default_sleep_time, max_retries=max_retries,
                         extra_status_forcelist=extra_status_forcelist,
                         extra_retry_after_status=extra_retry_after_status,
                         extra_headers=extra_headers)
        super().setup_rate_limit_handler(sleep_for_rate=sleep_for_rate,
                                         min_rate_to_sleep=min_rate_to_sleep,
                                         rate_limit_header=rate_limit_header,
                                         rate_limit_reset_header=rate_limit_reset_header)


class TestHttpClient(unittest.TestCase):
    """Http client tests"""

    def test_initialization(self):
        """Test whether attributes are initializated"""

        client = MockedClient(CLIENT_API_URL)

        self.assertEqual(client.base_url, CLIENT_API_URL)
        self.assertEqual(client.max_retries, HttpClient.MAX_RETRIES)
        self.assertEqual(client.max_retries_on_connect, HttpClient.MAX_RETRIES_ON_CONNECT)
        self.assertEqual(client.max_retries_on_read, HttpClient.MAX_RETRIES_ON_READ)
        self.assertEqual(client.max_retries_on_redirect, HttpClient.MAX_RETRIES_ON_REDIRECT)
        self.assertEqual(client.max_retries_on_read, HttpClient.MAX_RETRIES_ON_READ)
        self.assertEqual(client.max_retries_on_status, HttpClient.MAX_RETRIES_ON_STATUS)
        self.assertEqual(client.status_forcelist, HttpClient.DEFAULT_STATUS_FORCE_LIST)
        self.assertEqual(client.retry_after_status, HttpClient.DEFAULT_RETRY_AFTER_STATUS_CODES)
        self.assertEqual(client.method_whitelist, HttpClient.DEFAULT_METHOD_WHITELIST)
        self.assertEqual(client.raise_on_redirect, HttpClient.DEFAULT_RAISE_ON_REDIRECT)
        self.assertEqual(client.raise_on_status, HttpClient.DEFAULT_RAISE_ON_STATUS)
        self.assertEqual(client.respect_retry_after_header, HttpClient.DEFAULT_RESPECT_RETRY_AFTER_HEADER)
        self.assertEqual(client.default_sleep_time, HttpClient.DEFAULT_SLEEP_TIME)

        self.assertIsNotNone(client.session)
        self.assertEqual(client.session.headers['User-Agent'], HttpClient.DEFAULT_HEADERS.get('User-Agent'))

        self.assertEqual(client.rate_limit, None)
        self.assertEqual(client.rate_limit_reset_ts, None)

        expected_retries = 5
        expected_sleep_time = 100
        expected_headers = {'User-Agent': 'ACME Corp.', 'Token': "your-token"}
        extra_status = 555

        client = MockedClient(CLIENT_API_URL,
                              max_retries=expected_retries,
                              default_sleep_time=expected_sleep_time,
                              extra_headers=expected_headers,
                              extra_retry_after_status=[extra_status],
                              extra_status_forcelist=[extra_status])

        self.assertEqual(client.session.headers['User-Agent'], expected_headers.get('User-Agent'))
        self.assertEqual(client.session.headers['Token'], expected_headers.get('Token'))
        self.assertEqual(client.max_retries, expected_retries)
        self.assertEqual(client.default_sleep_time, expected_sleep_time)
        self.assertTrue(extra_status in client.status_forcelist)
        self.assertTrue(extra_status in client.retry_after_status)

    @httpretty.activate
    def test_close_session(self):
        """Test wheter the session is properly closed"""

        output = "success"
        httpretty.register_uri(httpretty.GET,
                               CLIENT_SPIDERMAN_URL,
                               body=output,
                               status=200)

        client = MockedClient(CLIENT_API_URL)
        response = client.fetch(CLIENT_SPIDERMAN_URL)
        self.assertEqual(response.headers['connection'], 'close')

    @httpretty.activate
    def test_fetch_get(self):
        """Test fetch method"""

        output = "success"
        httpretty.register_uri(httpretty.GET,
                               CLIENT_SPIDERMAN_URL,
                               body=output,
                               status=200)

        client = MockedClient(CLIENT_API_URL, default_sleep_time=0.1, max_retries=1)
        response = client.fetch(CLIENT_SPIDERMAN_URL)

        self.assertEqual(response.request.method, HttpClient.GET)
        self.assertEqual(response.text, output)

    @httpretty.activate
    def test_fetch_http_error(self):
        """Test fetch method"""

        httpretty.register_uri(httpretty.GET,
                               CLIENT_SUPERMAN_URL,
                               body="",
                               status=403)

        client = MockedClient(CLIENT_API_URL, default_sleep_time=0.1, max_retries=1)

        with self.assertRaises(requests.exceptions.HTTPError):
            _ = client.fetch(CLIENT_SUPERMAN_URL)

    @httpretty.activate
    def test_fetch_post(self):
        """Test fetch method"""

        output = "success"

        httpretty.register_uri(httpretty.POST,
                               CLIENT_SPIDERMAN_URL,
                               body=output,
                               status=200)

        client = MockedClient(CLIENT_API_URL, default_sleep_time=0.1, max_retries=1)

        response = client.fetch(CLIENT_SPIDERMAN_URL, method=HttpClient.POST)
        self.assertEqual(response.request.method, HttpClient.POST)
        self.assertEqual(response.text, output)

    @httpretty.activate
    def test_fetch_retry_after(self):
        """Test whether calls returning 503, 413, 429 status codes are retried"""

        retry_after_value = 1

        httpretty.register_uri(httpretty.GET,
                               CLIENT_SPIDERMAN_URL,
                               body="",
                               status=413,
                               forcing_headers={
                                   'Retry-After': str(retry_after_value)
                               })

        httpretty.register_uri(httpretty.GET,
                               CLIENT_SUPERMAN_URL,
                               body="",
                               status=429,
                               forcing_headers={
                                   'Retry-After': str(retry_after_value)
                               })

        httpretty.register_uri(httpretty.GET,
                               CLIENT_BATMAN_URL,
                               body="",
                               status=503,
                               forcing_headers={
                                   'Retry-After': str(retry_after_value)
                               })

        client = MockedClient(CLIENT_API_URL, default_sleep_time=0.1, max_retries=1)

        urls = [CLIENT_SPIDERMAN_URL, CLIENT_SUPERMAN_URL, CLIENT_BATMAN_URL]

        for url in urls:
            before = int(time.time())
            expected = before + (retry_after_value * client.max_retries)

            with self.assertRaises(requests.exceptions.HTTPError):
                _ = client.fetch(url)

            after = int(time.time())
            self.assertTrue(expected <= after)

    @httpretty.activate
    def test_fetch_retry(self):
        """Test whether calls returning redirect codes (3xx) or 408, 423, 504 are retried"""

        httpretty.register_uri(httpretty.GET,
                               CLIENT_IRONMAN_URL,
                               body="",
                               status=301)

        httpretty.register_uri(httpretty.GET,
                               CLIENT_SPIDERMAN_URL,
                               body="",
                               status=408)

        httpretty.register_uri(httpretty.GET,
                               CLIENT_SUPERMAN_URL,
                               body="",
                               status=423)

        httpretty.register_uri(httpretty.GET,
                               CLIENT_BATMAN_URL,
                               body="",
                               status=504)

        client = MockedClient(CLIENT_API_URL, default_sleep_time=0.1, max_retries=1, extra_status_forcelist=[301])

        urls = [CLIENT_IRONMAN_URL, CLIENT_SPIDERMAN_URL, CLIENT_SUPERMAN_URL, CLIENT_BATMAN_URL]

        for url in urls:
            with self.assertRaises(requests.exceptions.RetryError):
                _ = client.fetch(url)


class TestRateLimitHandler(unittest.TestCase):
    """RateLimit handler tests"""

    def test_setup_rate_limit_handler(self):
        """Test whether variables are properly initialized during the setup"""

        client = MockedClient(CLIENT_API_URL, default_sleep_time=0.1, max_retries=1)

        self.assertEqual(client.sleep_for_rate, False)
        self.assertEqual(client.min_rate_to_sleep, RateLimitHandler.MIN_RATE_LIMIT)
        self.assertEqual(client.rate_limit_header, RateLimitHandler.RATE_LIMIT_HEADER)
        self.assertEqual(client.rate_limit_reset_header, RateLimitHandler.RATE_LIMIT_RESET_HEADER)

        expected_sleep_for_rate = True
        expected_min_rate_to_sleep = 200
        expected_rate_limit_header = "ACME Corp."
        expected_rate_limit_reset_header = "UMBRELLA Corp."

        client = MockedClient(CLIENT_API_URL, default_sleep_time=0.1, max_retries=1,
                              sleep_for_rate=expected_sleep_for_rate,
                              min_rate_to_sleep=expected_min_rate_to_sleep,
                              rate_limit_header=expected_rate_limit_header,
                              rate_limit_reset_header=expected_rate_limit_reset_header)

        self.assertEqual(client.sleep_for_rate, expected_sleep_for_rate)
        self.assertEqual(client.min_rate_to_sleep, expected_min_rate_to_sleep)
        self.assertEqual(client.rate_limit_header, expected_rate_limit_header)
        self.assertEqual(client.rate_limit_reset_header, expected_rate_limit_reset_header)

        expected_min_rate_to_sleep = 1000
        client = MockedClient(CLIENT_API_URL, min_rate_to_sleep=expected_min_rate_to_sleep,
                              default_sleep_time=0.1, max_retries=1)
        self.assertEqual(client.min_rate_to_sleep, min(expected_min_rate_to_sleep, RateLimitHandler.MAX_RATE_LIMIT))

    @httpretty.activate
    def test_update_rate_limit(self):
        """Test update rate limit"""

        httpretty.register_uri(httpretty.GET,
                               CLIENT_SPIDERMAN_URL,
                               body="",
                               status=200,
                               forcing_headers={
                                   'X-RateLimit-Remaining': '20',
                                   'X-RateLimit-Reset': '15'
                               })

        httpretty.register_uri(httpretty.GET,
                               CLIENT_SUPERMAN_URL,
                               body="",
                               status=200)

        client = MockedClient(CLIENT_API_URL, default_sleep_time=0.1, max_retries=1)
        response = client.fetch(CLIENT_SPIDERMAN_URL)
        client.update_rate_limit(response)

        self.assertEqual(client.rate_limit, 20)
        self.assertEqual(client.rate_limit_reset_ts, 15)

        client = MockedClient(CLIENT_API_URL, default_sleep_time=0.1, max_retries=1)
        response = client.fetch(CLIENT_SUPERMAN_URL)
        client.update_rate_limit(response)

        self.assertEqual(client.rate_limit, None)
        self.assertEqual(client.rate_limit_reset_ts, None)

    @httpretty.activate
    def test_calculate_rate_limit_not_implemented(self):
        """Test whether a NotImplemented error is raisen when calculate_rate_limit is not defined"""

        reset_time = 1
        httpretty.register_uri(httpretty.GET,
                               CLIENT_SPIDERMAN_URL,
                               body="",
                               status=200,
                               forcing_headers={
                                   'X-RateLimit-Remaining': '20',
                                   'X-RateLimit-Reset': str(reset_time)
                               })

        httpretty.register_uri(httpretty.GET,
                               CLIENT_SUPERMAN_URL,
                               body="",
                               status=200)

        client = MockedClient(CLIENT_API_URL, min_rate_to_sleep=50, default_sleep_time=0.1, max_retries=1)
        response = client.fetch(CLIENT_SPIDERMAN_URL)
        client.update_rate_limit(response)

        with self.assertRaises(NotImplementedError):
            client.sleep_for_rate_limit()


if __name__ == "__main__":
    unittest.main(warnings='ignore')