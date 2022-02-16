# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2020 Bitergia
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
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
# Authors:
#     Valerio Cosentino <valcos@bitergia.com>
#     Santiago Dueñas <sduenas@bitergia.com>
#     Jesus M. Gonzalez-Barahona <jgb@gsyc.es>
#     Harshal Mittal <harshalmittal4@gmail.com>
#

import json
import logging
from datetime import datetime


from grimoirelab_toolkit.datetime import datetime_to_utc, datetime_utcnow

from ...backend import (Backend,
                        BackendCommand,
                        BackendCommandArgumentParser)
from ...client import HttpClient, RateLimitHandler
from ...utils import DEFAULT_DATETIME


CATEGORY_EVENT = "event"

MEETUP_URL = 'https://meetup.com/'
MEETUP_API_URL = 'https://api.meetup.com/gql'
MAX_ITEMS = 10


# Range before sleeping until rate limit reset
MIN_RATE_LIMIT = 1

# Time to avoid too many request exception
SLEEP_TIME = 30

QUERY_EVENT_FULL = """
{
  id
  title
  eventUrl
  description
  shortDescription
  dateTime
  host {
    id
    name
    memberPhoto {
      id
      baseUrl
    }
  }
  howToFindUs
  maxTickets
  group {
    id
    foundedDate
    joinMode
    name
    urlname
    latitude
    longitude
    customMemberLabel
    topics {
      urlkey
      name
    }
    stats {
      memberCounts {
        all
      }
    }
  }
  venue {
    id
    name
    address
    city
    state
    postalCode
    country
    lat
    lng
  }
  status
  endTime
  createdAt
  going
  waiting
}
"""

QUERY_EVENT_COMMENTS = """
{
  comments (offset: %s, limit: %s) {
    count
    edges {
      node {
        id
        created
        likeCount
        link
        text
        member {
          id
          name
          memberPhoto {
            id
            baseUrl
          }
        }
      }
    }     
  }
}
"""

QUERY_EVENT_TICKETS = """
{
  tickets (input: {first: %d, after: %s}) {
    pageInfo {
      hasNextPage
      endCursor
    }
    edges {
      node {
        createdAt
        updatedAt
        status
        membership {
          role
        }
        guestsCount
        user {
          id
          name
          memberPhoto {
            id
            baseUrl
          }
        }
      }
    }
  }
}
"""

QUERY_EVENT_DATE = """
{
  id
  dateTime
}
"""

QUERY_GROUP_EVENTS_TEMPLATE = """
{
  groupByUrlname(urlname: "%s") {
    %s (input: {first: %d, after: %s}) {
      count
      pageInfo {
        hasNextPage
        endCursor
      }
      edges {
        cursor
        node %s
      }
    }
  }
}
"""

QUERY_EVENT_TEMPLATE = """
{
  event(id: "%s") %s
}
"""

logger = logging.getLogger(__name__)


class Meetup(Backend):
    """Meetup backend.

    This class allows to fetch the events of a group from the
    Meetup server. Initialize this class passing the OAuth2 token needed
    for authentication with the parameter `api_token`.

    :param group: name of the group where data will be fetched
    :param api_token: OAuth2 token to access the API
    :param max_items:  maximum number of events requested on the same query
    :param tag: label used to mark the data
    :param archive: archive to store/retrieve items
    :param sleep_for_rate: sleep until rate limit is reset
    :param min_rate_to_sleep: minimum rate needed to sleep until
         it will be reset
    :param sleep_time: time (in seconds) to sleep in case
        of connection problems
    :param ssl_verify: enable/disable SSL verification
    """
    version = '0.17.0'

    CATEGORIES = [CATEGORY_EVENT]
    CLASSIFIED_FIELDS = [
        ['group', 'topics'],
        ['event_hosts'],
        ['rsvps'],
        ['venue']
    ]
    EXTRA_SEARCH_FIELDS = {
        'group_name': ['group', 'name'],
        'group_id': ['group', 'id']
    }

    def __init__(self, group, api_token,
                 max_items=MAX_ITEMS, tag=None, archive=None,
                 sleep_for_rate=False, min_rate_to_sleep=MIN_RATE_LIMIT,
                 sleep_time=SLEEP_TIME, ssl_verify=True):
        origin = MEETUP_URL

        super().__init__(origin, tag=tag, archive=archive, ssl_verify=ssl_verify)
        self.group = group
        self.max_items = max_items
        self.api_token = api_token
        self.sleep_for_rate = sleep_for_rate
        self.min_rate_to_sleep = min_rate_to_sleep
        self.sleep_time = sleep_time

        self.client = None

    def fetch(self, category=CATEGORY_EVENT, from_date=DEFAULT_DATETIME, to_date=None,
              filter_classified=False):
        """Fetch the events from the server.

        This method fetches those events of a group stored on the server
        that were updated since the given date. Data comments and rsvps
        are included within each event.

        :param category: the category of items to fetch
        :param from_date: obtain events updated since this date
        :param to_date: obtain events updated before this date
        :param filter_classified: remove classified fields from the resulting items

        :returns: a generator of events
        """
        if not from_date:
            from_date = DEFAULT_DATETIME

        from_date = datetime_to_utc(from_date)

        kwargs = {"from_date": from_date}
        items = super().fetch(category,
                              filter_classified=filter_classified,
                              **kwargs)

        return items

    def fetch_items(self, category, **kwargs):
        """Fetch the events

        :param category: the category of items to fetch
        :param kwargs: backend arguments

        :returns: a generator of items
        """
        from_date = kwargs['from_date']

        logger.info("Fetching events of '%s' group from %s",
                    self.group, str(from_date))

        nevents = 0
        events = self.client.events(self.group, from_date=from_date)

        for event in events:
            event_id = event['id']

            event['comments'] = self.__fetch_and_parse_comments(event_id)
            event['rsvps'] = self.__fetch_and_parse_rsvps(event_id)
            event['fetched_on'] = datetime_utcnow().timestamp()

            yield event
            nevents += 1

        logger.info("Fetch process completed: %s events fetched", nevents)

    @classmethod
    def has_archiving(cls):
        """Returns whether it supports archiving items on the fetch process.

        :returns: this backend does not support items archive
        """
        return False

    @classmethod
    def has_resuming(cls):
        """Returns whether it supports to resume the fetch process.

        :returns: this backend does not support items resuming
        """
        return False

    @staticmethod
    def metadata_id(item):
        """Extracts the identifier from a Meetup item."""

        return str(item['id'])

    @staticmethod
    def metadata_updated_on(item):
        """Extracts and coverts the update time from a Docker Hub item.

        The timestamp is extracted from 'fetched_on' field. This field
        is not part of the data provided by Meetup. It is added
        by this backend.

        :param item: item generated by the backend

        :returns: a UNIX timestamp
        """
        return item['fetched_on']

    @staticmethod
    def metadata_category(item):
        """Extracts the category from a Meetup item.

        This backend only generates one type of item which is
        'event'.
        """
        return CATEGORY_EVENT

    def _init_client(self, from_archive=False):
        """Init client"""

        return MeetupClient(self.api_token, self.max_items,
                            self.sleep_for_rate, self.min_rate_to_sleep, self.sleep_time,
                            self.archive, from_archive, self.ssl_verify)

    def __fetch_and_parse_comments(self, event_id):
        logger.debug("Fetching and parsing comments from event '%s'",
                     str(event_id))

        comments = []
        pages = self.client.comments(event_id)

        for page in pages:
            for comment in page:
                comments.append(comment['node'])

        return comments

    def __fetch_and_parse_rsvps(self, event_id):
        logger.debug("Fetching and parsing rsvps from event '%s'",
                     str(event_id))

        rsvps = []
        pages = self.client.rsvps(event_id)

        for page in pages:
            for rsvp in page:
                rsvps.append(rsvp['node'])

        return rsvps


class MeetupCommand(BackendCommand):
    """Class to run Meetup backend from the command line."""

    BACKEND = Meetup

    @classmethod
    def setup_cmd_parser(cls):
        """Returns the Meetup argument parser."""

        parser = BackendCommandArgumentParser(cls.BACKEND,
                                              from_date=True,
                                              token_auth=True,
                                              ssl_verify=True)

        # Meetup options
        group = parser.parser.add_argument_group('Meetup arguments')
        group.add_argument('--max-items', dest='max_items',
                           type=int, default=MAX_ITEMS,
                           help="Maximum number of items requested on the same query")
        group.add_argument('--sleep-for-rate', dest='sleep_for_rate',
                           action='store_true',
                           help="sleep for getting more rate")
        group.add_argument('--min-rate-to-sleep', dest='min_rate_to_sleep',
                           default=MIN_RATE_LIMIT, type=int,
                           help="sleep until reset when the rate limit reaches this value")
        group.add_argument('--sleep-time', dest='sleep_time',
                           default=SLEEP_TIME, type=int,
                           help="minimun sleeping time to avoid too many request exception")

        # Required arguments
        parser.parser.add_argument('group',
                                   help="Meetup group name")

        return parser


class MeetupClient(HttpClient, RateLimitHandler):
    """Meetup API client.

    Client for fetching information from the Meetup server
    using its GraphQL API.

    :param api_token: OAuth2 token needed to access the API
    :param max_items: maximum number of items per request
    :param sleep_for_rate: sleep until rate limit is reset
    :param min_rate_to_sleep: minimum rate needed to sleep until
         it will be reset
    :param sleep_time: time (in seconds) to sleep in case
        of connection problems
    :param archive: an archive to store/read fetched data
    :param from_archive: it tells whether to write/read the archive
    :param ssl_verify: enable/disable SSL verification
    """

    RCOMMENTS = 'comments'
    RTICKETS = 'tickets'

    PKEY_OAUTH2 = 'Authorization'

    VPAST_EVENTS = "pastEvents"
    VFUTURE_EVENTS = "upcomingEvents"
    VEVENT_TYPES = [VPAST_EVENTS, VFUTURE_EVENTS]

    HCONTENT_TYPE = 'Content-Type'
    VCONTENT_TYPE = 'application/json'

    def __init__(self, api_token, max_items=MAX_ITEMS,
                 sleep_for_rate=False, min_rate_to_sleep=MIN_RATE_LIMIT, sleep_time=SLEEP_TIME,
                 ssl_verify=True):
        self.api_token = api_token
        self.max_items = max_items

        super().__init__(MEETUP_API_URL, sleep_time=sleep_time, ssl_verify=ssl_verify)
        super().setup_rate_limit_handler(sleep_for_rate=sleep_for_rate, min_rate_to_sleep=min_rate_to_sleep)

    def calculate_time_to_reset(self):
        """Number of seconds to wait. They are contained in the rate limit reset header"""

        time_to_reset = 0 if self.rate_limit_reset_ts < 0 else self.rate_limit_reset_ts
        return time_to_reset

    def event(self, event_id):
        """Fetch a full event from the API"""

        query = QUERY_EVENT_TEMPLATE % (event_id, QUERY_EVENT_FULL)
        event = self._fetch(query)
        return event['data']['event']

    def events(self, group, from_date=DEFAULT_DATETIME):
        """
        Fetch the events for a given group.

        It is divided in nested two steps:
        - Get from all the events the id and date.
        - Fetch only the event information after a specific date.
        """

        event_idx = 0
        event_type = self.VEVENT_TYPES[event_idx]
        cursor = 'null'

        while True:
            cursor = cursor if cursor == 'null' else '"{}"'.format(cursor)
            query = QUERY_GROUP_EVENTS_TEMPLATE % (group, event_type, self.max_items,
                                                   cursor, QUERY_EVENT_DATE)
            page = self._fetch(query)
            if not page['data']['groupByUrlname']:
                logger.error("Can't get meetup group: %s", group)
                raise Exception("Can't get meetup group: %s" % group)

            events_page = page['data']['groupByUrlname'][event_type]
            for node in events_page['edges']:
                event = node['node']
                date = datetime.strptime(event['dateTime'], '%Y-%m-%dT%H:%M%z')
                if date > from_date:
                    yield self.event(event['id'])

            if events_page['pageInfo']['hasNextPage']:
                cursor = events_page['pageInfo']['endCursor']
            elif event_idx < len(self.VEVENT_TYPES)-1:
                event_idx += 1
                event_type = self.VEVENT_TYPES[event_idx]
                cursor = 'null'
            else:
                break

    def comments(self, event_id):
        """Fetch the comments of a given event."""

        do_fetch = True
        ncomments = 0

        while do_fetch:
            comments_query = QUERY_EVENT_COMMENTS % (ncomments, self.max_items)
            query = QUERY_EVENT_TEMPLATE % (event_id, comments_query)

            event = self._fetch(query)
            comments_node = event['data']['event'][self.RCOMMENTS]

            yield comments_node['edges']

            ncomments += self.max_items
            if ncomments >= comments_node['count']:
                do_fetch = False

    def rsvps(self, event_id):
        """Fetch the rsvps (tickets) of a given event."""

        do_fetch = True
        cursor = 'null'

        while do_fetch:
            cursor = cursor if cursor == 'null' else '"{}"'.format(cursor)
            tickets_query = QUERY_EVENT_TICKETS % (self.max_items, cursor)
            query = QUERY_EVENT_TEMPLATE % (event_id, tickets_query)

            event = self._fetch(query)
            tickets_node = event['data']['event'][self.RTICKETS]

            yield tickets_node['edges']

            if tickets_node['pageInfo']['hasNextPage']:
                cursor = tickets_node['pageInfo']['endCursor']
            else:
                do_fetch = False

    @staticmethod
    def sanitize_for_archive(url, headers, payload):
        """Sanitize payload of a HTTP request by removing the token information
        before storing/retrieving archived items
        :param: url: HTTP url request
        :param: headers: HTTP headers request
        :param: payload: HTTP payload request
        :returns url, headers and the sanitized payload
        """
        if MeetupClient.PKEY_OAUTH2 in headers:
            headers.pop(MeetupClient.PKEY_OAUTH2)

        return url, headers, payload

    def _fetch(self, query):
        """Fetch a resource using the query.

        :param query: query to be transmitted

        :returns: json result of the query
        """
        headers = {
            self.PKEY_OAUTH2: 'Bearer {}'.format(self.api_token),
            self.HCONTENT_TYPE: self.VCONTENT_TYPE
        }

        logger.debug("Meetup client query: %s", str(query))

        if not self.from_archive:
            self.sleep_for_rate_limit()

        r = self.fetch(url=self.base_url,
                       payload=json.dumps({'query': query}),
                       headers=headers,
                       method=HttpClient.POST)

        return r.json()
