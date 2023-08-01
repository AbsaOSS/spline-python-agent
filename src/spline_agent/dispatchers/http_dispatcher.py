#  Copyright 2023 ABSA Group Limited
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import logging
from urllib.parse import urljoin

import requests
from http_constants.headers import HttpHeaders
from requests import Response

from spline_agent.dispatcher import LineageDispatcher
from spline_agent.json_serde import to_compact_json_str
from spline_agent.lineage_model import ExecutionEvent, ExecutionPlan

logger = logging.getLogger(__name__)


class HttpLineageDispatcher(LineageDispatcher):
    """
    Lineage dispatcher that sends lineage information
    to the remote endpoint over the REST protocol.
    """

    def __init__(self,
                 base_url: str,
                 plans_url: str,
                 events_url: str,
                 content_type: str,
                 ):
        base_plan_with_slash = f'{base_url}/'
        self.__plans_url = urljoin(base_plan_with_slash, plans_url)
        self.__events_url = urljoin(base_plan_with_slash, events_url)
        self.__content_type = content_type

        logger.info(f"Execution plans URL: {self.__plans_url}")
        logger.info(f"Execution events URL: {self.__events_url}")

    def send_plan(self, plan: ExecutionPlan):
        """POST execution plan"""
        plan_json: str = to_compact_json_str(plan)
        res = self.__do_send(plan_json, self.__plans_url)
        logger.info(f'execution plan sent: {res.status_code}, {res.text}')

    def send_event(self, event: ExecutionEvent):
        """POST execution event"""
        event_json: str = to_compact_json_str([event])
        res = self.__do_send(event_json, self.__events_url)
        logger.info(f'execution event sent: {res.status_code}, {res.text}')

    def __do_send(self, json_payload: str, url: str) -> Response:
        res = requests.post(url=url, data=json_payload, headers={HttpHeaders.CONTENT_TYPE: self.__content_type})
        res.raise_for_status()
        return res
