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

from __future__ import annotations

import logging

from spline_agent.dispatcher import LineageDispatcher
from spline_agent.json_serde import to_pretty_json_str
from spline_agent.lineage_model import ExecutionEvent, ExecutionPlan


class LoggingLineageDispatcher(LineageDispatcher):
    """
    Lineage dispatcher that prints lineage information
    as a prettified JSON string, using the logging framework.
    """

    def __init__(
            self,
            level: str | int,
            logger: str | logging.Logger,
    ):
        """
        :param level: The logging level. Could be specified as an integer or a name ('INFO', 'DEBUG' etc.)
        :param logger: The logger instance or name.
        """
        self.__level = level if isinstance(level, int) else logging.getLevelName(level)
        self.__logger = logger if isinstance(logger, logging.Logger) else logging.getLogger(logger)

    def send_plan(self, plan: ExecutionPlan):
        plan_json: str = to_pretty_json_str(plan)
        self.__logger.log(self.__level, f'Execution Plan: {plan_json}')

    def send_event(self, event: ExecutionEvent):
        event_json: str = to_pretty_json_str(event)
        self.__logger.log(self.__level, f'Execution Event: {event_json}')
