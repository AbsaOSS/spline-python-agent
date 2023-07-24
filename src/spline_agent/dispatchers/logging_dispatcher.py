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
            level: int = logging.INFO,
            logger: logging.Logger = logging.getLogger(__name__),
    ):
        self.level = level
        self.logger = logger

    def send_plan(self, plan: ExecutionPlan):
        plan_json: str = to_pretty_json_str(plan)
        self.logger.log(self.level, f'Execution Plan: {plan_json}')

    def send_event(self, event: ExecutionEvent):
        event_json: str = to_pretty_json_str(event)
        self.logger.log(self.level, f'Execution Event: {event_json}')
