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

from spline_agent.dispatcher import LineageDispatcher
from spline_agent.json_serde import to_pretty_json_str
from spline_agent.lineage_model import ExecutionEvent, ExecutionPlan


class ConsoleLineageDispatcher(LineageDispatcher):
    """
    Lineage dispatcher that prints lineage information
    to the standard output as a prettified JSON string.
    """

    def send_plan(self, plan: ExecutionPlan):
        plan_json: str = to_pretty_json_str(plan)
        print(f'Execution Plan: {plan_json}')

    def send_event(self, event: ExecutionEvent):
        event_json: str = to_pretty_json_str(event)
        print(f'Execution Event: {event_json}')
