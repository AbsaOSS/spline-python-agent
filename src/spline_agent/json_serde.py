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

import json
import uuid
from dataclasses import asdict
from json import JSONEncoder
from typing import Any

from spline_agent.lineage_model import Lineage, ExecutionPlan, ExecutionEvent


def to_compact_json_str(obj: Any):
    json_str = json.dumps(obj, cls=LineageEncoder, indent=0)
    return json_str


def to_pretty_json_str(obj: Any):
    json_str = json.dumps(obj, cls=LineageEncoder, indent=4)
    return json_str


class LineageEncoder(JSONEncoder):
    def default(self, o: Any) -> Any:
        if isinstance(o, uuid.UUID):
            return str(o)
        elif isinstance(o, Lineage) or isinstance(o, ExecutionPlan) or isinstance(o, ExecutionEvent):
            return asdict(o)
        return super().default(o)
