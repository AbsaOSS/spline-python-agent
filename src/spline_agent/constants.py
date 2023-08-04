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

import importlib.metadata
import os
import platform
from uuid import UUID

import spline_agent
from spline_agent.lineage_model import NameAndVersion

AGENT_INFO = NameAndVersion(
    name='spline-python-agent',
    version=importlib.metadata.version(spline_agent.__name__)
)

DEFAULT_SYSTEM_INFO = NameAndVersion(
    name=platform.python_implementation(),
    version=platform.python_version()
)

EXECUTION_PLAN_NAMESPACE: UUID = UUID('475196d0-16ca-4cba-aec7-c9f2ddd9326c')

CONFIG_FILE_DEFAULT = f'{os.path.dirname(__file__)}/spline.default.yaml'
CONFIG_FILE_USER = f'{os.getcwd()}/spline.yaml'
