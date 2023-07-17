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

import spline_agent
from spline_agent.lineage_model import NameAndVersion

__pkg_meta = importlib.metadata.metadata(spline_agent.__name__)

AGENT_INFO = NameAndVersion(
    name=__pkg_meta['name'],
    version=__pkg_meta['version']
)