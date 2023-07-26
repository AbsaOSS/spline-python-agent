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

from types import MappingProxyType
from typing import Any

from .configuration import Configuration


class DictConfiguration(Configuration):
    """
    In-memory implementation of Configuration
    """

    def __init__(self, settings: dict[str, Any]) -> None:
        self.settings = MappingProxyType(settings)

    def __getitem__(self, key: str) -> Any:
        return self.settings.get(key)

    def __contains__(self, key: str) -> bool:
        return key in self.settings

    def keys(self) -> set[str]:
        return set(self.settings.keys())
