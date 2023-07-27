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

from abc import ABC
from typing import Any, Optional

from .configuration import Configuration
from ...exceptions import ConfigurationError


class BaseConfiguration(Configuration, ABC):
    """
    Base implementation for other Configuration classes
    """

    def __getitem__(self, key: str) -> Any:
        value: Optional[Any] = self.get(key)
        if value is None:
            raise ConfigurationError(f'Configuration property not found: {key}')
        return value
