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

from typing import Type, Optional

from dynaconf import Dynaconf

from .base_configuration import BaseConfiguration
from .configuration import T


class DynaconfConfiguration(BaseConfiguration):
    """
    A Configuration adapter for Dynaconf
    """

    def __init__(self, settings: Dynaconf) -> None:
        self.__settings = settings

    def get(self, key: str, typ: Optional[Type[T]] = None) -> Optional[T]:
        return self.__settings.get(key)

    def keys(self) -> set[str]:
        return {k.lower() for k in self.__settings}
