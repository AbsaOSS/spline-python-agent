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

from functools import reduce
from typing import Type, Optional

from dynaconf import Dynaconf

from .base_configuration import BaseConfiguration
from .configuration import T


class DynaconfConfiguration(BaseConfiguration):
    """
    A Configuration adapter for Dynaconf.
    Supports retrieving keys using dot-notation (a.b.c)
    """

    def __init__(self, settings: Dynaconf) -> None:
        self.__settings = settings

    def get(self, key: str, typ: Optional[Type[T]] = None) -> Optional[T]:
        return self.__locate_item(key)

    def __contains__(self, key: str) -> bool:
        return self.__locate_item(key) is not None

    def __locate_item(self, key: str) -> Optional[T]:
        keys = key.split('.')
        root = self.__settings
        return reduce(
            lambda i, k: i.get(k) if i is not None and hasattr(i, 'get') and callable(i.get) else None,
            keys,
            root
        )
