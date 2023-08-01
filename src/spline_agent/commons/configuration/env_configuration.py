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

from typing import Type, Optional

from dynaconf import Dynaconf

from . import DynaconfConfiguration
from .base_configuration import BaseConfiguration
from .configuration import T


class EnvConfiguration(BaseConfiguration):
    """
    Loads settings from environment variable.
    It loads environment variables with the specified prefix.
    """

    def __init__(self, prefix: str = '') -> None:
        """
        :param prefix: environment variable name prefix
        """
        dynaconf_env_prefix: str | bool = prefix.replace('.', '_') if prefix else False
        self.__dynaconf = DynaconfConfiguration(Dynaconf(envvar_prefix=dynaconf_env_prefix))
        self.__prefix = f'{prefix}.' if prefix else ''
        self.__prefix_len = len(self.__prefix)

    def get(self, key: str, typ: Optional[Type[T]] = None) -> Optional[T]:
        return self.__dynaconf.get(self.__encode_key(key)) if key.startswith(self.__prefix) else None

    def __contains__(self, key: str) -> bool:
        return key.startswith(self.__prefix) and self.__dynaconf.__contains__(self.__encode_key(key))

    def __encode_key(self, key: str) -> str:
        return key[self.__prefix_len:].replace('.', '_')
