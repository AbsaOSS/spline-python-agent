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

from . import DynaconfConfiguration
from .base_configuration import BaseConfiguration
from .configuration import T


class FileConfiguration(BaseConfiguration):
    """
    Loads settings from YAML, TOML, INI and other config files.
    """

    def __init__(self, file_path: str) -> None:
        """
        :param file_path: absolute path to the config file
        """
        self.__dynaconf = DynaconfConfiguration(Dynaconf(settings_files=[file_path]))

    def get(self, key: str, typ: Optional[Type[T]] = None) -> Optional[T]:
        return self.__dynaconf.get(key)

    def __contains__(self, key: str) -> bool:
        return self.__dynaconf.__contains__(key)
