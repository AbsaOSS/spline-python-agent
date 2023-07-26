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

from typing import Any

from .configuration import Configuration


class CompositeConfiguration(Configuration):
    """
    Composite implementation that merges one or more other Configuration instances.
    When looking up by a key, the first value found is returned. The search is depth-first.
    """

    def __init__(self, *configs: Configuration) -> None:
        assert configs
        self.__configs = configs

    def __getitem__(self, key: str) -> Any:
        for config in self.__configs:
            if key in config:
                return config.__getitem__(key)
        return None

    def __contains__(self, key: str) -> bool:
        return any(key in config for config in self.__configs)

    def keys(self) -> set[str]:
        all_keys = set()
        for config in self.__configs:
            all_keys.update(config.keys())
        return set(all_keys)
