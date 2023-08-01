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

from abc import ABC, abstractmethod
from typing import Any, TypeVar, Type, Optional

T = TypeVar('T')


class Configuration(ABC):
    """
    A composable read-only configuration interface
    """

    @abstractmethod
    def __getitem__(self, key: str) -> Any:
        """
        Get a value by key, or throw an error if not found
        """
        pass

    @abstractmethod
    def get(self, key: str, typ: Optional[Type[T]] = None) -> Optional[T]:
        """
        Returns a value by key, or None is not found
        """
        pass

    @abstractmethod
    def __contains__(self, key: str) -> bool:
        """
        Returns `True` in the given key is present in the config, `False` otherwise.
        Allows for syntax like `if key in config: ...`
        """
        pass
