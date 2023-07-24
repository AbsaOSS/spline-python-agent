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

from enum import Enum


class WriteMode(Enum):
    OVERWRITE = 0
    APPEND = 1


class SplineMode(Enum):
    DISABLED = 0  # Fully disabled, the decorator is no-op.
    ENABLED = 1  # Fully enabled
    BYPASS = 2  # The context management is enabled (to avoid None errors in client code), but the side effect is zero.
