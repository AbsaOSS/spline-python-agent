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

from abc import abstractmethod
from unittest.mock import NonCallableMock, Mock

from spline_agent.commons.configuration import Configuration
from spline_agent.dispatcher import LineageDispatcher


# noinspection PyMethodOverriding
class LineageDispatcherMock(LineageDispatcher):
    @property
    @abstractmethod
    def send_plan(self) -> NonCallableMock: pass

    @property
    @abstractmethod
    def send_event(self) -> NonCallableMock: pass


# noinspection PyMethodOverriding
class ConfigurationMock(Configuration):
    @property
    @abstractmethod
    def __getitem__(self) -> Mock: pass

    @property
    @abstractmethod
    def __contains__(self) -> Mock: pass

    @property
    @abstractmethod
    def keys(self) -> Mock: pass
