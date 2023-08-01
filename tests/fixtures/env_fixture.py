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

import os
from typing import Callable

import pytest

EnvVars = dict[str, str]


@pytest.fixture
def set_env_vars():
    # a place to store original env vars when they are set by the test
    original_env_vars: EnvVars = {}

    # create a fixture instance and run the test
    yield _set_env_vars(original_env_vars)

    # restore original values
    for key, original_value in original_env_vars.items():
        if original_value is None:
            del os.environ[key]
        else:
            os.environ[key] = original_value


def _set_env_vars(original_env_vars: EnvVars) -> Callable[[EnvVars], None]:
    def impl(test_env_vars: EnvVars):
        for key, value in test_env_vars.items():
            # save the original value, so we can restore it later
            original_env_vars[key] = os.environ.get(key)  # type: ignore
            # set the new value for the test
            os.environ[key] = str(value)

    return impl
