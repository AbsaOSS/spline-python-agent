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

import logging
from typing import Generator
from unittest.mock import patch, Mock, create_autospec

import pytest

from spline_agent.dispatchers.logging_dispatcher import LoggingLineageDispatcher
from spline_agent.lineage_model import ExecutionPlan
from ..mocks import LoggerMock


@pytest.fixture
def mock_to_pretty_json_str():
    fn_name = 'spline_agent.dispatchers.logging_dispatcher.to_pretty_json_str'
    with (patch(fn_name, return_value='dummy_json') as patched_fn):
        yield patched_fn


@pytest.fixture
def mock_logger() -> Generator[LoggerMock, None, None]:
    yield create_autospec(logging.Logger)


_DUMMY_PLAN: ExecutionPlan = Mock()


def test_with_logger_instance_and_level_as_integer(mock_logger, mock_to_pretty_json_str):
    # prepare
    dispatcher = LoggingLineageDispatcher(logging.CRITICAL, mock_logger)

    # execute
    dispatcher.send_plan(_DUMMY_PLAN)

    # verify
    mock_logger.log.assert_called_once_with(logging.CRITICAL, 'Execution Plan: dummy_json')


def test_with_logger_by_name_and_level_as_string(mock_logger, mock_to_pretty_json_str):
    # prepare
    with (patch('logging.getLogger', return_value=mock_logger)):
        dispatcher = LoggingLineageDispatcher('WARNING', 'my_logger')

    # execute
    dispatcher.send_plan(_DUMMY_PLAN)

    # verify
    mock_logger.log.assert_called_once_with(logging.WARNING, 'Execution Plan: dummy_json')
