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

from typing import Optional
from unittest.mock import create_autospec

import pytest

from spline_agent.context import WriteMode, get_tracking_context, LineageTrackingContext
from spline_agent.datasources import DataSource
from spline_agent.decorator import track_lineage
from spline_agent.dispatcher import LineageDispatcher
from spline_agent.enums import SplineMode
from spline_agent.exceptions import LineageTrackingContextNotInitialized, LineageTrackingContextIncompleteError
from spline_agent.lineage_model import NameAndVersion
from .mocks import LineageDispatcherMock


def test_decorator_calls_func_and_returns_value():
    # prepare
    dummy_dispatcher = create_autospec(LineageDispatcher)
    dummy_output = DataSource('dummy')
    dummy_si = NameAndVersion(name="dummy", version="dummy")

    @track_lineage(dispatcher=dummy_dispatcher, output=dummy_output, write_mode=WriteMode.APPEND, system_info=dummy_si)
    def test_plus_func(x: int, y: int): return x + y

    # execute
    five = test_plus_func(2, 3)

    # verify
    assert five == 5


def test_context_mgmt():
    # prepare
    dummy_dispatcher = create_autospec(LineageDispatcher)
    dummy_output = DataSource('dummy')
    dummy_si = NameAndVersion(name="dummy", version="dummy")

    captured_ctx: Optional[LineageTrackingContext] = None

    @track_lineage(dispatcher=dummy_dispatcher, output=dummy_output, write_mode=WriteMode.APPEND, system_info=dummy_si)
    def test_func():
        nonlocal captured_ctx
        captured_ctx = get_tracking_context()

    # verify pre-conditions:
    # - the context does not exist yet outside decorated function
    with pytest.raises(LineageTrackingContextNotInitialized):
        get_tracking_context()

    # execute
    test_func()

    # verify post-conditions:
    # - the context used to exist inside decorated function
    assert captured_ctx is not None
    # - the context does not exist anymore outside decorated function
    with pytest.raises(LineageTrackingContextNotInitialized):
        get_tracking_context()


def test_decorator_mode_bypass():
    # prepare
    ctx: Optional[LineageTrackingContext] = None

    @track_lineage(mode=SplineMode.BYPASS)
    def test_bypass_plus_func(a: int, b: int) -> int:
        # verify the context is accessible and no error is thrown
        nonlocal ctx
        ctx = get_tracking_context()
        return a + b

    # execute
    five = test_bypass_plus_func(2, 3)

    # verify
    assert ctx is not None
    ctx.name = 'foo'
    ctx.add_input(DataSource('bar'))

    assert five == 5
    assert ctx.name == "foo"
    assert ctx.inputs == (DataSource('bar'),)


def test_decorator_mode_disabled__no_context_access():
    # prepare
    @track_lineage(mode=SplineMode.DISABLED)
    def test_untracked_plus_func(a: int, b: int) -> int:
        return a + b

    # execute
    five = test_untracked_plus_func(2, 3)

    # verify
    assert five == 5


def test_decorator_mode_disabled__with_context_access():
    # prepare
    @track_lineage(mode=SplineMode.DISABLED)
    def test_untracked_func():
        get_tracking_context()

    # execute and verify
    with pytest.raises(LineageTrackingContextNotInitialized):
        test_untracked_func()


def test_decorator_with_default_args__no_capture_lineage():
    # prepare
    mock_dispatcher: LineageDispatcherMock = create_autospec(LineageDispatcher)

    ctx: Optional[LineageTrackingContext] = None

    @track_lineage()
    def test_func():
        nonlocal ctx
        ctx = get_tracking_context()

    # execute
    with pytest.raises(LineageTrackingContextIncompleteError):
        test_func()

    # verify
    assert ctx is not None
    assert ctx.name == 'test_func'
    assert len(ctx.inputs) == 0
    assert ctx.output is None
    assert ctx.write_mode is None
    assert ctx.system_info is None

    # we didn't provide required configuration, so no lineage should be captured
    mock_dispatcher.send_plan.assert_not_called()
    mock_dispatcher.send_event.assert_not_called()


def test_decorator_with_provided_args__capture_lineage():
    # prepare
    mock_dispatcher: LineageDispatcherMock = create_autospec(LineageDispatcher)
    ctx: Optional[LineageTrackingContext] = None

    # noinspection PyUnusedLocal
    @track_lineage(
        name='My test app',
        inputs=('foo', '{arg1}', '{arg2}'),
        output='qux',
        write_mode=WriteMode.OVERWRITE,
        system_info=NameAndVersion('dummy system', 'dummy version'),
        dispatcher=mock_dispatcher
    )
    def my_test_func(arg1: str, arg2: DataSource):
        nonlocal ctx
        ctx = get_tracking_context()

    # execute
    my_test_func('bar', arg2=DataSource('baz'))

    # verify
    assert ctx is not None
    assert ctx.name == "My test app"
    assert ctx.inputs == (DataSource('foo'), DataSource('bar'), DataSource('baz'))
    assert ctx.output == DataSource('qux')

    mock_dispatcher.send_plan.assert_called_once()
    mock_dispatcher.send_event.assert_called_once()


def test_decorator_with_name_as_expression():
    # prepare
    dummy_dispatcher = create_autospec(LineageDispatcher)
    dummy_output = DataSource('dummy')
    dummy_si = NameAndVersion(name="dummy", version="dummy")

    ctx: Optional[LineageTrackingContext] = None

    # noinspection PyUnusedLocal
    @track_lineage(name='{arg1}',
                   dispatcher=dummy_dispatcher,
                   output=dummy_output,
                   write_mode=WriteMode.APPEND,
                   system_info=dummy_si)
    def my_test_func(arg1: str):
        nonlocal ctx
        ctx = get_tracking_context()

    # execute
    my_test_func('foo')

    # verify
    assert ctx is not None
    assert ctx.name == 'foo'
