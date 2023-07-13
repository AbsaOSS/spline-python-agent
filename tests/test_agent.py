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

from spline_agent.decorator import track_lineage
from spline_agent.context import get_tracking_context
from spline_agent.datasources import DataSource


def test_decorator_calls_func_and_returns_value():
    @track_lineage()
    def test_func(x: int, y: int): return x + y

    assert test_func(2, 3) == 5


def test_context_mgmt():
    @track_lineage()
    def test_func():
        assert get_tracking_context() is not None

    assert get_tracking_context() is None

    test_func()

    assert get_tracking_context() is None


def test_decorator_with_default_args():
    @track_lineage()
    def test_func():
        assert get_tracking_context().name == 'test_func'
        assert len(get_tracking_context().ins) == 0
        assert get_tracking_context().out is None

    test_func()


def test_decorator_with_provided_args():
    @track_lineage(name="My test app", ins=('foo', '{arg1}', '{arg2}'), out='qux')
    def my_test_func(arg1: str, arg2: DataSource):
        assert get_tracking_context().name == "My test app"
        assert get_tracking_context().ins == (DataSource('foo'), DataSource(arg1), arg2)
        assert get_tracking_context().out == DataSource('qux')

    my_test_func('bar', arg2=DataSource('baz'))


def test_decorator_with_name_as_expression():
    @track_lineage(name="{arg1}")
    def my_test_func(arg1: str):
        assert get_tracking_context().name == arg1

    my_test_func('foo')
