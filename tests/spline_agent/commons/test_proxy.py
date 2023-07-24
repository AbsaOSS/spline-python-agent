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

from typing import Any, cast
from unittest.mock import create_autospec, Mock

from spline_agent.commons.proxy import ObservingProxy, MemberType


class Foo:
    def __init__(self):
        self._my_prop = None

    # noinspection PyMethodMayBeStatic
    def my_plus(self, a: int, b: int):
        return a + b

    @property
    def my_prop(self) -> Any:
        return self._my_prop

    @my_prop.setter
    def my_prop(self, value: Any):
        self._my_prop = value


def test_method_call_interception():
    # prepare
    test_object: Foo = Foo()
    test_observer: Mock = create_autospec(ObservingProxy.InvocationObserver)
    test_proxy: Foo = cast(Foo, ObservingProxy(test_object, test_observer))

    # execute
    test_proxy.my_plus(1, 2)
    test_proxy.my_plus(a=2, b=3)

    seven = test_proxy.my_plus(3, 4)

    # verify
    assert seven == 7
    assert test_observer.call_count == 3
    assert test_observer.call_args_list == [
        ((test_object, 'my_plus', MemberType.METHOD, (1, 2), {}), {}),
        ((test_object, 'my_plus', MemberType.METHOD, (), {'a': 2, 'b': 3}), {}),
        ((test_object, 'my_plus', MemberType.METHOD, (3, 4), {}), {}),
    ]


def test_property_access_interception():
    # prepare
    test_object: Foo = Foo()
    test_observer: Mock = create_autospec(ObservingProxy.InvocationObserver)
    test_proxy: Foo = cast(Foo, ObservingProxy(test_object, test_observer))

    # execute
    # ... one
    test_proxy.my_prop = 'one'
    _ = test_proxy.my_prop
    # ... two
    test_proxy.my_prop = 'two'

    # verify
    assert test_proxy.my_prop == 'two'
    assert test_observer.call_count == 4
    assert test_observer.call_args_list == [
        ((test_object, 'my_prop', MemberType.SETTER, ('one',), {}), {}),
        ((test_object, 'my_prop', MemberType.GETTER, (), {}), {}),
        ((test_object, 'my_prop', MemberType.SETTER, ('two',), {}), {}),
        ((test_object, 'my_prop', MemberType.GETTER, (), {}), {}),
    ]
