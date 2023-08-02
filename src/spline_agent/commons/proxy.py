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

from enum import Enum
from typing import TypeVar, Callable, Any, Mapping


class MemberType(Enum):
    GETTER = 0
    SETTER = 1
    METHOD = 2


Target = TypeVar('Target')
MemberName = str
Args = tuple[Any, ...]
Kwargs = Mapping[str, Any]


class ObservingProxy:
    """
    A simple proxy that intercepts any `@property` access and method calls
    on a target object, and notifies an observer.
    The proxy implementation doesn't change any input or output values
    of the invocations. The observer is called purely for side effect purposes.
    """

    InvocationObserver = Callable[[Target, MemberName, MemberType, Args, Kwargs], None]

    def __init__(self, target: Target, observer: InvocationObserver[Target]) -> None:
        # We're using `object.__setattr__(self, 'xxx', yyy)` instead of `self.xxx=yyy`
        # as because of overridden magic methods `__get/setattr__`, when the `xxx` attribute
        # isn't yet in the instance dictionary, the latter expression would cause infinite recursion.
        object.__setattr__(self, f'_{self.__class__.__name__}__target', target)
        object.__setattr__(self, f'_{self.__class__.__name__}__observer', observer)

    def __getattr__(self, name: str):
        value = getattr(self.__target, name)

        if not callable(value):
            # intercept property read access
            self.__observer(self.__target, name, MemberType.GETTER, (), {})
            return value

        # intercept method call
        target_method: Callable = value

        def proxied_method(*args, **kwargs) -> Any:
            self.__observer(self.__target, name, MemberType.METHOD, args, kwargs)
            return target_method(*args, **kwargs)

        return proxied_method

    def __setattr__(self, name: str, value: Any) -> None:
        # intercept property write access
        self.__observer(self.__target, name, MemberType.SETTER, (value,), {})
        setattr(self.__target, name, value)
