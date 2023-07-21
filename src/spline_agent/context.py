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

from contextvars import ContextVar
from enum import Enum
from enum import auto
from typing import Optional, Callable

from ordered_set import OrderedSet

from spline_agent.datasources import DataSource
from spline_agent.exceptions import LineageContextNotInitialized
from spline_agent.lineage_model import NameAndVersion


class WriteMode(Enum):
    APPEND = auto()
    OVERWRITE = auto()


class LineageTrackingContext:
    def __init__(self):
        self._name: Optional[str] = None
        self._ins: OrderedSet[DataSource] = OrderedSet()
        self._out: Optional[DataSource] = None
        self._write_mode: Optional[WriteMode] = None
        self._system_info: Optional[NameAndVersion] = None

    @property
    def name(self) -> Optional[str]:
        return self._name

    @name.setter
    def name(self, value: str):
        # todo: warning if _name is reassigned
        self._name = value

    @property
    def inputs(self) -> tuple[DataSource, ...]:
        return tuple(self._ins)

    def add_input(self, ds: DataSource):
        # todo: warning if ds is already registered
        self._ins.add(ds)

    @property
    def output(self) -> Optional[DataSource]:
        return self._out

    @output.setter
    def output(self, ds: DataSource):
        # todo: warning if _out is reassigned
        self._out = ds

    @property
    def write_mode(self) -> Optional[WriteMode]:
        return self._write_mode

    @write_mode.setter
    def write_mode(self, mode: WriteMode):
        self._write_mode = mode

    @property
    def system_info(self) -> Optional[NameAndVersion]:
        return self._system_info

    @system_info.setter
    def system_info(self, mode: NameAndVersion):
        self._system_info = mode


_context_holder: ContextVar[LineageTrackingContext] = ContextVar('context')


def get_tracking_context() -> LineageTrackingContext:
    from .decorator import track_lineage

    ctx = _context_holder.get(None)
    if ctx is None:
        this_fn_name = get_tracking_context.__name__
        decorator_name = track_lineage.__name__
        raise LineageContextNotInitialized(
            f"The function `{this_fn_name}()` was called outside of `@{decorator_name}()` decorator")
    return ctx


def with_context_do(ctx: LineageTrackingContext, call: Callable):
    ctx_token = _context_holder.set(ctx)
    try:
        return call()
    finally:
        _context_holder.reset(ctx_token)
