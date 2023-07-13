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
from typing import Optional, Callable

from ordered_set import OrderedSet
from spline_agent.datasources import DataSource


class LineageHarvestingContext:
    def __init__(self):
        self._name: Optional[str] = None
        self._ins: OrderedSet[DataSource] = OrderedSet()
        self._out: Optional[DataSource] = None

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


_context_holder: ContextVar[LineageHarvestingContext] = ContextVar('context')


def get_tracking_context() -> Optional[LineageHarvestingContext]:
    return _context_holder.get(None)


def with_context_do(ctx: LineageHarvestingContext, call: Callable):
    ctx_token = _context_holder.set(ctx)
    try:
        return call()
    finally:
        _context_holder.reset(ctx_token)
