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
from contextvars import ContextVar
from typing import Optional, Callable, List

from spline_agent.datasources import DataSource
from spline_agent.enums import WriteMode
from spline_agent.exceptions import LineageTrackingContextNotInitialized
from spline_agent.lineage_model import NameAndVersion

logger = logging.getLogger(__name__)


class LineageTrackingContext:
    def __init__(self):
        self._name: Optional[str] = None
        self._ins: List[DataSource] = []
        self._out: Optional[DataSource] = None
        self._write_mode: Optional[WriteMode] = None
        self._system_info: Optional[NameAndVersion] = None

    @property
    def name(self) -> Optional[str]:
        return self._name

    @name.setter
    def name(self, value: str):
        assert value is not None
        if self._name is not None:
            logger.warning(f"Tracking context property 'name' is reassigned: "
                           f"old value '{self._name}', new value '{value}'")
        self._name = value

    @property
    def inputs(self) -> tuple[DataSource, ...]:
        return tuple(self._ins)

    def add_input(self, ds: DataSource):
        self._ins.append(ds)

    @property
    def output(self) -> Optional[DataSource]:
        return self._out

    @output.setter
    def output(self, ds: DataSource):
        assert ds is not None
        if self._out is not None:
            logger.warning(f"Tracking context property 'output' is reassigned: "
                           f"old value '{self._out}', new value '{ds}'")
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
        raise LineageTrackingContextNotInitialized(
            f"The function '{this_fn_name}()' must be called from inside a function, that itself or any of its callers "
            f"is decorated with the '@{decorator_name}()' decorator. "
            f"Also the Spline mode must not be DISABLED. (If you want to disable Spline temporarily use mode BYPASS)")
    return ctx


def with_context_do(ctx: LineageTrackingContext, call: Callable):
    ctx_token = _context_holder.set(ctx)
    try:
        return call()
    finally:
        _context_holder.reset(ctx_token)
