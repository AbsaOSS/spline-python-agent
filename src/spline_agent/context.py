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
from spline_agent.exceptions import LineageTrackingContextNotInitializedError
from spline_agent.lineage_model import NameAndVersion

logger = logging.getLogger(__name__)


class LineageTrackingContext:
    def __init__(self):
        self.__name: Optional[str] = None
        self.__ins: List[DataSource] = []
        self.__out: Optional[DataSource] = None
        self.__write_mode: Optional[WriteMode] = None
        self.__system_info: Optional[NameAndVersion] = None

    @property
    def name(self) -> Optional[str]:
        return self.__name

    @name.setter
    def name(self, value: str):
        assert value is not None
        if self.__name is not None:
            logger.warning(f"Tracking context property 'name' is reassigned: "
                           f"old value '{self.__name}', new value '{value}'")
        self.__name = value

    @property
    def inputs(self) -> tuple[DataSource, ...]:
        return tuple(self.__ins)

    def add_input(self, ds: DataSource):
        self.__ins.append(ds)

    @property
    def output(self) -> Optional[DataSource]:
        return self.__out

    @output.setter
    def output(self, ds: DataSource):
        assert ds is not None
        if self.__out is not None:
            logger.warning(f"Tracking context property 'output' is reassigned: "
                           f"old value '{self.__out}', new value '{ds}'")
        self.__out = ds

    @property
    def write_mode(self) -> Optional[WriteMode]:
        return self.__write_mode

    @write_mode.setter
    def write_mode(self, mode: WriteMode):
        self.__write_mode = mode

    @property
    def system_info(self) -> Optional[NameAndVersion]:
        return self.__system_info

    @system_info.setter
    def system_info(self, mode: NameAndVersion):
        self.__system_info = mode


_context_holder: ContextVar[LineageTrackingContext] = ContextVar('context')


def get_tracking_context() -> LineageTrackingContext:
    from spline_agent.decorators.track_lineage_decorator import track_lineage

    ctx = _context_holder.get(None)
    if ctx is None:
        this_fn_name = get_tracking_context.__name__
        decorator_name = track_lineage.__name__
        raise LineageTrackingContextNotInitializedError(
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
