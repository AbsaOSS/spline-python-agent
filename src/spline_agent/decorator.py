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

import inspect
import logging
import time
from functools import wraps
from typing import Optional, Union, Mapping, Any
from urllib.parse import urlparse

from spline_agent.context import with_context_do, LineageTrackingContext, WriteMode
from spline_agent.datasources import DataSource
from spline_agent.dispatcher import LineageDispatcher
from spline_agent.exceptions import LineageContextIncompleteError
from spline_agent.harvester import harvest_lineage
from spline_agent.lineage_model import NameAndVersion, DurationNs

DsParamExpr = Union[str, DataSource]


def track_lineage(
        dispatcher: LineageDispatcher,
        name: Optional[str] = None,
        inputs: tuple[DsParamExpr, ...] = (),
        output: Optional[DsParamExpr] = None,
        write_mode: Optional[WriteMode] = None,
        system_info: Optional[NameAndVersion] = None,
):
    # check if the decorator is used correctly
    if callable(name):
        # it happens when the user forgets parenthesis when using a parametrized decorator,
        # so the decorated function unintentionally becomes the value for the 1st positional parameter
        # that in this case happens to be `name`.
        decor_name = inspect.currentframe().f_code.co_name
        raise TypeError(f'@{decor_name}() decorator should be used with parentheses, even if no arguments are provided')

    def decorator(func):
        # inspect the 'func' signature and collect the parameter names
        sig: inspect.Signature = inspect.signature(func)
        params: Mapping[str, inspect.Parameter] = sig.parameters

        @wraps(func)
        def wrapper(*args, **kwargs):
            # create a combined dictionary of all arguments passed to target function
            bindings = {**{key: arg for key, arg in zip(params, args)}, **kwargs}

            # create and pre-populate a new harvesting context
            ctx = LineageTrackingContext()

            # ... app name
            ctx.name = _eval_str_expr(name, bindings) if name else func.__name__

            # ... inputs
            for inp in inputs:
                ds = _eval_ds_expr(inp, bindings)
                ctx.add_input(ds)

            # ... output
            if output is not None:
                ds = _eval_ds_expr(output, bindings)
                ctx.output = ds

            # ... other params
            ctx.write_mode = write_mode
            ctx.system_info = system_info

            duration_ns: Optional[DurationNs] = None
            error: Optional[Any] = None

            def execute_func():
                nonlocal duration_ns
                nonlocal error
                start_time: DurationNs = time.time_ns()
                try:
                    return func(*args, **kwargs)
                except Exception as ex:
                    error = ex.__str__()
                finally:
                    end_time: DurationNs = time.time_ns()
                    duration_ns = end_time - start_time

            # call target function within the given harvesting context
            func_res = with_context_do(ctx, execute_func)

            try:
                # obtain lineage model
                lineage = harvest_lineage(ctx, func, duration_ns, error)

                # dispatch captured lineage
                dispatcher.send_plan(lineage.plan)
                dispatcher.send_event(lineage.event)

            except LineageContextIncompleteError as e:
                logging.warning(f'Lineage skipped: {e.__str__()}')

            return func_res

        return wrapper

    return decorator


def _eval_ds_expr(expr: DsParamExpr, mapping=None) -> DataSource:
    if mapping is None:
        mapping = {}

    if isinstance(expr, DataSource):
        return expr

    if isinstance(expr, str):
        if expr.startswith('{') and expr.endswith('}') and (expr[1:-1]).isidentifier():
            key = expr[1:-1]
            val = mapping[key]
            return _eval_ds_expr(val)
        if urlparse(expr):
            return DataSource(expr)

    raise ValueError(f'{expr} should be a DataSource, URL string, or a parameter binding expression like {{name}}')


def _eval_str_expr(expr: str, mapping=None) -> str:
    if mapping is None:
        mapping = {}

    if expr.startswith('{') and expr.endswith('}') and (expr[1:-1]).isidentifier():
        key = expr[1:-1]
        val = mapping[key]
        return val

    return expr
