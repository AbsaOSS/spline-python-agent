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
from functools import wraps
from typing import Optional, Union, Any, Mapping
from urllib.parse import urlparse

from spline_agent.context import get_tracking_context, with_context_do, LineageHarvestingContext
from spline_agent.datasources import DataSource

DsParamExpr = Union[str, DataSource]


def track_lineage(
        name: Optional[str] = None,
        ins: tuple[DsParamExpr, ...] = (),
        out: Optional[DsParamExpr] = None):
    # check if the decorator is used correctly
    if callable(name):
        # it happens when the user forgets parenthesis when using a parametrized decorator,
        # so the decorated function unintentionally becomes the value for the 1st positional parameter
        # that in this case happens to be `name`.
        decor_name = inspect.currentframe().f_code.co_name
        raise TypeError(f"'{decor_name}' decorator should be used with parentheses, even if no arguments are provided")

    def decorator(func):
        # inspect the 'func' signature and collect the parameter names
        sig: inspect.Signature = inspect.signature(func)
        params: Mapping[str, inspect.Parameter] = sig.parameters

        @wraps(func)
        def wrapper(*args, **kwargs):
            # the function is not reentrant
            # there must be no context in the context holder
            assert get_tracking_context() is None

            # create a combined dictionary of all arguments passed to target function
            bindings = {**{name: arg for name, arg in zip(params, args)}, **kwargs}

            # create and prepopulate a new harvesting context
            ctx = LineageHarvestingContext()

            # ... app name
            ctx.name = _eval_str_expr(name, bindings) if name else func.__name__

            # ... inputs
            for inp in ins:
                ds = _eval_ds_expr(inp, bindings)
                ctx.add_input(ds)

            # ... output
            if out is not None:
                ds = _eval_ds_expr(out, bindings)
                ctx.set_output(ds)

            # call target function within the given harvesting context
            return with_context_do(ctx, lambda: func(*args, **kwargs))

        return wrapper

    return decorator


def _eval_ds_expr(expr: DsParamExpr, mapping: Mapping[str, Any] = None) -> DataSource:
    if isinstance(expr, DataSource):
        return expr

    if isinstance(expr, str):
        if expr.startswith("{") and expr.endswith("}") and (expr[1:-1]).isidentifier():
            key = expr[1:-1]
            val = mapping[key]
            return _eval_ds_expr(val)
        if urlparse(expr):
            return DataSource(expr)

    raise ValueError(f"{expr} should be a DataSource, URL string, or a parameter binding expression like {{name}}")


def _eval_str_expr(expr: str, mapping: Mapping[str, Any] = None) -> DataSource:
    if expr.startswith("{") and expr.endswith("}") and (expr[1:-1]).isidentifier():
        key = expr[1:-1]
        val = mapping[key]
        return val
    return expr
