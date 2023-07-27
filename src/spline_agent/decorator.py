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
import os
import time
from functools import wraps
from typing import Optional, Union, Mapping, Any, cast, Callable
from urllib.parse import urlparse

from dynaconf import Dynaconf

from spline_agent.commons.configuration import Configuration, DynaconfConfiguration
from spline_agent.commons.proxy import ObservingProxy
from spline_agent.context import with_context_do, LineageTrackingContext, WriteMode
from spline_agent.datasources import DataSource
from spline_agent.dispatcher import LineageDispatcher
from spline_agent.enums import SplineMode
from spline_agent.exceptions import LineageTrackingContextIncompleteError
from spline_agent.harvester import harvest_lineage
from spline_agent.lineage_model import NameAndVersion, DurationNs
from spline_agent.object_factory import ObjectFactory

logger = logging.getLogger(__name__)

DsParamExpr = Union[str, DataSource]


def track_lineage(
        mode: Optional[SplineMode] = None,
        name: Optional[str] = None,
        inputs: tuple[DsParamExpr, ...] = (),
        output: Optional[DsParamExpr] = None,
        write_mode: Optional[WriteMode] = None,
        system_info: Optional[NameAndVersion] = None,
        dispatcher: Optional[LineageDispatcher] = None,
        config: Configuration = DynaconfConfiguration(Dynaconf(settings_files=[
            f'{os.path.dirname(__file__)}/spline.default.yaml',
            f'{os.getcwd()}/spline.yaml',
        ])),
):
    # check if the decorator is used correctly
    first_arg = locals()[next(iter(inspect.signature(track_lineage).parameters.keys()))]
    if callable(first_arg):
        # it happens when the user forgets parenthesis when using a parametrized decorator,
        # so the decorated function unintentionally becomes the value for the 1st positional parameter.
        raise TypeError(
            f'@{track_lineage.__name__}() decorator should be used with parentheses, even if no arguments are provided')

    # determine mode
    mode = mode if mode is not None else SplineMode[config['spline.mode']]

    # proceed according to the mode
    if mode is SplineMode.ENABLED:
        logging.info('Lineage tracking is ENABLED')
        # obtain dispatcher from config if not provided
        factory = ObjectFactory(config)
        dispatcher = dispatcher if dispatcher is not None else factory.instantiate(LineageDispatcher)
        return lambda func: _active_decorator(func, name, inputs, output, write_mode, system_info, dispatcher)

    elif mode is SplineMode.BYPASS:
        logging.info('Lineage tracking is in BYPASS mode -- not captured')
        return _bypass_decorator

    elif mode is SplineMode.DISABLED:
        logging.info('Lineage tracking is DISABLED')
        return lambda _: _

    else:
        raise ValueError(f"Unknown Spline mode '{mode.name.rpartition('.')[-1]}'")


def _active_decorator(
        func: Callable,
        name: Optional[str] = None,
        inputs: tuple[DsParamExpr, ...] = (),
        output: Optional[DsParamExpr] = None,
        write_mode: Optional[WriteMode] = None,
        system_info: Optional[NameAndVersion] = None,
        dispatcher: Optional[LineageDispatcher] = None,
):
    # inspect the 'func' signature and collect the parameter names
    sig: inspect.Signature = inspect.signature(func)
    params: Mapping[str, inspect.Parameter] = sig.parameters

    @wraps(func)
    def active_wrapper(*args, **kwargs):
        # create and pre-populate a new harvesting context
        ctx = LineageTrackingContext()

        # create a combined dictionary of all arguments passed to target function
        bindings = {**{key: arg for key, arg in zip(params, args)}, **kwargs}

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

        # prepare execution stage
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

        # call target function within the given tracking context
        func_res = with_context_do(ctx, execute_func)

        # obtain lineage model
        lineage = harvest_lineage(ctx, func, duration_ns, error)

        # todo: to be validated on the config level (issue #8)
        if dispatcher is None:
            raise LineageTrackingContextIncompleteError('dispatcher')

        # dispatch captured lineage
        dispatcher.send_plan(lineage.plan)
        dispatcher.send_event(lineage.event)

        # return the original target function result
        return func_res

    return active_wrapper


def _bypass_decorator(func):
    @wraps(func)
    def bypass_wrapper(*args, **kwargs):
        # prepare an isolated context whose only role is to emulate Spline Agent API for the client code.
        ctx_obj = LineageTrackingContext()
        ctx_proxy = ObservingProxy(ctx_obj, lambda _, _member_name, _member_type, _args, _kwargs: logger.warning(
            f"The {_member_type.name.rpartition('.')[-1].lower()} '{_member_name}' was called "
            f"on a disabled lineage tracking context: args: {_args}, kwargs: {_kwargs}"))
        isolated_ctx = cast(LineageTrackingContext, ctx_proxy)

        # call a target function within an isolated context, and return the result
        return with_context_do(isolated_ctx, lambda: func(*args, **kwargs))

    return bypass_wrapper


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
