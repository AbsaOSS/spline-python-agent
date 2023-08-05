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
from typing import Optional, Any, cast, Callable

from spline_agent.commons.configuration import Configuration, CompositeConfiguration
from spline_agent.commons.configuration.env_configuration import EnvConfiguration
from spline_agent.commons.configuration.file_configuration import FileConfiguration
from spline_agent.commons.proxy import ObservingProxy
from spline_agent.constants import CONFIG_FILE_DEFAULT, CONFIG_FILE_USER, DEFAULT_SYSTEM_INFO
from spline_agent.context import with_context_do, LineageTrackingContext
from spline_agent.decorators.spel_evaluator import SpELEvaluator
from spline_agent.dispatcher import LineageDispatcher
from spline_agent.enums import SplineMode
from spline_agent.harvester import harvest_lineage
from spline_agent.lineage_model import NameAndVersion, DurationNs
from spline_agent.object_factory import ObjectFactory

logger = logging.getLogger(__name__)


def track_lineage(
        mode: Optional[SplineMode] = None,
        name: Optional[str] = None,
        system_info: Optional[NameAndVersion] = None,
        dispatcher: Optional[LineageDispatcher] = None,
        config: Optional[Configuration] = None,
):
    # check if the decorator is used correctly
    first_arg = locals()[next(iter(inspect.signature(track_lineage).parameters.keys()))]
    if callable(first_arg):
        # it happens when the user forgets parenthesis when using a parametrized decorator,
        # so the decorated function unintentionally becomes the value for the 1st positional parameter.
        raise TypeError(
            f'@{track_lineage.__name__}() decorator should be used with parentheses, even if no arguments are provided')

    # configure
    logger.debug(f'CONFIG_FILE_DEFAULT : {CONFIG_FILE_DEFAULT}')
    logger.debug(f'CONFIG_FILE_USER    : {CONFIG_FILE_USER}')
    default_config = FileConfiguration(CONFIG_FILE_DEFAULT)
    user_config = config if config is not None else FileConfiguration(CONFIG_FILE_USER)
    env_config = EnvConfiguration(prefix='spline')
    config = CompositeConfiguration(env_config, user_config, default_config)

    # determine mode
    mode = mode if mode is not None else SplineMode[config['spline.mode']]

    # proceed according to the mode
    if mode is SplineMode.ENABLED:
        logging.info('Lineage tracking is ENABLED')
        # obtain dispatcher from config if not provided
        factory = ObjectFactory(config)
        disp = dispatcher if dispatcher is not None else factory.instantiate(LineageDispatcher)
        si = system_info if system_info is not None else DEFAULT_SYSTEM_INFO
        return lambda func: _active_decorator(func, name, si, disp)

    elif mode is SplineMode.BYPASS:
        logging.info('Lineage tracking is in BYPASS mode -- not captured')
        return _bypass_decorator

    elif mode is SplineMode.DISABLED:
        logging.info('Lineage tracking is DISABLED')
        return lambda _: _

    else:
        mode_name = mode.name.rpartition('.')[-1]
        raise ValueError(f"Unknown Spline mode '{mode_name}'")


def _active_decorator(
        func: Callable,
        name: Optional[str],
        system_info: NameAndVersion,
        dispatcher: LineageDispatcher,
):
    @wraps(func)
    def active_wrapper(*args, **kwargs):
        spel_evaluator = SpELEvaluator(func, args, kwargs)

        # create and pre-populate a new harvesting context
        ctx = LineageTrackingContext()
        app_name = spel_evaluator.eval(name) if name else name
        ctx.name = app_name if app_name else func.__name__
        ctx.system_info = system_info

        # prepare execution stage
        error: Optional[Any] = None
        start_time: DurationNs = time.time_ns()

        # call target function within the given tracking context
        try:
            return with_context_do(ctx, lambda: func(*args, **kwargs))
        except Exception as ex:
            error = ex
            raise
        finally:
            end_time: DurationNs = time.time_ns()
            duration_ns = end_time - start_time

            # obtain lineage model
            lineage = harvest_lineage(ctx, func, duration_ns, error.__str__() if error is not None else None)

            # dispatch captured lineage
            dispatcher.send_plan(lineage.plan)
            dispatcher.send_event(lineage.event)

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
