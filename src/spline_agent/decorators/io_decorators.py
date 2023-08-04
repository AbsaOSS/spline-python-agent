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

from __future__ import annotations

import logging
from functools import wraps
from typing import Callable

from .model import DsParamExpr, DataSource
from ..context import get_tracking_context, LineageTrackingContext
from ..decorators.spel_evaluator import SpELEvaluator
from ..enums import WriteMode

logger = logging.getLogger(__name__)


def inputs(*exprs: DsParamExpr):
    def handler(ctx: LineageTrackingContext, ds: DataSource):
        ctx.add_input(ds)

    return lambda func: _decor(func, handler, *exprs)


def output(expr: DsParamExpr, write_mode: WriteMode):
    def handler(ctx: LineageTrackingContext, ds: DataSource):
        ctx.output = ds
        ctx.write_mode = write_mode

    return lambda func: _decor(func, handler, expr)


def _decor(
        func: Callable,
        handler: Callable[[LineageTrackingContext, DataSource], None],
        *ds_exprs: DsParamExpr
):
    @wraps(func)
    def wrapper(*args, **kwargs):
        ctx = get_tracking_context()
        spel_evaluator = SpELEvaluator(func, args, kwargs)
        for expr in ds_exprs:
            ds = spel_evaluator.eval_as_data_source(expr)
            handler(ctx, ds)

        return func(*args, **kwargs)

    return wrapper
