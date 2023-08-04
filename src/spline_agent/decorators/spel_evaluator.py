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
from typing import Callable, Mapping, Any
from urllib.parse import urlparse

from spline_agent.datasources import DataSource
from spline_agent.decorators.model import SpELExpr, DsParamExpr


class SpELEvaluator:
    def __init__(self, func: Callable, args, kwargs):
        # inspect the 'func' signature and collect the parameter names
        sig: inspect.Signature = inspect.signature(func)
        params: Mapping[str, inspect.Parameter] = sig.parameters

        # create a combined dictionary of all arguments passed to target function
        self.__bindings = {**{key: arg for key, arg in zip(params, args)}, **kwargs}

    def eval(self, expr: SpELExpr) -> Any:
        if type(expr) is not str:
            raise TypeError(f'SpEL expression has to be string, but was {type(expr)}')
        if expr.startswith('{') and expr.endswith('}') and (expr[1:-1]).isidentifier():
            key = expr[1:-1]
            val = self.__bindings[key]
            return self.eval(val) if type(val) is str else val
        return expr

    def eval_as_data_source(self, expr: DsParamExpr) -> DataSource:
        if isinstance(expr, DataSource):
            return expr

        if type(expr) is str:
            val = self.eval(expr)
            if isinstance(val, DataSource):
                return val
            if type(val) is str and urlparse(val):
                return DataSource(val)

        raise ValueError(f'{expr} should be a DataSource, URL string, or a parameter binding expression like {{name}}')
