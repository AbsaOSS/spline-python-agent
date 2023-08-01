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

import importlib
import inspect
import logging
from enum import Enum
from typing import Type, TypeVar, cast, Any

from spline_agent.commons.configuration import Configuration
from spline_agent.commons.utils import camel_to_snake

logger = logging.getLogger(__name__)

T = TypeVar('T')


class ObjectFactory:
    def __init__(self, config: Configuration):
        self.__config = config

    def instantiate(self, typ: Type[T]) -> T:
        # the method shouldn't be called without `typ`. The Optional here is only needed to satisfy mypy.
        assert typ is not None
        logger.debug(f'instantiating {typ}')

        # find the implementation
        object_kind = camel_to_snake(typ.__name__)
        conf_prefix: str = f'spline.{object_kind}'
        object_name: str = self.__config[f'{conf_prefix}.type']
        full_classname: str = self.__config[f'{conf_prefix}.{object_name}.class_name']
        logger.debug(f'found configured implementation: {full_classname}')

        # load class
        [module_name, _, short_classname] = full_classname.rpartition('.')
        module = importlib.import_module(module_name)
        class_ = getattr(module, short_classname)
        logger.debug(f'class loaded: {class_}')

        # resolve constructor arguments
        kwargs: dict[str, Any] = {}
        constr_sig = inspect.signature(class_)
        for param_name, param_def in constr_sig.parameters.items():
            if param_def.default == inspect.Parameter.empty:
                conf_value = self.__config[f'{conf_prefix}.{object_name}.{param_name}']
                type_annotation = param_def.annotation
                if isinstance(type_annotation, type) and issubclass(type_annotation, Enum):
                    kwargs[param_name] = param_def.annotation[conf_value]
                else:
                    kwargs[param_name] = conf_value

        # instantiate the class
        instance = class_(**kwargs)
        return cast(T, instance)
