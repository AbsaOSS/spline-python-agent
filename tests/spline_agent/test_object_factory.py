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

from abc import ABC, abstractmethod
from enum import Enum
from typing import cast

from spline_agent.commons.configuration import DictConfiguration
from spline_agent.object_factory import ObjectFactory


class FruitOrBerry(ABC):
    @property
    @abstractmethod
    def color(self) -> str:
        pass


class Banana(FruitOrBerry):
    @property
    def color(self) -> str:
        return "yellow"


class MelonKind(Enum):
    KORDOFAN = 1
    DOMESTICATED = 2


class Watermelon(FruitOrBerry):
    def __init__(self, color: str, weight: int, kind: MelonKind):
        self.__color = color
        self.weight = weight
        self.kind = kind

    @property
    def color(self) -> str:
        return self.__color


def test_create_object__default_constructor():
    # prepare
    test_conf = DictConfiguration({
        'spline.fruit_or_berry.type': 'banana',
        'spline.fruit_or_berry.banana.class_name': f'{Banana.__module__}.{Banana.__name__}',
    })

    factory = ObjectFactory(test_conf)

    # execute
    obj: FruitOrBerry = factory.instantiate(FruitOrBerry)

    # verify
    assert obj.__class__ == Banana
    banana = cast(Banana, obj)

    assert banana.color == 'yellow'


def test_create_object__constructor_with_params():
    # prepare
    test_conf = DictConfiguration({
        'spline.fruit_or_berry.type': 'watermelon',
        'spline.fruit_or_berry.watermelon.class_name': f'{Watermelon.__module__}.{Watermelon.__name__}',
        'spline.fruit_or_berry.watermelon.color': 'whitish',
        'spline.fruit_or_berry.watermelon.weight': 5,
        'spline.fruit_or_berry.watermelon.kind': 'KORDOFAN',
    })

    factory = ObjectFactory(test_conf)

    # execute
    obj: FruitOrBerry = factory.instantiate(FruitOrBerry)

    # verify
    assert obj.__class__ == Watermelon
    watermelon = cast(Watermelon, obj)

    assert watermelon.color == 'whitish'
    assert watermelon.kind == MelonKind.KORDOFAN
    assert watermelon.weight == 5
