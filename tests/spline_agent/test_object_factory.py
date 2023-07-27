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

from spline_agent.commons.configuration import DictConfiguration
from spline_agent.object_factory import ObjectFactory


class FruitOrBerry(ABC):
    @abstractmethod
    def color(self) -> str:
        pass


class Banana(FruitOrBerry):
    def color(self) -> str:
        return "yellow"


def test_create_object():
    # prepare
    test_conf = DictConfiguration({
        'spline.fruit_or_berry.type': 'banana',
        'spline.fruit_or_berry.banana.class_name': f'{Banana.__module__}.{Banana.__name__}'
    })

    factory = ObjectFactory(test_conf)

    # execute
    banana: FruitOrBerry = factory.instantiate(FruitOrBerry)

    # verify
    assert banana.__class__ == Banana
    assert banana.color() == 'yellow'
