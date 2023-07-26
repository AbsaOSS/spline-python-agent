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

from spline_agent.commons.utils import camel_to_snake


def test_camel_to_snake():
    assert camel_to_snake('A') == 'a'
    assert camel_to_snake('A1') == 'a1'
    assert camel_to_snake('A1B') == 'a1_b'
    assert camel_to_snake('A1b') == 'a1b'
    assert camel_to_snake('A1BC') == 'a1_bc'
    assert camel_to_snake('A1Bc') == 'a1_bc'
    assert camel_to_snake('A1bC') == 'a1b_c'
    assert camel_to_snake('AB1') == 'ab1'
    assert camel_to_snake('Ab1') == 'ab1'
    assert camel_to_snake('MySuperDuperA1testString42One2THREE') == 'my_super_duper_a1test_string42_one2_three'
