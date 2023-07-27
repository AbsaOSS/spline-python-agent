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

import os
from typing import Any
from unittest.mock import create_autospec

import pytest

from spline_agent.commons.configuration.composite_configuration import CompositeConfiguration
from spline_agent.commons.configuration.configuration import Configuration
from spline_agent.commons.configuration.dict_configuration import DictConfiguration
from spline_agent.commons.configuration.env_configuration import EnvConfiguration
from spline_agent.commons.configuration.file_configuration import FileConfiguration
from spline_agent.exceptions import ConfigurationError
from ...mocks import ConfigurationMock

_PATH: str = os.path.dirname(__file__)


def _assert_item(conf: Configuration, key: str, expected: Any):
    assert key in conf
    assert conf.get(key) == expected
    assert conf[key] == expected


def _assert_no_item(conf: Configuration, key: str):
    assert key not in conf
    assert conf.get(key) is None
    with pytest.raises(ConfigurationError, match=fr': {key}'):
        _ = conf[key]


def test_dict_configuration():
    # prepare
    test_dict = {
        'a': 1,
        'b': True,
        'c': 'Hello',
        'd': [10, 20]
    }

    # execute
    conf = DictConfiguration(test_dict)

    # verify
    _assert_item(conf, 'a', 1)
    _assert_item(conf, 'b', True)
    _assert_item(conf, 'c', 'Hello')
    _assert_item(conf, 'd', [10, 20])
    _assert_no_item(conf, 'nah')


def test_dynaconf_configuration__flat_structure():
    # prepare
    conf = FileConfiguration(f'{_PATH}/test_configuration_settings.flat.yaml')

    # execute and verify
    _assert_item(conf, 'x.y', 1)
    _assert_item(conf, 'a.b', True)
    _assert_item(conf, 'm.n.o', 'Hello')
    _assert_item(conf, 'p.q.r', [10, 20])

    _assert_item(conf, 'X.Y', 1)
    _assert_item(conf, 'A.B', True)
    _assert_item(conf, 'M.N.O', 'Hello')
    _assert_item(conf, 'P.Q.R', [10, 20])

    _assert_no_item(conf, 'nah')


def test_file_configuration__nested_structure():
    # prepare
    conf = FileConfiguration(f'{_PATH}/test_configuration_settings.nested.yaml')

    # execute and verify
    _assert_item(conf, 'a.b', 1)
    _assert_item(conf, 'c.d', True)
    _assert_item(conf, 'e.f.g', 'Hello')
    _assert_item(conf, 'h.i.j', [10, 20])

    _assert_item(conf, 'A.B', 1)
    _assert_item(conf, 'C.D', True)
    _assert_item(conf, 'E.F.G', 'Hello')
    _assert_item(conf, 'H.I.J', [10, 20])

    _assert_no_item(conf, 'nah')


def test_file_configuration__mixed_structure():
    # prepare
    conf = FileConfiguration(f'{_PATH}/test_configuration_settings.mixed_struct.yaml')

    # execute and verify
    _assert_item(conf, 'a.b.c', 1)
    _assert_item(conf, 'd.e.f', True)
    _assert_item(conf, 'g', {'h.i': 'Hello'})
    _assert_item(conf, 'j.k', {'l.m': [10, 20]})

    _assert_item(conf, 'A.B.C', 1)
    _assert_item(conf, 'D.E.F', True)
    _assert_item(conf, 'G', {'h.i': 'Hello'})
    _assert_item(conf, 'J.K', {'l.m': [10, 20]})

    _assert_no_item(conf, 'nah')


def test_file_configuration__mixed_cases():
    # prepare
    conf = FileConfiguration(f'{_PATH}/test_configuration_settings.mixed_cases.yaml')

    # execute and verify
    _assert_item(conf, 'a.b.c', 1)
    _assert_item(conf, 'd.e.f', True)
    _assert_item(conf, 'g', {'h.i': 'Hello'})
    _assert_item(conf, 'j.k', {'l.M': [10, 20]})
    _assert_item(conf, 'j', {'K': {'l.M': [10, 20]}})

    _assert_item(conf, 'A.B.C', 1)
    _assert_item(conf, 'D.E.F', True)
    _assert_item(conf, 'G', {'h.i': 'Hello'})
    _assert_item(conf, 'J.K', {'l.M': [10, 20]})
    _assert_item(conf, 'J', {'K': {'l.M': [10, 20]}})

    _assert_no_item(conf, 'nah')


def test_composite_configuration():
    # prepare
    test_dict_1 = {
        'a': 1,
        'c': 1,
    }

    test_dict_2 = {
        'b': 2,
        'c': 2,
        'd': [2, 22]
    }

    conf_1_mock: ConfigurationMock = create_autospec(Configuration)
    conf_1_mock.__contains__.side_effect = test_dict_1.__contains__
    conf_1_mock.__getitem__.side_effect = test_dict_1.get
    conf_1_mock.get.side_effect = test_dict_1.get

    conf_2_mock: ConfigurationMock = create_autospec(Configuration)
    conf_2_mock.__contains__.side_effect = test_dict_2.__contains__
    conf_2_mock.__getitem__.side_effect = test_dict_2.get
    conf_2_mock.get.side_effect = test_dict_2.get

    # execute
    conf = CompositeConfiguration(conf_1_mock, conf_2_mock)

    # verify
    _assert_item(conf, 'a', 1)
    _assert_item(conf, 'b', 2)
    _assert_item(conf, 'c', 1)
    _assert_item(conf, 'd', [2, 22])
    _assert_no_item(conf, 'nah')


def test_env_configuration(set_env_vars):
    # prepare
    set_env_vars({
        'TEST_FOO_ENV': 42,
        'TEST_BAR_ENV': 'forty-two',
        'SOME_OTHER_ENV': 'true',
    })

    # execute
    conf_all = EnvConfiguration()
    conf_test = EnvConfiguration(prefix='test')
    conf_test_foo = EnvConfiguration(prefix='test.foo')

    # verify
    _assert_item(conf_all, 'test.foo.env', 42)
    _assert_item(conf_all, 'test.bar.env', 'forty-two')
    _assert_item(conf_all, 'some.other.env', True)

    _assert_item(conf_test, 'test.foo.env', 42)
    _assert_item(conf_test, 'test.bar.env', 'forty-two')
    _assert_no_item(conf_test, 'some.other.env')

    _assert_item(conf_test_foo, 'test.foo.env', 42)
    _assert_no_item(conf_test_foo, 'test.bar.env')
    _assert_no_item(conf_test_foo, 'some.other.env')
