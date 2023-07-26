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
from dynaconf import Dynaconf

from spline_agent.commons.configuration.composite_configuration import CompositeConfiguration
from spline_agent.commons.configuration.configuration import Configuration
from spline_agent.commons.configuration.dict_configuration import DictConfiguration
from spline_agent.commons.configuration.dynaconf_configuration import DynaconfConfiguration
from spline_agent.exceptions import ConfigurationError
from ...mocks import ConfigurationMock


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
    assert conf.keys() == {'a', 'b', 'c', 'd'}


def test_dynaconf_configuration():
    # prepare
    test_settings = Dynaconf(settings_files=[f'{os.path.dirname(__file__)}/test_configuration_dynaconf.settings.toml'])

    # execute
    conf = DynaconfConfiguration(test_settings)

    # verify
    _assert_item(conf, 'a', 1)
    _assert_item(conf, 'b', True)
    _assert_item(conf, 'c', 'Hello')
    _assert_item(conf, 'd', [10, 20])
    _assert_item(conf, 'e', {'foo': 'bar'})
    _assert_no_item(conf, 'nah')
    assert {'a', 'b', 'c', 'd', 'e'}.issubset(conf.keys())


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
    conf_1_mock.keys.return_value = test_dict_1.keys()
    conf_1_mock.__contains__.side_effect = test_dict_1.__contains__
    conf_1_mock.__getitem__.side_effect = test_dict_1.get
    conf_1_mock.get.side_effect = test_dict_1.get

    conf_2_mock: ConfigurationMock = create_autospec(Configuration)
    conf_2_mock.keys.return_value = test_dict_2.keys()
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
    assert conf.keys() == {'a', 'b', 'c', 'd'}
