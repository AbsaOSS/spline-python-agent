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
from unittest.mock import create_autospec

from dynaconf import Dynaconf

from spline_agent.commons.configuration.composite_configuration import CompositeConfiguration
from spline_agent.commons.configuration.configuration import Configuration
from spline_agent.commons.configuration.dict_configuration import DictConfiguration
from spline_agent.commons.configuration.dynaconf_configuration import DynaconfConfiguration
from ...mocks import ConfigurationMock


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
    assert 'a' in conf and conf['a'] == 1
    assert 'b' in conf and conf['b']
    assert 'c' in conf and conf['c'] == 'Hello'
    assert 'd' in conf and conf['d'] == [10, 20]

    assert 'nah' not in conf

    assert conf.keys() == {'a', 'b', 'c', 'd'}


def test_dynaconf_configuration():
    # prepare
    test_settings = Dynaconf(settings_files=[f'{os.path.dirname(__file__)}/test_configuration_dynaconf.settings.toml'])

    # execute
    conf = DynaconfConfiguration(test_settings)

    # verify
    assert 'a' in conf and conf['a'] == 1
    assert 'b' in conf and conf['b']
    assert 'c' in conf and conf['c'] == 'Hello'
    assert 'd' in conf and conf['d'] == [10, 20]
    assert 'e' in conf and conf['e'] == {'foo': 'bar'}

    assert 'nah' not in conf

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

    conf_2_mock: ConfigurationMock = create_autospec(Configuration)
    conf_2_mock.keys.return_value = test_dict_2.keys()
    conf_2_mock.__contains__.side_effect = test_dict_2.__contains__
    conf_2_mock.__getitem__.side_effect = test_dict_2.get

    # execute
    conf = CompositeConfiguration(conf_1_mock, conf_2_mock)

    # verify
    assert 'a' in conf and conf['a'] == 1
    assert 'b' in conf and conf['b']
    assert 'c' in conf and conf['c'] == 1
    assert 'd' in conf and conf['d'] == [2, 22]

    assert 'nah' not in conf

    assert conf.keys() == {'a', 'b', 'c', 'd'}
