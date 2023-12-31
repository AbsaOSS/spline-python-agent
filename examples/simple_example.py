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

import logging
import os

import spline_agent
from spline_agent.enums import WriteMode
from spline_agent.lineage_model import NameAndVersion

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.getLevelName(os.environ.get('LOG_LEVEL') or 'INFO'))


@spline_agent.track_lineage(
    name='My awesome python app',
    system_info=NameAndVersion(name='name of my data processing engine', version='0.0.0'),
)
def main(data_url_1: str, data_url_2: str, result_url: str):
    """
    This function pretends that it reads data from imaginary locations represented by URLs
    and writes the result into another imaginary location.

    Returns:
        _type_: _description_
    """

    data_1 = _dummy_read(data_url_1)
    data_2 = _dummy_read(data_url_2)

    result_data = _do_some_magic(data_1, data_2)

    _dummy_write(result_url, result_data)

    logger.info('DONE')


@spline_agent.inputs('{url}')
def _dummy_read(url: str):
    logger.info(f'reading from {url} ... OK')
    return "dummy data"


@spline_agent.output('{url}', WriteMode.OVERWRITE)
def _dummy_write(url: str, data):
    logger.info(f'writing [{data}] to {url} ... OK')
    pass


def _do_some_magic(a, b):
    return a + b


if __name__ == "__main__":
    main(
        data_url_1='s3://my-bucket-name/data/input.csv',
        data_url_2='hdfs://my-cluster/data/files/datafile.txt',
        result_url='jdbc:database://data-server.example.com:5432/mydatabase?table=mytable',
    )
