import spline_agent


@spline_agent.track_lineage(
    name='My awesome python app',
    inputs=('{data_url_1}', '{data_url_2}'),
    output='{result_url}',
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

    print('DONE')


def _dummy_read(url: str):
    print(f'reading from {url} ... OK')
    return "dummy data"


def _dummy_write(url: str, data):
    print(f'writing to {url} ... OK')
    pass


def _do_some_magic(a, b):
    return a + b


if __name__ == "__main__":
    main(
        data_url_1='s3://my-bucket-name/data/input.csv',
        data_url_2='hdfs://my-cluster/data/files/datafile.txt',
        result_url='jdbc:database://data-server.example.com:5432/mydatabase?table=mytable',
    )
