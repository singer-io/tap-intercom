import nose
from singer.transform import unix_milliseconds_to_datetime
from tap_intercom.transform import get_integer_places


def test_get_integer_places():

    input_output_mapping = {
        -62135596800: 10,
        62135596800: 11,
        1612998233: 10,
        -1: 10,
        -9999999999999: 10,
        -999999999999997: 10,
        1612998430000: 13,
        9999999999999999: 16,
    }

    for input_values, expected_values in input_output_mapping.items():
        assert expected_values == get_integer_places(input_values)


def test_unix_milliseconds_to_datetime():

    input_output_mapping = {
        -62135596800: "1968-01-12T20:06:43.200000Z",
        62135596800: "1971-12-21T03:53:16.800000Z",
        99999999999999: "5138-11-16T09:46:39.998993Z",
        -1: "1969-12-31T23:59:59.999000Z",
    }
    for input_values, expected_values in input_output_mapping.items():
        assert expected_values == unix_milliseconds_to_datetime(input_values)


if __name__ == '__main__':
    nose.run()
