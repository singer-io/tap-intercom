import unittest
from tap_intercom.streams import BaseStream

class TestEpochToDatetimeTransform(unittest.TestCase):

    def test_epoch_milliseconds_to_dt_str(self):
        """
            Verify one epoch time with expected UTC datetime 
        """
        test_epoch = 1640760200000
        expected_utc_datetime = "2021-12-29T06:43:20.000000Z" # expected UTC for `1640760200000`

        datetime_str = BaseStream.epoch_milliseconds_to_dt_str(test_epoch)

        # Verify that test epoch time is converted to valid UTC datetime
        self.assertEquals(datetime_str, expected_utc_datetime)
