import unittest
from tap_intercom.transform import transform_times

class TestTransformMultipleTimes(unittest.TestCase):
    def test_transfrom_multiple_dates(self):
        """
            Test case to verify we are not getting KeyError if we cannot find date-time value for list of value
        """
        time = 1659346509
        record = {
            'id': 1,
            'company': [{'id': 1, 'created_at': time}, {'id': 2}] # 1st record contains 'created_at'
        }
        transform_times(
            record,
            [['company', ['created_at']]]
        )
        self.assertEqual(record.get('company')[0].get('created_at'), time * 1000)
        self.assertIsNone(record.get('company')[1].get('created_at'))
