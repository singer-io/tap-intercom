import unittest
from tap_intercom.sync import translate_state

class TestTranslateState(unittest.TestCase):

    def test_state_translation_for_old_format(self):
        '''
            Verify that state is translated to new format if old formatted state is provided
        '''

        state = {
            "bookmarks": {
                "companies": "2021-12-22T07:23:47.000000Z",
                "company_segments": "2021-12-20T21:30:35.000000Z",
                "conversations": "2021-12-22T08:01:05.000000Z",
                "contacts": "2021-12-22T08:07:57.000000Z",
                "segments": "2021-11-01T00:00:00Z"
            }
        }

        expected_state = {
            "bookmarks": {
                "companies": {
                    "updated_at": "2021-12-22T07:23:47.000000Z"
                },
                "company_segments": {
                    "updated_at": "2021-12-20T21:30:35.000000Z"
                },
                "conversations": {
                    "updated_at": "2021-12-22T08:01:05.000000Z"
                },
                "contacts": {
                    "updated_at": "2021-12-22T08:07:57.000000Z"
                },
                "segments": {
                    "updated_at": "2021-11-01T00:00:00Z"
                }
            }
        }

        new_state = translate_state(state)

        # Verify that returned state is as expected with new format
        self.assertEqual(new_state, expected_state)


    def test_state_translate_for_new_format(self):
        '''
            Verify that state remain same if new formatted state is provided
        '''

        new_format_state = {
            "bookmarks": {
                "companies": {
                    "updated_at": "2021-12-22T07:23:47.000000Z"
                },
                "company_segments": {
                    "updated_at": "2021-12-20T21:30:35.000000Z"
                },
                "conversations": {
                    "updated_at": "2021-12-22T08:01:05.000000Z"
                },
                "contacts": {
                    "updated_at": "2021-12-22T08:07:57.000000Z"
                },
                "segments": {
                    "updated_at": "2021-11-01T00:00:00Z"
                }
            }
        }

        new_state = translate_state(new_format_state)

        # Verify that returned state is same for new formatted state
        self.assertEqual(new_state, new_format_state)


class TestGetStreamsToSync(unittest.TestCase):

    def test_admins_stream_included_directly(self):
        """When 'admins' is selected, get_streams_to_sync adds catalog admins stream."""
        from tap_intercom.sync import get_streams_to_sync
        from tap_intercom.streams import Admins

        class _FakeStream:
            def __init__(self, tap_stream_id):
                self.tap_stream_id = tap_stream_id

        class _FakeCatalog:
            def get_stream(self, name):
                return _FakeStream(name)

        admins_obj = Admins.__new__(Admins)
        admins_obj.tap_stream_id = 'admins'

        result = get_streams_to_sync(
            _FakeCatalog(), [admins_obj], ['admins']
        )
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].tap_stream_id, 'admins')
