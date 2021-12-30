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
                "conversation_parts": "2021-12-22T08:01:05.000000Z",
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
                "conversation_parts": {
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
        self.assertEquals(new_state, expected_state)


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
                "conversation_parts": {
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
        self.assertEquals(new_state, new_format_state)