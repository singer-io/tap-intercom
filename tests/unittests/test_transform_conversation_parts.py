import nose
import unittest
import json
import singer
from tap_intercom.transform import transform_conversation_parts
from tap_intercom.tests.conversation_parts_data import RAW_CONVERSATION, EXPECTED_CONVERSATION_PARTS

LOGGER = singer.get_logger()
LOGGER.info('test')

def test_transform_conversation_parts():

    data_key = 'conversations'
    conversations_dict = {}
    conversations_list = []
    conversations_list.append(RAW_CONVERSATION)
    conversations_dict[data_key] = conversations_list

    transformed_conv_parts = transform_conversation_parts(conversations_dict, data_key)
    transformed_conv_parts_len = len(transformed_conv_parts)
    LOGGER.info('transformed_conv_parts_len = {}'.format(transformed_conv_parts_len))
    transformed_conv_parts_json = json.dumps(transformed_conv_parts, indent=2, sort_keys=True)

    expected_conv_parts = EXPECTED_CONVERSATION_PARTS
    expected_conv_parts_len = len(expected_conv_parts)
    LOGGER.info('expected_conv_parts_len = {}'.format(expected_conv_parts_len))
    expected_conv_parts_json = json.dumps(expected_conv_parts, indent=2, sort_keys=True)

    assert transformed_conv_parts_len == expected_conv_parts_len, "conversation_parts len != expected_conversation_parts len"
    assert transformed_conv_parts_json == expected_conv_parts_json, "conversation_parts_json != expected_conversation_parts_json"


if __name__ == '__main__':   
    nose.run()
