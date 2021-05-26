import nose
import unittest
import json
import singer
from tap_intercom.transform import denest_list_nodes
from tap_intercom.tests.denest_list_nodes_data import *

LOGGER = singer.get_logger()


def test_denest_list_nodes():
    
    list_nodes = ['segments', 'tags']
    data_key = 'companies'
    transformed_companies = denest_list_nodes(RAW_COMPANIES, data_key, list_nodes)

    segments_expected = str(sorted(EXPECTED_COMPANIES.get(data_key, [])[0].get('segments')))
    LOGGER.info('segments_expected = {}'.format(segments_expected))
    
    segments = str(sorted(transformed_companies.get(data_key, [])[0].get('segments')))
    LOGGER.info('segments= {}'.format(segments))

    tags_expected = str(sorted(EXPECTED_COMPANIES.get(data_key, [])[0].get('tags')))
    LOGGER.info('tags_expected = {}'.format(tags_expected))
    
    tags= str(sorted(transformed_companies.get(data_key, [])[0].get('tags')))
    LOGGER.info('tags_ = {}'.format(tags))

    assert segments == segments_expected, "%r != %r" % (segments, segments_expected)

    assert tags == tags_expected, "%r != %r" % (tags, tags_expected)


if __name__ == '__main__':   
    nose.run()
