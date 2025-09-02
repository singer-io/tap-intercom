import singer
from tap_intercom.transform import denest_list_nodes
# from tap_intercom.tests.denest_list_nodes_data import *

LOGGER = singer.get_logger()
LOGGER.info("running test_denest_list_nodes unit test")

RAW_COMPANIES = {
    "companies": [{
        "app_id": "abcdefg",
        "company_id": "123456",
        "created_at": 1497519500,
        "custom_attributes": {
        "Number_of_Integrations": 0,
        "hasDataWarehouse": False,
        "product": "pipeline"
        },
        "id": "9876543210",
        "last_request_at": 1497519500,
        "monthly_spend": 100,
        "name": "Test Company",
        "plan": {},
        "segments": {
        "segments": ['Segment1', 'Segment2', 'Segment3'],
        "type": "segment.list"
        },
        "session_count": 1,
        "tags": {
        "tags": ['Tag1', 'Tag2'],
        "type": "tag.list"
        },
        "type": "company",
        "updated_at": 1500174433,
        "user_count": 1
    }],
    "pages": {
        "next": None,
        "page": 1,
        "per_page": 15,
        "total_pages": 1,
        "type": "pages"
    },
    "total_count": 1,
    "type": "company.list"
}

# Expected Result, tags and segments de-nested
EXPECTED_COMPANIES = {
    "companies": [{
        "app_id": "abcdefg",
        "company_id": "123456",
        "created_at": 1497519500,
        "custom_attributes": {
        "Number_of_Integrations": 0,
        "hasDataWarehouse": False,
        "product": "pipeline"
        },
        "id": "9876543210",
        "last_request_at": 1497519500,
        "monthly_spend": 100,
        "name": "Test Company",
        "plan": {},
        "segments": ['Segment1', 'Segment2', 'Segment3'],
        "session_count": 1,
        "tags": ['Tag1', 'Tag2'],
        "type": "company",
        "updated_at": 1500174433,
        "user_count": 1
    }],
    "pages": {
        "next": None,
        "page": 1,
        "per_page": 15,
        "total_pages": 1,
        "type": "pages"
    },
    "total_count": 1,
    "type": "company.list"
}


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
