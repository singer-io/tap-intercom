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