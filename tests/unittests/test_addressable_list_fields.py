import unittest
from unittest import mock
from tap_intercom.client import IntercomClient
from tap_intercom.streams import Contacts

@mock.patch('tap_intercom.client.IntercomClient.get')
class TestAddressableListFields(unittest.TestCase):
    """
        Test cases to verify the behavior of addressable list fields
    """
    def test_addressable_list_field_not_selected(self, mocked_get):
        """
            Test case to verify we do not do API call if the field is not selected
        """
        client = IntercomClient('test_access_token', 100, None)
        contacts = Contacts(client, None, ['contacts'])
        contact_list = {
            'data': [
                {
                    'id': 1,
                    'tags': {
                        'type': 'list',
                        'data': [
                            {
                                "id": "t1",
                                "type": "tag",
                                "url": "/tags/t1"
                            },
                            {
                                "id": "t2",
                                "type": "tag",
                                "url": "/tags/t2"
                            }
                        ],
                        'url': '/contacts/1/tags',
                        'total_count': 2,
                        'has_more': False
                    },
                    'companies': {
                        'type': 'list',
                        'data': [
                            {
                                "id": "c1",
                                "type": "companies",
                                "url": "/companiess/c1"
                            }
                        ],
                        'url': '/contacts/1/companies',
                        'total_count': 2,
                        'has_more': False
                    }
                }
            ]
        }
        metadata = {
            (): {
                'selected': True,
                'table-key-properties': ['id'],
                'forced-replication-method': 'INCREMENTAL',
                'valid-replication-keys': ['updated_at'],
                'inclusion': 'available'
            }, ('properties', 'id'): {
                'inclusion': 'available',
                'selected': True
            }, ('properties', 'tags'): {
                'inclusion': 'available'
            }, ('properties', 'companies'): {
                'inclusion': 'available'
            }
        }
        data = contacts.get_addressable_list(contact_list, metadata)
        self.assertEqual(data, contact_list)
        self.assertEqual(mocked_get.call_count, 0)

    def test_addressable_list_field_0_records(self, mocked_get):
        """
            Test case to verify we do not do API call if the field has 0 record
        """
        client = IntercomClient('test_access_token', 100, None)
        contacts = Contacts(client, None, ['contacts'])
        contact_list = {
            'data': [
                {
                    'id': 1,
                    'tags': {
                        'type': 'list',
                        'data': [],
                        'url': '/contacts/1/tags',
                        'total_count': 0,
                        'has_more': False
                    },
                    'companies': {
                        'type': 'list',
                        'data': [
                            {
                                "id": "c1",
                                "type": "companies",
                                "url": "/companiess/c1"
                            }
                        ],
                        'url': '/contacts/1/companies',
                        'total_count': 2,
                        'has_more': False
                    }
                }
            ]
        }
        metadata = {
            (): {
                'selected': True,
                'table-key-properties': ['id'],
                'forced-replication-method': 'INCREMENTAL',
                'valid-replication-keys': ['updated_at'],
                'inclusion': 'available'
            }, ('properties', 'id'): {
                'inclusion': 'available',
                'selected': True
            }, ('properties', 'tags'): {
                'inclusion': 'available',
                'selected': True
            }, ('properties', 'companies'): {
                'inclusion': 'available',
                'selected': True
            }
        }
        data = contacts.get_addressable_list(contact_list, metadata)
        self.assertEqual(data, contact_list)
        self.assertEqual(mocked_get.call_count, 0)

    def test_addressable_list_API_Call(self, mocked_get):
        """
            Test case to verify do the API call (without pagination) if has_more is true
        """
        mocked_get.return_value = {
            "type": "list",
            "data": [
                {
                    "type": "tag",
                    "id": "t1",
                    "name": "tag 1"
                },
                {
                    "type": "tag",
                    "id": "t2",
                    "name": "tag 2"
                },
                {
                    "type": "tag",
                    "id": "t3",
                    "name": "tag 3"
                }
            ]
        }

        client = IntercomClient('test_access_token', 100, None)
        contacts = Contacts(client, None, ['contacts'])
        contact_list = {
            'data': [
                {
                    'id': 1,
                    'tags': {
                        'type': 'list',
                        'data': [
                            {
                                "id": "t1",
                                "type": "tag",
                                "url": "/tags/t1"
                            },
                            {
                                "id": "t2",
                                "type": "tag",
                                "url": "/tags/t2"
                            }
                        ],
                        'url': '/contacts/1/tags',
                        'total_count': 2,
                        'has_more': True
                    },
                    'companies': {
                        'type': 'list',
                        'data': [
                            {
                                "id": "c1",
                                "type": "companies",
                                "url": "/companiess/c1"
                            }
                        ],
                        'url': '/contacts/1/companies',
                        'total_count': 2,
                        'has_more': False
                    }
                }
            ]
        }
        metadata = {
            (): {
                'selected': True,
                'table-key-properties': ['id'],
                'forced-replication-method': 'INCREMENTAL',
                'valid-replication-keys': ['updated_at'],
                'inclusion': 'available'
            }, ('properties', 'id'): {
                'inclusion': 'available',
                'selected': True
            }, ('properties', 'tags'): {
                'inclusion': 'available',
                'selected': True
            }, ('properties', 'companies'): {
                'inclusion': 'available',
                'selected': True
            }
        }
        expected_data = {
            'data': [
                {
                    'id': 1,
                    'tags': {
                        'type': 'list',
                        "data": [
                            {
                                "type": "tag",
                                "id": "t1",
                                "name": "tag 1"
                            },
                            {
                                "type": "tag",
                                "id": "t2",
                                "name": "tag 2"
                            },
                            {
                                "type": "tag",
                                "id": "t3",
                                "name": "tag 3"
                            }
                        ],
                        'url': '/contacts/1/tags',
                        'total_count': 2,
                        'has_more': True
                    },
                    'companies': {
                        'type': 'list',
                        'data': [
                            {
                                "id": "c1",
                                "type": "companies",
                                "url": "/companiess/c1"
                            }
                        ],
                        'url': '/contacts/1/companies',
                        'total_count': 2,
                        'has_more': False
                    }
                }
            ]
        }
        data = contacts.get_addressable_list(contact_list, metadata)
        self.assertEqual(data, expected_data)

    def test_addressable_list_API_Call_with_pagination(self, mocked_get):
        """
            Test case to verify do the API call (with pagination) if has_more is true
        """
        mocked_get.side_effect = [{
            "type": "list",
            "data": [
                {
                    "type": "tag",
                    "id": "t1",
                    "name": "tag 1"
                },
                {
                    "type": "tag",
                    "id": "t2",
                    "name": "tag 2"
                },
                {
                    "type": "tag",
                    "id": "t3",
                    "name": "tag 3"
                }
            ],
            "pages": {
                "type": "pages",
                "next": "next"
            }
        },
        {
            "type": "list",
            "data": [
                {
                    "type": "tag",
                    "id": "t4",
                    "name": "tag 4"
                },
                {
                    "type": "tag",
                    "id": "t5",
                    "name": "tag 5"
                },
                {
                    "type": "tag",
                    "id": "t6",
                    "name": "tag 6"
                }
            ]
        }]

        client = IntercomClient('test_access_token', 100, None)
        contacts = Contacts(client, None, ['contacts'])
        contact_list = {
            'data': [
                {
                    'id': 1,
                    'tags': {
                        'type': 'list',
                        'data': [
                            {
                                "id": "t1",
                                "type": "tag",
                                "url": "/tags/t1"
                            },
                            {
                                "id": "t2",
                                "type": "tag",
                                "url": "/tags/t2"
                            }
                        ],
                        'url': '/contacts/1/tags',
                        'total_count': 2,
                        'has_more': True
                    },
                    'companies': {
                        'type': 'list',
                        'data': [
                            {
                                "id": "c1",
                                "type": "companies",
                                "url": "/companiess/c1"
                            }
                        ],
                        'url': '/contacts/1/companies',
                        'total_count': 2,
                        'has_more': False
                    }
                }
            ]
        }
        metadata = {
            (): {
                'selected': True,
                'table-key-properties': ['id'],
                'forced-replication-method': 'INCREMENTAL',
                'valid-replication-keys': ['updated_at'],
                'inclusion': 'available'
            }, ('properties', 'id'): {
                'inclusion': 'available',
                'selected': True
            }, ('properties', 'tags'): {
                'inclusion': 'available',
                'selected': True
            }, ('properties', 'companies'): {
                'inclusion': 'available',
                'selected': True
            }
        }
        expected_data = {
            'data': [
                {
                    'id': 1,
                    'tags': {
                        'type': 'list',
                        "data": [
                            {
                                "type": "tag",
                                "id": "t1",
                                "name": "tag 1"
                            },
                            {
                                "type": "tag",
                                "id": "t2",
                                "name": "tag 2"
                            },
                            {
                                "type": "tag",
                                "id": "t3",
                                "name": "tag 3"
                            },
                            {
                                "type": "tag",
                                "id": "t4",
                                "name": "tag 4"
                            },
                            {
                                "type": "tag",
                                "id": "t5",
                                "name": "tag 5"
                            },
                            {
                                "type": "tag",
                                "id": "t6",
                                "name": "tag 6"
                            }
                        ],
                        'url': '/contacts/1/tags',
                        'total_count': 2,
                        'has_more': True
                    },
                    'companies': {
                        'type': 'list',
                        'data': [
                            {
                                "id": "c1",
                                "type": "companies",
                                "url": "/companiess/c1"
                            }
                        ],
                        'url': '/contacts/1/companies',
                        'total_count': 2,
                        'has_more': False
                    }
                }
            ]
        }
        data = contacts.get_addressable_list(contact_list, metadata)
        self.assertEqual(data, expected_data)
