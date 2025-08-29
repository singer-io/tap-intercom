import json
import singer
from copy import deepcopy
from tap_intercom.transform import transform_times

LOGGER = singer.get_logger()
LOGGER.info('running test_transform_times unit test')

RAW_CONVERSATION = {
      "type": "conversation",
      "id": "181796700000003",
      "created_at": 1625185065,
      "updated_at": 1628109159,
      "waiting_since": None,
      "snoozed_until": None,
      "source": {
        "type": "conversation",
        "id": "899228398",
        "delivered_as": "customer_initiated",
        "subject": "",
        "body": "<p>Hi, I have a question</p>",
        "author": {
          "type": "lead",
          "id": "60de5b1bbfce4159687586a7",
          "name": None,
          "email": ""
        },
        "attachments": [],
        "url": "https://app.intercom.com/hosted_messenger/itjnb9en"
      },
      "contacts": [
        {
          "type": "contact",
          "id": "60de5b1bbfce4159687586a7"
        }
      ],
      "first_contact_reply": {
        "created_at": 1625185065,
        "type": "conversation",
        "url": "https://app.intercom.com/hosted_messenger/itjnb9en"
      },
      "open": True,
      "state": "open",
      "read": True,
      "tags": [
        {
          "type": "tag",
          "id": "5513362",
          "name": "convo",
          "applied_at": 1628109159,
          "applied_by": {
            "type": "admin",
            "id": "5025932"
          }
        },
        {
          "type": "tag",
          "id": "5513362",
          "name": "convo",
          "applied_at": 1628109136,
          "applied_by": {
            "type": "admin",
            "id": "5025932"
          }
        },
        {
          "type": "tag",
          "id": "5513335",
          "name": "test",
          "applied_at": 1628109136,
          "applied_by": {
            "type": "admin",
            "id": "5025932"
          }
        },
        {
          "type": "tag",
          "id": "5537523",
          "name": "new-tag",
          "applied_at": 1628109136,
          "applied_by": {
            "type": "admin",
            "id": "5025932"
          }
        }
      ],
      "priority": "not_priority",
      "sla_applied": None,
      "statistics": {
        "type": "conversation_statistics",
        "time_to_assignment": None,
        "time_to_admin_reply": None,
        "time_to_first_close": None,
        "time_to_last_close": None,
        "median_time_to_reply": None,
        "first_contact_reply_at": '2018-01-01T00:00:00Z',
        "first_assignment_at": 1625185121,
        "first_admin_reply_at": 1625185121,
        "first_close_at": None,
        "last_assignment_at": 1625185121,
        "last_assignment_admin_reply_at": 1625185121,
        "last_contact_reply_at": 1625185066,
        "last_admin_reply_at": 1625185132,
        "last_close_at": None,
        "last_closed_by_id": None,
        "count_reopens": 0,
        "count_assignments": 1,
        "count_conversation_parts": 4
      },
      "conversation_rating": None,
      "teammates": {
        "type": "admin.list",
        "admins": [
          {
            "type": "admin",
            "id": "5025932"
          }
        ]
      },
      "assignee": {
        "type": "admin",
        "id": "5025932"
      }
    }

EXPECTED_CONVERSATION = {
  "type": "conversation",
  "id": "181796700000003",
  "created_at": 1625185065000,
  "updated_at": 1628109159000,
  "waiting_since": None,
  "snoozed_until": None,
  "source": {
    "type": "conversation",
    "id": "899228398",
    "delivered_as": "customer_initiated",
    "subject": "",
    "body": "<p>Hi, I have a question</p>",
    "author": {
      "type": "lead",
      "id": "60de5b1bbfce4159687586a7",
      "name": None,
      "email": ""
    },
    "attachments": [],
    "url": "https://app.intercom.com/hosted_messenger/itjnb9en"
  },
  "contacts": [
    {
      "type": "contact",
      "id": "60de5b1bbfce4159687586a7"
    }
  ],
  "first_contact_reply": {
    "created_at": 1625185065000,
    "type": "conversation",
    "url": "https://app.intercom.com/hosted_messenger/itjnb9en"
  },
  "open": True,
  "state": "open",
  "read": True,
  "tags": [
    {
      "type": "tag",
      "id": "5513362",
      "name": "convo",
      "applied_at": 1628109159000,
      "applied_by": {
        "type": "admin",
        "id": "5025932"
      }
    },
    {
      "type": "tag",
      "id": "5513362",
      "name": "convo",
      "applied_at": 1628109136000,
      "applied_by": {
        "type": "admin",
        "id": "5025932"
      }
    },
    {
      "type": "tag",
      "id": "5513335",
      "name": "test",
      "applied_at": 1628109136000,
      "applied_by": {
        "type": "admin",
        "id": "5025932"
      }
    },
    {
      "type": "tag",
      "id": "5537523",
      "name": "new-tag",
      "applied_at": 1628109136000,
      "applied_by": {
        "type": "admin",
        "id": "5025932"
      }
    }
  ],
  "priority": "not_priority",
  "sla_applied": None,
  "statistics": {
    "type": "conversation_statistics",
    "time_to_assignment": None,
    "time_to_admin_reply": None,
    "time_to_first_close": None,
    "time_to_last_close": None,
    "median_time_to_reply": None,
    "first_contact_reply_at": 1514764800000.0,
    "first_assignment_at": 1625185121000,
    "first_admin_reply_at": 1625185121000,
    "first_close_at": None,
    "last_assignment_at": 1625185121000,
    "last_assignment_admin_reply_at": 1625185121000,
    "last_contact_reply_at": 1625185066000,
    "last_admin_reply_at": 1625185132000,
    "last_close_at": None,
    "last_closed_by_id": None,
    "count_reopens": 0,
    "count_assignments": 1,
    "count_conversation_parts": 4
  },
  "conversation_rating": None,
  "teammates": {
    "type": "admin.list",
    "admins": [
      {
        "type": "admin",
        "id": "5025932"
      }
    ]
  },
  "assignee": {
    "type": "admin",
    "id": "5025932"
  }
}

CONVERSATION_SCHEMA_DATETIMES = [
    ['first_contact_reply', 'created_at'],
    ['conversation_rating', 'created_at'],
    ['created_at'],
    ['customer_first_reply', 'created_at'],
    ['sent_at'],
    ['snoozed_until'],
    ['statistics', 'first_contact_reply_at'],
    ['statistics', 'first_assignment_at'],
    ['statistics', 'first_admin_reply_at'],
    ['statistics', 'first_close_at'],
    ['statistics', 'last_assignment_at'],
    ['statistics', 'last_assignment_admin_reply_at'],
    ['statistics', 'last_contact_reply_at'],
    ['statistics', 'last_admin_reply_at'],
    ['statistics', 'last_close_at'],
    ['tags', ['applied_at']],
    ['updated_at'],
    ['waiting_since']
]


def test_transform_times():
    conversation_to_transfrom = deepcopy(RAW_CONVERSATION)

    transform_times(conversation_to_transfrom, CONVERSATION_SCHEMA_DATETIMES)
    transformed_conversation_json = json.dumps(conversation_to_transfrom)

    expected_conversation_json = json.dumps(EXPECTED_CONVERSATION)

    assert transformed_conversation_json == expected_conversation_json, "transformed_conversation_json != expected_conversation_json"
