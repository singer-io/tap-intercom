RAW_CONVERSATION = {
    "type": "conversation",
    "id": "96343043564",
    "created_at": 1526379112,
    "updated_at": 1526460726,
    "waiting_since": None,
    "snoozed_until": None,
    "conversation_message": {
      "type": "conversation",
      "id": "910808426",
      "delivered_as": "customer_initiated",
      "subject": "",
      "body": "Hi there, this is a test.",
      "author": {
        "type": "user",
        "id": "9bc4dffaaf5ce40e0c8fb5f5",
        "name": "John Doe",
        "email": "john.doe@myglobal.com"
      },
      "attachments": [],
      "url": "https://app.stitchdata.com/client/999999/pipeline/connections/99999/loading-reports"
    },
    "user": {
      "type": "user",
      "id": "9bc4dffaaf5ce40e0c8fb5f5"
    },
    "customers": [
      {
        "type": "user",
        "id": "9bc4dffaaf5ce40e0c8fb5f5"
      }
    ],
    "customer_first_reply": {
      "created_at": 1526379112,
      "type": "conversation",
      "url": "https://app.stitchdata.com/client/999999/pipeline/connections/99999/loading-reports"
    },
    "assignee": {
      "type": "nobody_admin",
      "id": None
    },
    "open": False,
    "state": "closed",
    "read": True,
    "tags": {
      "type": "tag.list",
      "tags": []
    },
    "conversation_parts": {
      "type": "conversation_part.list",
      "conversation_parts": [
        {
          "type": "conversation_part",
          "id": "999998330",
          "part_type": "comment",
          "body": None,
          "created_at": 1526379112,
          "updated_at": 1526379112,
          "notified_at": 1526379112,
          "assigned_to": {
            "type": "admin",
            "id": "1234567"
          },
          "author": {
            "id": "1274497",
            "type": "bot",
            "name": "Operator",
            "email": "operator@intercom2.com"
          },
          "attachments": [],
          "external_id": None
        },
        {
          "type": "conversation_part",
          "id": "999998331",
          "part_type": "comment",
          "body": "You'll be notified here and by email",
          "created_at": 1526379120,
          "updated_at": 1526379120,
          "notified_at": 1526379120,
          "assigned_to": None,
          "author": {
            "id": "1274497",
            "type": "bot",
            "name": "Operator",
            "email": "operator@intercom2.com"
          },
          "attachments": [],
          "external_id": None
        },
        {
          "type": "conversation_part",
          "id": "999998332",
          "part_type": "comment",
          "body": "You're welcome. And good luck!",
          "created_at": 1526396472,
          "updated_at": 1526396472,
          "notified_at": 1526396472,
          "assigned_to": None,
          "author": {
            "id": "1274890",
            "type": "admin",
            "name": "Jane Smith",
            "email": "jane.smith@help-me-please.com"
          },
          "attachments": [],
          "external_id": None
        }
      ],
      "total_count": 3
    },
    "conversation_rating": {
      "rating": 4,
      "remark": None,
      "created_at": 1526419264,
      "customer": {
        "type": "user",
        "id": "9bc4dffaaf5ce40e0c8fb5f5"
      },
      "teammate": {
        "type": "admin",
        "id": 1234567
      }
    }
  }

EXPECTED_CONVERSATION_PARTS = [{
    'assigned_to': {
      'id': '1234567',
      'type': 'admin'
    },
    'attachments': [],
    'author': {
      'email': 'operator@intercom2.com',
      'id': '1274497',
      'name': 'Operator',
      'type': 'bot'
    },
    'body': None,
    'conversation_created_at': 1526379112,
    'conversation_id': '96343043564',
    'conversation_total_parts': None,
    'conversation_updated_at': 1526460726,
    'created_at': 1526379112,
    'external_id': None,
    'id': '999998330',
    'notified_at': 1526379112,
    'part_type': 'comment',
    'type': 'conversation_part',
    'updated_at': 1526379112
  },
  {
    'assigned_to': None,
    'attachments': [],
    'author': {
      'email': 'operator@intercom2.com',
      'id': '1274497',
      'name': 'Operator',
      'type': 'bot'
    },
    'body': "You'll be notified here and by email",
    'conversation_created_at': 1526379112,
    'conversation_id': '96343043564',
    'conversation_total_parts': None,
    'conversation_updated_at': 1526460726,
    'created_at': 1526379120,
    'external_id': None,
    'id': '999998331',
    'notified_at': 1526379120,
    'part_type': 'comment',
    'type': 'conversation_part',
    'updated_at': 1526379120
  },
  {
    'assigned_to': None,
    'attachments': [],
    'author': {
      'email': 'jane.smith@help-me-please.com',
      'id': '1274890',
      'name': 'Jane Smith',
      'type': 'admin'
    },
    'body': "You're welcome. And good luck!",
    'conversation_created_at': 1526379112,
    'conversation_id': '96343043564',
    'conversation_total_parts': None,
    'conversation_updated_at': 1526460726,
    'created_at': 1526396472,
    'external_id': None,
    'id': '999998332',
    'notified_at': 1526396472,
    'part_type': 'comment',
    'type': 'conversation_part',
    'updated_at': 1526396472
  }
]
