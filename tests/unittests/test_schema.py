import unittest
from unittest import mock

from parameterized import parameterized

from tap_intercom.client import (IntercomClient, IntercomError,
                                 IntercomForbiddenError, IntercomNotFoundError,
                                 IntercomRequestTimeoutError,
                                 IntercomScrollExistsError,
                                 IntercomUnauthorizedError)
from tap_intercom.schema import (check_stream_access, get_schemas,
                                 prune_inaccessible_children)


def _make_stream_obj(
    tap_stream_id,
    path,
    parent=None,
    to_replicate=True,
    probe_http_method='GET',
    params=None,
    probe_search_query=None,
):
    """
    Helper: returns a mock stream object with explicit attributes
    so that MagicMock auto-creation does not pollute kwargs.
    """
    obj = mock.MagicMock()
    obj.tap_stream_id = tap_stream_id
    obj.path = path
    obj.parent = parent
    obj.to_replicate = to_replicate
    obj.key_properties = ['id']
    obj.valid_replication_keys = []
    obj.replication_method = 'FULL_TABLE'
    obj.replication_key = None
    obj.probe_http_method = probe_http_method
    obj.params = params if params is not None else {}
    if probe_search_query is not None:
        obj.probe_search_query = probe_search_query
    return obj


def _make_parent_obj(tap_stream_id):
    """Returns a minimal mock representing a parent stream class."""
    parent = mock.MagicMock()
    parent.tap_stream_id = tap_stream_id
    return parent


# ---------------------------------------------------------------------------
# Tests for check_stream_access()
# ---------------------------------------------------------------------------

class TestCheckStreamAccess(unittest.TestCase):
    """Tests for check_stream_access()"""

    def setUp(self):
        self.client = IntercomClient(
            access_token='test_token', config_request_timeout=''
        )

    # -------------------------------------------------------------------
    # Child streams return True immediately — probe_stream never called
    # -------------------------------------------------------------------

    @mock.patch('tap_intercom.client.IntercomClient.probe_stream')
    def test_child_stream_returns_true_without_probe(self, mock_probe):
        """
        Streams with a parent must return True immediately;
        probe_stream must never be called for them.
        """
        parent = _make_parent_obj('conversations')
        stream_obj = _make_stream_obj('conversation_parts', 'conversations/{}',
                                      parent=parent)

        result = check_stream_access(self.client, stream_obj)

        self.assertTrue(result)
        mock_probe.assert_not_called()

    # -------------------------------------------------------------------
    # GET stream — probe called with correct kwargs
    # -------------------------------------------------------------------

    @mock.patch('tap_intercom.client.IntercomClient.probe_stream')
    def test_get_stream_probe_called_with_get_method(self, mock_probe):
        """GET stream: probe_stream called with http_method='GET'."""
        mock_probe.return_value = {'type': 'list'}
        stream_obj = _make_stream_obj('tags', 'tags',
                                      probe_http_method='GET',
                                      params={'include_count': 'true'})

        result = check_stream_access(self.client, stream_obj)

        self.assertTrue(result)
        mock_probe.assert_called_once_with(
            'tags',
            http_method='GET',
            params={'include_count': 'true'},
            json={},
        )

    # -------------------------------------------------------------------
    # POST stream — probe called with http_method='POST' and search query
    # -------------------------------------------------------------------

    @mock.patch('tap_intercom.client.IntercomClient.probe_stream')
    def test_post_stream_probe_called_with_post_method_and_query(
        self, mock_probe
    ):
        """POST stream: probe_stream called with http_method='POST' and json body."""
        mock_probe.return_value = {'conversations': []}
        probe_query = {
            'pagination': {'per_page': 1},
            'query': {'field': 'id', 'operator': '!=', 'value': None},
        }
        stream_obj = _make_stream_obj(
            'conversations', 'conversations/search',
            probe_http_method='POST',
            probe_search_query=probe_query,
        )

        result = check_stream_access(self.client, stream_obj)

        self.assertTrue(result)
        mock_probe.assert_called_once_with(
            'conversations/search',
            http_method='POST',
            params={},
            json=probe_query,
        )

    # -------------------------------------------------------------------
    # Accessible stream returns True
    # -------------------------------------------------------------------

    @mock.patch('tap_intercom.client.IntercomClient.probe_stream')
    def test_accessible_stream_returns_true(self, mock_probe):
        """probe_stream succeeds -> returns True."""
        mock_probe.return_value = {'type': 'list'}
        stream_obj = _make_stream_obj('tags', 'tags')

        result = check_stream_access(self.client, stream_obj)

        self.assertTrue(result)

    # -------------------------------------------------------------------
    # Parameterized: various IntercomError subclasses -> returns False
    # -------------------------------------------------------------------

    @parameterized.expand([
        ['401_unauthorized',
         IntercomUnauthorizedError, 'HTTP-error-code: 401', 'contacts'],
        ['403_forbidden',
         IntercomForbiddenError, 'HTTP-error-code: 403', 'companies']
    ])
    @mock.patch('tap_intercom.client.IntercomClient.probe_stream')
    def test_error_returns_false(
        self, _name, exc_class, exc_msg, stream_name, mock_probe
    ):
        """Only IntercomForbiddenError and IntercomUnauthorizedError are handled to raise error"""
        mock_probe.side_effect = exc_class(exc_msg)
        stream_obj = _make_stream_obj(stream_name, stream_name)

        result = check_stream_access(self.client, stream_obj)

        self.assertFalse(result)

    # -------------------------------------------------------------------
    # scroll_exists (400) must return True — endpoint is reachable
    # -------------------------------------------------------------------

    @mock.patch('tap_intercom.client.IntercomClient.probe_stream')
    def test_scroll_exists_error_returns_true(self, mock_probe):
        """
        IntercomScrollExistsError means a scroll session is already open,
        which proves the endpoint is reachable. Must return True, not False.
        """
        mock_probe.side_effect = IntercomScrollExistsError(
            'HTTP-error-code: 400, Error:scroll already exists '
            'for this workspace, Error_Code:scroll_exists'
        )
        stream_obj = _make_stream_obj('companies', 'companies/scroll')

        result = check_stream_access(self.client, stream_obj)

        self.assertTrue(result)

    @mock.patch('tap_intercom.schema.LOGGER')
    @mock.patch('tap_intercom.client.IntercomClient.probe_stream')
    def test_scroll_exists_logs_info_not_error(self, mock_probe, mock_logger):
        """scroll_exists must log at INFO level, not ERROR."""
        mock_probe.side_effect = IntercomScrollExistsError(
            'HTTP-error-code: 400, Error_Code:scroll_exists'
        )
        stream_obj = _make_stream_obj('companies', 'companies/scroll')

        check_stream_access(self.client, stream_obj)

        mock_logger.error.assert_not_called()
        mock_logger.info.assert_called()

    # -------------------------------------------------------------------
    # Parameterized: correct log level emitted
    # -------------------------------------------------------------------

    @parameterized.expand([
        ['unauthorized_logs_error',
         IntercomUnauthorizedError('HTTP-error-code: 401'), 'error', True],
        ['forbidden_logs_error',
         IntercomForbiddenError('HTTP-error-code: 403'), 'error', True],
        ['accessible_does_not_log_error', None, 'error', False],
    ])
    @mock.patch('tap_intercom.schema.LOGGER')
    @mock.patch('tap_intercom.client.IntercomClient.probe_stream')
    def test_log_behaviour(
        self, _name, exc, log_method, expect_called, mock_probe, mock_logger
    ):
        """Verify the correct log level is (or is not) triggered."""
        if exc:
            mock_probe.side_effect = exc
        else:
            mock_probe.return_value = {}

        stream_obj = _make_stream_obj('contacts', 'contacts')
        check_stream_access(self.client, stream_obj)

        logger_fn = getattr(mock_logger, log_method)
        if expect_called:
            logger_fn.assert_called_once()
            self.assertIn('contacts', logger_fn.call_args[0][0])
        else:
            logger_fn.assert_not_called()


# ---------------------------------------------------------------------------
# Tests for prune_inaccessible_children()
# ---------------------------------------------------------------------------

class TestPruneInaccessibleChildren(unittest.TestCase):
    """Tests for prune_inaccessible_children()"""

    def _schemas_and_meta(self, stream_names):
        """Build minimal schemas/field_metadata dicts for the given stream names."""
        schemas = {name: {'type': 'object'} for name in stream_names}
        field_metadata = {name: [] for name in stream_names}
        return schemas, field_metadata

    def test_child_removed_from_schemas_when_parent_inaccessible(self):
        """
        conversation_parts must be removed from schemas when its parent
        (conversations) is in inaccessible_streams.
        """
        schemas, field_metadata = self._schemas_and_meta(
            ['conversations', 'conversation_parts', 'tags']
        )
        inaccessible = ['conversations']

        prune_inaccessible_children(schemas, field_metadata, inaccessible)

        self.assertNotIn('conversation_parts', schemas)
        self.assertNotIn('conversation_parts', field_metadata)

    def test_child_removed_from_field_metadata_when_parent_inaccessible(self):
        """field_metadata entry for the child must also be pruned."""
        schemas, field_metadata = self._schemas_and_meta(
            ['conversations', 'conversation_parts']
        )
        prune_inaccessible_children(schemas, field_metadata, ['conversations'])

        self.assertNotIn('conversation_parts', field_metadata)

    def test_unrelated_streams_unaffected_by_pruning(self):
        """Streams whose parent is not inaccessible must remain in schemas."""
        schemas, field_metadata = self._schemas_and_meta(
            ['conversations', 'conversation_parts', 'tags', 'teams']
        )
        prune_inaccessible_children(schemas, field_metadata, ['conversations'])

        self.assertIn('tags', schemas)
        self.assertIn('teams', schemas)

    def test_no_pruning_when_no_inaccessible_streams(self):
        """No streams should be removed when inaccessible_streams is empty."""
        schemas, field_metadata = self._schemas_and_meta(
            ['conversations', 'conversation_parts', 'tags']
        )
        prune_inaccessible_children(schemas, field_metadata, [])

        self.assertIn('conversation_parts', schemas)
        self.assertIn('conversation_parts', field_metadata)

    def test_noop_when_child_not_already_in_schemas(self):
        """
        prune_inaccessible_children must not raise even if the child stream
        was never added to schemas (e.g. already skipped during discovery).
        """
        schemas, field_metadata = self._schemas_and_meta(['tags'])
        # conversations is inaccessible but conversation_parts is not in schemas
        try:
            prune_inaccessible_children(
                schemas, field_metadata, ['conversations']
            )
        except KeyError:
            self.fail(
                'prune_inaccessible_children raised KeyError for a child '
                'stream that was not in schemas.'
            )

    @parameterized.expand([
        ['conversations_inaccessible', 'conversations', 'conversation_parts'],
    ])
    def test_parameterized_parent_child_pruning(
        self, _name, inaccessible_parent, expected_removed_child
    ):
        """Parameterized: given inaccessible parent, child is pruned."""
        schemas, field_metadata = self._schemas_and_meta(
            [inaccessible_parent, expected_removed_child, 'tags']
        )

        prune_inaccessible_children(
            schemas, field_metadata, [inaccessible_parent]
        )

        self.assertNotIn(expected_removed_child, schemas)
        self.assertNotIn(expected_removed_child, field_metadata)


# ---------------------------------------------------------------------------
# Tests for get_schemas()
# ---------------------------------------------------------------------------

class TestGetSchemas(unittest.TestCase):
    """Tests for get_schemas()"""

    def setUp(self):
        self.client = IntercomClient(
            access_token='test_token', config_request_timeout=''
        )

    # -------------------------------------------------------------------
    # All streams authorized
    # -------------------------------------------------------------------

    @mock.patch('tap_intercom.schema.check_stream_access', return_value=True)
    def test_all_streams_authorized_all_included(self, _mock_access):
        """All accessible -> all replicable streams appear in the catalog."""
        schemas, field_metadata = get_schemas(self.client)

        self.assertGreater(len(schemas), 0)
        self.assertEqual(set(schemas.keys()), set(field_metadata.keys()))

    # -------------------------------------------------------------------
    # Parameterized: single unauthorized stream excluded; others survive
    # -------------------------------------------------------------------

    @parameterized.expand([
        ['tags_denied',     'tags'],
        ['teams_denied',    'teams'],
        ['segments_denied', 'segments'],
    ])
    @mock.patch('tap_intercom.schema.check_stream_access')
    def test_unauthorized_stream_excluded_from_catalog(
        self, _name, denied_stream, mock_access
    ):
        """A denied stream must not appear in schemas output."""
        mock_access.side_effect = (
            lambda client, obj: obj.tap_stream_id != denied_stream
        )

        schemas, _ = get_schemas(self.client)

        self.assertNotIn(denied_stream, schemas)

    @parameterized.expand([
        ['tags_denied',     'tags',     ['teams', 'contacts', 'segments']],
        ['teams_denied',    'teams',    ['tags',  'contacts', 'segments']],
        ['segments_denied', 'segments', ['tags',  'teams',    'contacts']],
    ])
    @mock.patch('tap_intercom.schema.check_stream_access')
    def test_authorized_streams_unaffected_when_one_unauthorized(
        self, _name, denied_stream, expected_present, mock_access
    ):
        """Authorized streams must still appear when one stream is denied."""
        mock_access.side_effect = (
            lambda client, obj: obj.tap_stream_id != denied_stream
        )

        schemas, _ = get_schemas(self.client)

        for stream in expected_present:
            self.assertIn(stream, schemas)

    # -------------------------------------------------------------------
    # Child stream excluded when parent is inaccessible (early-continue path)
    # -------------------------------------------------------------------

    @mock.patch('tap_intercom.schema.LOGGER')
    @mock.patch('tap_intercom.schema.check_stream_access')
    def test_child_excluded_when_parent_inaccessible(
        self, mock_access, mock_logger
    ):
        """
        conversation_parts (child of conversations) must be excluded
        without calling check_stream_access for it, and a WARNING logged.
        """
        mock_access.side_effect = (
            lambda client, obj: obj.tap_stream_id != 'conversations'
        )

        schemas, _ = get_schemas(self.client)

        self.assertNotIn('conversation_parts', schemas)

        checked = [c[0][1].tap_stream_id for c in mock_access.call_args_list]
        self.assertNotIn('conversation_parts', checked)

        warning_msgs = [str(c) for c in mock_logger.warning.call_args_list]
        self.assertTrue(
            any('conversation_parts' in m for m in warning_msgs),
            msg="Expected a WARNING log mentioning 'conversation_parts'"
        )

    # -------------------------------------------------------------------
    # prune_inaccessible_children called as post-processing step
    # -------------------------------------------------------------------

    @mock.patch('tap_intercom.schema.prune_inaccessible_children')
    @mock.patch('tap_intercom.schema.check_stream_access', return_value=True)
    def test_prune_inaccessible_children_called(
        self, _mock_access, mock_prune
    ):
        """get_schemas must call prune_inaccessible_children after the loop."""
        get_schemas(self.client)

        mock_prune.assert_called_once()

    @mock.patch('tap_intercom.schema.check_stream_access')
    def test_prune_removes_child_that_slipped_into_schemas(self, mock_access):
        """
        If a child stream enters schemas (check_stream_access returns True for
        child streams now), prune_inaccessible_children must remove it when
        the parent was denied.
        """
        # Allow child (check_stream_access returns True for it) but deny parent
        mock_access.side_effect = (
            lambda client, obj: obj.tap_stream_id != 'conversations'
        )

        schemas, field_metadata = get_schemas(self.client)

        # After pruning, child must not appear
        self.assertNotIn('conversation_parts', schemas)
        self.assertNotIn('conversation_parts', field_metadata)

    # -------------------------------------------------------------------
    # All streams unauthorized -> raises IntercomError
    # -------------------------------------------------------------------

    @mock.patch('tap_intercom.schema.check_stream_access', return_value=False)
    def test_all_streams_unauthorized_raises(self, _mock_access):
        """All inaccessible -> IntercomError raised."""
        with self.assertRaises(IntercomError):
            get_schemas(self.client)

    @mock.patch('tap_intercom.schema.LOGGER')
    @mock.patch('tap_intercom.schema.check_stream_access', return_value=False)
    def test_all_streams_unauthorized_logs_error(
        self, _mock_access, mock_logger
    ):
        """All inaccessible -> error logged with 'No accessible streams'."""
        with self.assertRaises(IntercomError):
            get_schemas(self.client)

        mock_logger.error.assert_called()
        self.assertIn(
            'No accessible streams', mock_logger.error.call_args[0][0]
        )

    # -------------------------------------------------------------------
    # to_replicate=False streams are always skipped
    # -------------------------------------------------------------------

    @mock.patch('tap_intercom.schema.check_stream_access', return_value=True)
    def test_non_replicable_streams_never_checked(self, mock_access):
        """Streams with to_replicate=False must skip check_stream_access."""
        from tap_intercom.streams import STREAMS

        non_replicable = [
            name for name, cls in STREAMS.items() if not cls.to_replicate
        ]
        if not non_replicable:
            self.skipTest('No non-replicable streams defined in STREAMS.')

        get_schemas(self.client)

        checked = [c[0][1].tap_stream_id for c in mock_access.call_args_list]
        for stream_name in non_replicable:
            self.assertNotIn(stream_name, checked)
