import unittest
from unittest import mock

from parameterized import parameterized

from singer.catalog import Catalog

from tap_intercom.client import (
    IntercomClient,
    IntercomForbiddenError,
    IntercomNotFoundError,
    IntercomPaymentRequiredError,
    IntercomUnauthorizedError,
    IntercomScrollExistsError,
)
from tap_intercom.discover import (
    _apply_access_checks,
    _prune_inaccessible_children,
    discover,
)
from tap_intercom.streams import (
    BaseStream,
    Companies,
    Contacts,
    Conversations,
    ConversationParts,
    Tags,
    Teams,
)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _make_mock_stream_cls(tap_stream_id, parent=None, accessible=True):
    """
    Return a mock stream *class* whose instances report a fixed check_access().

    parent   -- set to a mock/class to simulate a child stream
    accessible -- return value of check_access()
    """
    cls = mock.MagicMock()
    cls.parent = parent
    instance = mock.MagicMock()
    instance.tap_stream_id = tap_stream_id
    instance.check_access.return_value = accessible
    cls.return_value = instance
    return cls


def _make_schemas(stream_names):
    """Build minimal schemas / field_metadata dicts for the given names."""
    schemas = {name: {'type': 'object'} for name in stream_names}
    field_metadata = {name: [] for name in stream_names}
    return schemas, field_metadata


# ---------------------------------------------------------------------------
# 1.  BaseStream.check_access()
# ---------------------------------------------------------------------------

class TestBaseStreamCheckAccess(unittest.TestCase):
    """Tests for the check_access() method added to BaseStream."""

    def setUp(self):
        self.client = IntercomClient(
            access_token='test_token', config_request_timeout=''
        )

    # --- child streams probe parent then themselves ------------------------

    @mock.patch('tap_intercom.client.IntercomClient.perform')
    def test_child_stream_probes_parent_then_self(self, mock_perform):
        """Child stream probes parent first (to get an id) then probes itself."""
        parent_id = 'conv-abc'
        mock_perform.side_effect = [
            {Conversations.data_key: [{'id': parent_id}]},  # parent probe
            {},                                              # child probe
        ]
        stream = ConversationParts(client=self.client)
        result = stream.check_access()

        self.assertTrue(result)
        self.assertEqual(mock_perform.call_count, 2)
        # First call uses parent (Conversations) path and POST method
        first_args = mock_perform.call_args_list[0]
        self.assertEqual(first_args[0][0], 'POST')
        self.assertEqual(first_args[0][1], Conversations.path)
        # Second call uses child path formatted with parent id
        second_args = mock_perform.call_args_list[1]
        self.assertEqual(second_args[0][0], 'GET')
        self.assertIn(parent_id, second_args[0][1])

    @mock.patch('tap_intercom.client.IntercomClient.perform')
    def test_child_stream_returns_false_when_parent_forbidden(self, mock_perform):
        """403 on parent probe → False without probing child."""
        mock_perform.side_effect = IntercomForbiddenError('HTTP-error-code: 403')
        stream = ConversationParts(client=self.client)

        result = stream.check_access()

        self.assertFalse(result)
        mock_perform.assert_called_once()  # only parent probe was attempted

    @mock.patch('tap_intercom.client.IntercomClient.perform')
    def test_child_stream_returns_false_when_parent_unauthorized(self, mock_perform):
        """401 on parent probe → False without probing child."""
        mock_perform.side_effect = IntercomUnauthorizedError('HTTP-error-code: 401')
        stream = ConversationParts(client=self.client)

        result = stream.check_access()

        self.assertFalse(result)
        mock_perform.assert_called_once()

    @mock.patch('tap_intercom.client.IntercomClient.perform')
    def test_child_stream_returns_false_when_self_probe_forbidden(self, mock_perform):
        """Parent probe succeeds; 403 on child probe → False."""
        parent_id = 'conv-xyz'
        mock_perform.side_effect = [
            {Conversations.data_key: [{'id': parent_id}]},
            IntercomForbiddenError('HTTP-error-code: 403'),
        ]
        stream = ConversationParts(client=self.client)

        result = stream.check_access()

        self.assertFalse(result)
        self.assertEqual(mock_perform.call_count, 2)

    @mock.patch('tap_intercom.client.IntercomClient.perform')
    def test_child_stream_parent_without_data_key_uses_record_directly(self, mock_perform):
        """When parent has no data_key, the raw probe response is used for id."""
        parent_id = 'raw-id-99'
        mock_perform.side_effect = [
            {'id': parent_id},  # raw response (no data_key wrapper)
            {},
        ]
        # Temporarily remove data_key from parent class
        original_data_key = Conversations.data_key
        Conversations.data_key = None
        try:
            stream = ConversationParts(client=self.client)
            result = stream.check_access()
        finally:
            Conversations.data_key = original_data_key

        self.assertTrue(result)
        second_args = mock_perform.call_args_list[1]
        self.assertIn(parent_id, second_args[0][1])

    # --- GET stream uses correct probe kwargs ------------------------------

    @mock.patch('tap_intercom.client.IntercomClient.perform')
    def test_get_stream_probe_called_with_get_method(self, mock_perform):
        """GET stream: perform is called with method='GET'."""
        mock_perform.return_value = {'type': 'list'}
        stream = Tags(client=self.client)

        result = stream.check_access()

        self.assertTrue(result)
        mock_perform.assert_called_once_with(
            'GET',
            stream.path,
            params=stream.params,
            json={},
        )

    # --- POST stream uses correct probe kwargs -----------------------------

    @mock.patch('tap_intercom.client.IntercomClient.perform')
    def test_post_stream_probe_called_with_post_method(self, mock_perform):
        """POST stream: perform called with method='POST' and body."""
        mock_perform.return_value = {'conversations': []}
        stream = Conversations(client=self.client)

        result = stream.check_access()

        self.assertTrue(result)
        mock_perform.assert_called_once_with(
            'POST',
            stream.path,
            params=stream.params,
            json=stream.probe_search_query,
        )

    # --- accessible stream returns True ------------------------------------

    @mock.patch('tap_intercom.client.IntercomClient.perform')
    def test_accessible_stream_returns_true(self, mock_perform):
        """Successful probe returns True."""
        mock_perform.return_value = {'type': 'list'}
        self.assertTrue(Tags(client=self.client).check_access())

    # --- parameterized: auth errors return False ---------------------------

    @parameterized.expand([
        ['401_unauthorized',
         IntercomUnauthorizedError, 'HTTP-error-code: 401', Tags],
        ['403_forbidden',
         IntercomForbiddenError, 'HTTP-error-code: 403', Companies],
    ])
    @mock.patch('tap_intercom.client.IntercomClient.perform')
    def test_auth_error_returns_false(
        self, _name, exc_class, exc_msg, stream_cls, mock_perform
    ):
        """HTTP 401 / 403 -> returns False (never re-raised)."""
        mock_perform.side_effect = exc_class(exc_msg)
        result = stream_cls(client=self.client).check_access()
        self.assertFalse(result)

    # --- scroll_exists treated as accessible -------------------------------

    @mock.patch('tap_intercom.client.IntercomClient.perform')
    def test_scroll_exists_returns_true(self, mock_perform):
        """
        scroll_exists (400) proves a prior scroll session was open and the
        endpoint is reachable — must return True.
        """
        mock_perform.side_effect = IntercomScrollExistsError(
            'HTTP-error-code: 400, Error_Code:scroll_exists'
        )
        result = Companies(client=self.client).check_access()
        self.assertTrue(result)

    # --- parameterized: correct log level on access check ------------------

    @parameterized.expand([
        ['unauthorized_logs_warning',
         IntercomUnauthorizedError('HTTP-error-code: 401'), 'warning', True],
        ['forbidden_logs_warning',
         IntercomForbiddenError('HTTP-error-code: 403'), 'warning', True],
        ['accessible_no_error_log', None, 'error', False],
    ])
    @mock.patch('tap_intercom.streams.LOGGER')
    @mock.patch('tap_intercom.client.IntercomClient.perform')
    def test_log_level(
        self, _name, exc, log_method, expect_called, mock_perform, mock_logger
    ):
        """Verify the correct log level is (or is not) triggered."""
        if exc:
            mock_perform.side_effect = exc
        else:
            mock_perform.return_value = {}

        Tags(client=self.client).check_access()

        logger_fn = getattr(mock_logger, log_method)
        if expect_called:
            logger_fn.assert_called()
        else:
            logger_fn.assert_not_called()

    @mock.patch('tap_intercom.streams.LOGGER')
    @mock.patch('tap_intercom.client.IntercomClient.perform')
    def test_scroll_exists_logs_info_not_warning(self, mock_perform, mock_logger):
        """scroll_exists must log at INFO level only — no WARNING or ERROR."""
        mock_perform.side_effect = IntercomScrollExistsError(
            'HTTP-error-code: 400, Error_Code:scroll_exists'
        )
        Companies(client=self.client).check_access()

        mock_logger.warning.assert_not_called()
        mock_logger.error.assert_not_called()
        mock_logger.info.assert_called()

    # --- empty parent data list still probes child with placeholder id -----

    @mock.patch('tap_intercom.client.IntercomClient.perform')
    def test_child_stream_probes_placeholder_when_parent_data_empty(self, mock_perform):
        """
        Parent reachable but empty data list → child endpoint is still
        probed using a placeholder id (no IndexError, no assumed access).
        """
        mock_perform.side_effect = [
            {Conversations.data_key: []},  # parent probe: no records
            {},                             # child probe with placeholder id succeeds
        ]
        result = ConversationParts(client=self.client).check_access()
        self.assertTrue(result)
        self.assertEqual(mock_perform.call_count, 2)
        second_args = mock_perform.call_args_list[1]
        self.assertIn('0', second_args[0][1])

    @mock.patch('tap_intercom.streams.LOGGER')
    @mock.patch('tap_intercom.client.IntercomClient.perform')
    def test_child_stream_warns_when_parent_data_empty(self, mock_perform, mock_logger):
        """WARNING must be logged when parent probe returns an empty records list."""
        mock_perform.side_effect = [
            {Conversations.data_key: []},
            {},
        ]
        ConversationParts(client=self.client).check_access()
        mock_logger.warning.assert_called()

    @mock.patch('tap_intercom.client.IntercomClient.perform')
    def test_child_stream_returns_false_when_placeholder_probe_forbidden(self, mock_perform):
        """Empty parent data, then 403 on the placeholder child probe → False."""
        mock_perform.side_effect = [
            {Conversations.data_key: []},
            IntercomForbiddenError('HTTP-error-code: 403'),
        ]
        result = ConversationParts(client=self.client).check_access()
        self.assertFalse(result)

    @mock.patch('tap_intercom.client.IntercomClient.perform')
    def test_placeholder_probe_not_found_is_accessible(self, mock_perform):
        """A 404 on the placeholder id proves the endpoint is reachable → True."""
        mock_perform.side_effect = [
            {Conversations.data_key: []},
            IntercomNotFoundError('HTTP-error-code: 404'),
        ]
        result = ConversationParts(client=self.client).check_access()
        self.assertTrue(result)

    # --- generic (non-401/403/404/scroll) API error during own probe -------

    @mock.patch('tap_intercom.client.IntercomClient.perform')
    def test_check_access_generic_error_returns_false(self, mock_perform):
        """
        Any other IntercomError (e.g. 402 payment required) raised while
        probing the stream's own endpoint must be caught and return False,
        never propagate and hard-fail discovery.
        """
        mock_perform.side_effect = IntercomPaymentRequiredError('HTTP-error-code: 402')
        result = Tags(client=self.client).check_access()
        self.assertFalse(result)

    # --- Companies probes the non-scroll list endpoint ---------------------

    @mock.patch('tap_intercom.client.IntercomClient.perform')
    def test_companies_check_access_probes_non_scroll_path(self, mock_perform):
        """Companies.check_access must not hit companies/scroll during discovery."""
        mock_perform.return_value = {'data': [{'id': 'c1'}]}
        Companies(client=self.client).check_access()
        call_path = mock_perform.call_args[0][1]
        self.assertEqual(call_path, Companies.probe_path)
        self.assertNotEqual(call_path, Companies.path)

    @mock.patch('tap_intercom.client.IntercomClient.perform')
    def test_companies_check_access_uses_probe_params(self, mock_perform):
        """Companies probe must send probe_params, not the scroll params."""
        mock_perform.return_value = {'data': [{'id': 'c1'}]}
        Companies(client=self.client).check_access()
        self.assertEqual(mock_perform.call_args[1]['params'], Companies.probe_params)


# ---------------------------------------------------------------------------
# 2.  get_probe_data() — probe_path / probe_params support
# ---------------------------------------------------------------------------

class TestGetProbeData(unittest.TestCase):
    """Tests for BaseStream.get_probe_data() probe_path/probe_params fallback."""

    def setUp(self):
        self.client = IntercomClient(
            access_token='test_token', config_request_timeout=''
        )

    def test_probe_path_takes_precedence_over_path(self):
        """When probe_path is defined it is used instead of path."""
        stream = Companies(client=self.client)
        path, _, _, _ = stream.get_probe_data(stream)
        self.assertEqual(path, Companies.probe_path)
        self.assertNotEqual(path, Companies.path)

    def test_probe_params_take_precedence_over_params(self):
        """When probe_params is defined it is used instead of params."""
        stream = Companies(client=self.client)
        _, params, _, _ = stream.get_probe_data(stream)
        self.assertEqual(params, Companies.probe_params)

    def test_falls_back_to_path_when_probe_path_absent(self):
        """Streams without probe_path fall back to path."""
        stream = Tags(client=self.client)
        path, _, _, _ = stream.get_probe_data(stream)
        self.assertEqual(path, Tags.path)

    def test_falls_back_to_params_when_probe_params_absent(self):
        """Streams without probe_params fall back to params."""
        stream = Tags(client=self.client)
        _, params, _, _ = stream.get_probe_data(stream)
        self.assertEqual(params, Tags.params)

    def test_parent_record_id_uses_formatted_path_not_probe_path(self):
        """When parent_record_id is given, path.format() is used — probe_path is ignored."""
        stream = ConversationParts(client=self.client)
        parent_id = 'conv-999'
        path, _, _, _ = stream.get_probe_data(stream, parent_record_id=parent_id)
        self.assertEqual(path, ConversationParts.path.format(parent_id))


# ---------------------------------------------------------------------------
# 4.  _prune_inaccessible_children()
# ---------------------------------------------------------------------------

class TestPruneInaccessibleChildren(unittest.TestCase):
    """Tests for _prune_inaccessible_children() in discover.py."""

    def test_child_removed_when_parent_absent_from_schemas(self):
        """
        conversation_parts must be pruned when conversations is absent from
        schemas (removed because it was inaccessible).
        """
        schemas = {'conversation_parts': {}, 'tags': {}}
        field_metadata = {'conversation_parts': [], 'tags': []}

        pruned = _prune_inaccessible_children(schemas, field_metadata)

        self.assertNotIn('conversation_parts', schemas)
        self.assertNotIn('conversation_parts', field_metadata)
        self.assertIn('conversation_parts', pruned)

    def test_child_not_pruned_when_parent_present(self):
        """Child stream is retained when its parent is still in schemas."""
        schemas = {'conversations': {}, 'conversation_parts': {}}
        field_metadata = {'conversations': [], 'conversation_parts': []}

        pruned = _prune_inaccessible_children(schemas, field_metadata)

        self.assertIn('conversation_parts', schemas)
        self.assertIn('conversation_parts', field_metadata)
        self.assertEqual(pruned, [])

    def test_unrelated_streams_unaffected(self):
        """Streams without a replicable parent in schemas are untouched."""
        schemas = {'conversations': {}, 'conversation_parts': {}, 'tags': {}}
        field_metadata = {
            'conversations': [], 'conversation_parts': [], 'tags': []
        }

        _prune_inaccessible_children(schemas, field_metadata)

        self.assertIn('tags', schemas)
        self.assertIn('conversations', schemas)

    def test_child_with_non_replicable_parent_not_pruned(self):
        """
        'admins' has parent AdminList (to_replicate=False).  Since AdminList is
        never in schemas (not replicable), admins must NOT be pruned.
        """
        schemas = {'admins': {}, 'tags': {}}
        field_metadata = {'admins': [], 'tags': []}

        _prune_inaccessible_children(schemas, field_metadata)

        self.assertIn('admins', schemas)

    def test_noop_when_child_already_absent(self):
        """Must not raise if the child stream was never added to schemas."""
        schemas = {'tags': {}}
        field_metadata = {'tags': []}

        try:
            pruned = _prune_inaccessible_children(schemas, field_metadata)
        except Exception as exc:  # pragma: no cover
            self.fail(
                '_prune_inaccessible_children raised unexpectedly: {}'.format(
                    exc
                )
            )
        self.assertEqual(pruned, [])

    @mock.patch('tap_intercom.discover.LOGGER')
    def test_warning_logged_for_pruned_child(self, mock_logger):
        """A WARNING must be emitted when a child stream is pruned."""
        schemas = {'conversation_parts': {}, 'tags': {}}
        field_metadata = {'conversation_parts': [], 'tags': []}

        _prune_inaccessible_children(schemas, field_metadata)

        mock_logger.warning.assert_called()
        self.assertIn('conversation_parts', str(mock_logger.warning.call_args))

    @parameterized.expand([
        ['conversations_inaccessible', 'conversations', 'conversation_parts'],
    ])
    def test_parameterized_prune(
        self, _name, inaccessible_parent, expected_pruned_child
    ):
        """Parameterized: child is pruned when its replicable parent is gone."""
        schemas = {inaccessible_parent: {}, expected_pruned_child: {}, 'tags': {}}
        field_metadata = {
            inaccessible_parent: [], expected_pruned_child: [], 'tags': []
        }
        # Remove the parent to simulate it being denied
        schemas.pop(inaccessible_parent)
        field_metadata.pop(inaccessible_parent)

        pruned = _prune_inaccessible_children(schemas, field_metadata)

        self.assertNotIn(expected_pruned_child, schemas)
        self.assertNotIn(expected_pruned_child, field_metadata)
        self.assertIn(expected_pruned_child, pruned)


# ---------------------------------------------------------------------------
# 5.  _apply_access_checks()
# ---------------------------------------------------------------------------

class TestApplyAccessChecks(unittest.TestCase):
    """Tests for _apply_access_checks() in discover.py."""

    def setUp(self):
        self.client = IntercomClient(
            access_token='test_token', config_request_timeout=''
        )

    # --- all accessible — schemas unchanged --------------------------------

    def test_all_accessible_schemas_unchanged(self):
        """When every stream is accessible, schemas is not modified."""
        mock_streams = {
            'tags': _make_mock_stream_cls('tags', accessible=True),
            'teams': _make_mock_stream_cls('teams', accessible=True),
        }
        schemas, field_metadata = _make_schemas(['tags', 'teams'])

        with mock.patch('tap_intercom.discover.STREAMS', mock_streams):
            _apply_access_checks(self.client, schemas, field_metadata)

        self.assertIn('tags', schemas)
        self.assertIn('teams', schemas)

    # --- parameterized: single denied stream excluded; others survive ------

    @parameterized.expand([
        ['tags_denied',  'tags',  ['teams']],
        ['teams_denied', 'teams', ['tags']],
    ])
    def test_unauthorized_stream_excluded(
        self, _name, denied, expected_present
    ):
        """A denied stream must not appear in schemas after access checks."""
        mock_streams = {
            denied: _make_mock_stream_cls(denied, accessible=False),
            **{
                name: _make_mock_stream_cls(name, accessible=True)
                for name in expected_present
            },
        }
        schemas, field_metadata = _make_schemas([denied] + expected_present)

        with mock.patch('tap_intercom.discover.STREAMS', mock_streams):
            _apply_access_checks(self.client, schemas, field_metadata)

        self.assertNotIn(denied, schemas)
        for name in expected_present:
            self.assertIn(name, schemas)

    # --- all denied → empty catalog, no hard-fail ---------------------------

    def test_all_unauthorized_does_not_raise(self):
        """
        All streams inaccessible -> discovery must NOT raise. schemas ends up
        empty and a warning is logged, but _apply_access_checks must return
        normally so discover() can produce an (empty) catalog.
        """
        mock_streams = {
            'tags': _make_mock_stream_cls('tags', accessible=False),
            'teams': _make_mock_stream_cls('teams', accessible=False),
        }
        schemas, field_metadata = _make_schemas(['tags', 'teams'])

        with mock.patch('tap_intercom.discover.STREAMS', mock_streams):
            try:
                _apply_access_checks(self.client, schemas, field_metadata)
            except Exception as exc:  # pragma: no cover
                self.fail(
                    '_apply_access_checks raised unexpectedly when all '
                    'streams were denied: {}'.format(exc)
                )

        self.assertEqual(schemas, {})
        self.assertEqual(field_metadata, {})

    @mock.patch('tap_intercom.discover.LOGGER')
    def test_all_unauthorized_logs_warning_for_empty_catalog(self, mock_logger):
        """When schemas ends up empty, a WARNING must explain why."""
        mock_streams = {
            'tags': _make_mock_stream_cls('tags', accessible=False),
        }
        schemas, field_metadata = _make_schemas(['tags'])

        with mock.patch('tap_intercom.discover.STREAMS', mock_streams):
            _apply_access_checks(self.client, schemas, field_metadata)

        mock_logger.warning.assert_called()

    # --- child pruned when its parent is denied (cascade) ------------------

    def test_child_excluded_when_parent_denied(self):
        """
        When conversations is denied, conversation_parts must also be removed
        without calling check_access on the child.
        """
        parent_cls = mock.MagicMock()
        parent_cls.tap_stream_id = 'conversations'
        parent_cls.to_replicate = True

        child_mock_cls = _make_mock_stream_cls(
            'conversation_parts', parent=parent_cls, accessible=True
        )
        mock_streams = {
            'conversations': _make_mock_stream_cls(
                'conversations', accessible=False
            ),
            'conversation_parts': child_mock_cls,
            'tags': _make_mock_stream_cls('tags', accessible=True),
        }
        schemas, field_metadata = _make_schemas(
            ['conversations', 'conversation_parts', 'tags']
        )

        with mock.patch('tap_intercom.discover.STREAMS', mock_streams):
            _apply_access_checks(self.client, schemas, field_metadata)

        self.assertNotIn('conversations', schemas)
        self.assertNotIn('conversation_parts', schemas)
        self.assertIn('tags', schemas)
        # child check_access must NOT have been called
        child_mock_cls.return_value.check_access.assert_not_called()

    # --- child itself denied while its parent IS accessible -----------------

    def test_child_excluded_when_child_itself_denied(self):
        """
        When the parent is accessible but the child's own check_access()
        returns False, the child must still be excluded (not short-circuited
        via the parent-denied path).
        """
        parent_cls = mock.MagicMock()
        parent_cls.tap_stream_id = 'conversations'
        parent_cls.to_replicate = True

        child_mock_cls = _make_mock_stream_cls(
            'conversation_parts', parent=parent_cls, accessible=False
        )
        mock_streams = {
            'conversations': _make_mock_stream_cls(
                'conversations', accessible=True
            ),
            'conversation_parts': child_mock_cls,
            'tags': _make_mock_stream_cls('tags', accessible=True),
        }
        schemas, field_metadata = _make_schemas(
            ['conversations', 'conversation_parts', 'tags']
        )

        with mock.patch('tap_intercom.discover.STREAMS', mock_streams):
            _apply_access_checks(self.client, schemas, field_metadata)

        self.assertIn('conversations', schemas)
        self.assertNotIn('conversation_parts', schemas)
        self.assertIn('tags', schemas)
        # child's own check_access WAS called since the parent is accessible
        child_mock_cls.return_value.check_access.assert_called_once()

    # --- warning logged for inaccessible streams ---------------------------

    @mock.patch('tap_intercom.discover.LOGGER')
    def test_warning_logged_for_inaccessible_streams(self, mock_logger):
        """A WARNING must list the excluded streams."""
        mock_streams = {
            'tags': _make_mock_stream_cls('tags', accessible=False),
            'teams': _make_mock_stream_cls('teams', accessible=True),
        }
        schemas, field_metadata = _make_schemas(['tags', 'teams'])

        with mock.patch('tap_intercom.discover.STREAMS', mock_streams):
            _apply_access_checks(self.client, schemas, field_metadata)

        mock_logger.warning.assert_called()
        self.assertIn('tags', str(mock_logger.warning.call_args))

    @mock.patch('tap_intercom.discover.LOGGER')
    def test_warning_includes_pruned_children(self, mock_logger):
        """Children excluded via _prune_inaccessible_children must appear in the summary warning."""
        parent_cls = mock.MagicMock()
        parent_cls.tap_stream_id = 'conversations'
        parent_cls.to_replicate = True

        # conversation_parts has a replicable parent; mark it accessible so
        # pass-2 short-circuit does NOT trigger — we want _prune to catch it.
        # Achieve this by keeping conversations out of STREAMS (not probed)
        # but present in schemas, so _prune removes it.
        mock_streams = {
            'tags': _make_mock_stream_cls('tags', accessible=True),
            'conversation_parts': _make_mock_stream_cls(
                'conversation_parts', parent=parent_cls, accessible=True
            ),
        }
        # conversations is in schemas but NOT in mock_streams — it won't be
        # probed and won't be removed by pass 1/2, so _prune_inaccessible_children
        # will catch conversation_parts (parent absent from schemas).
        schemas = {'tags': {}, 'conversation_parts': {}}
        field_metadata = {'tags': [], 'conversation_parts': []}

        with mock.patch('tap_intercom.discover.STREAMS', mock_streams):
            _apply_access_checks(self.client, schemas, field_metadata)

        all_warnings = ' '.join(str(c) for c in mock_logger.warning.call_args_list)
        self.assertIn('conversation_parts', all_warnings)


# ---------------------------------------------------------------------------
# 6.  discover()
# ---------------------------------------------------------------------------

class TestDiscover(unittest.TestCase):
    """End-to-end tests for discover(client)."""

    def setUp(self):
        self.client = IntercomClient(
            access_token='test_token', config_request_timeout=''
        )

    @mock.patch('tap_intercom.discover._apply_access_checks')
    def test_discover_returns_catalog_instance(self, _mock_access):
        """discover() must return a singer Catalog object."""
        self.assertIsInstance(discover(self.client), Catalog)

    @mock.patch('tap_intercom.discover._apply_access_checks')
    def test_discover_catalog_has_streams(self, _mock_access):
        """Catalog returned by discover() must contain at least one stream."""
        catalog = discover(self.client)
        self.assertGreater(len(catalog.streams), 0)

    @mock.patch('tap_intercom.discover._apply_access_checks')
    def test_discover_catalog_stream_ids_match_replicable_streams(
        self, _mock_access
    ):
        """
        When _apply_access_checks is a no-op, all replicable streams appear
        in the catalog.
        """
        from tap_intercom.streams import STREAMS

        catalog = discover(self.client)
        catalog_ids = {s.tap_stream_id for s in catalog.streams}
        expected = {
            name for name, cls in STREAMS.items() if cls.to_replicate
        }
        self.assertEqual(catalog_ids, expected)

    def test_discover_returns_empty_catalog_when_all_streams_denied(self):
        """
        discover() must not raise when _apply_access_checks empties every
        stream out of schemas/field_metadata — it should return a Catalog
        with zero streams instead of propagating an error.
        """
        def _empty_out(_client, schemas, field_metadata):
            schemas.clear()
            field_metadata.clear()

        with mock.patch(
            'tap_intercom.discover._apply_access_checks', side_effect=_empty_out
        ):
            catalog = discover(self.client)

        self.assertIsInstance(catalog, Catalog)
        self.assertEqual(len(catalog.streams), 0)

    def test_discover_invokes_apply_access_checks(self):
        """discover() must delegate access gating to _apply_access_checks."""
        with mock.patch(
            'tap_intercom.discover._apply_access_checks'
        ) as mock_access:
            discover(self.client)

        mock_access.assert_called_once()
        # First positional arg is the client
        self.assertEqual(mock_access.call_args[0][0], self.client)

    @mock.patch('tap_intercom.streams.BaseStream.check_access',
                return_value=True)
    def test_discover_full_integration_all_accessible(self, _mock_check):
        """
        Full integration path: with all streams accessible, all replicable
        streams appear in the catalog.
        """
        from tap_intercom.streams import STREAMS

        catalog = discover(self.client)
        catalog_ids = {s.tap_stream_id for s in catalog.streams}
        expected = {
            name for name, cls in STREAMS.items() if cls.to_replicate
        }
        self.assertEqual(catalog_ids, expected)
