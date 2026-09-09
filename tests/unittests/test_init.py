import subprocess
import sys
import textwrap
import unittest
from unittest import mock
from tap_intercom import main
from parameterized import parameterized
from tap_intercom.discover import discover
from tap_intercom.client import IntercomForbiddenError
from singer.catalog import Catalog


class Parse_Args:
    """Mocked Parse args class"""

    def __init__(self,discover=False, state = None, catalog = None) -> None:
        self.discover = discover
        self.state = state
        self.catalog = catalog
        self.config = {
            "access_token": "test_token",
            "user_agent": "test_agent"
        }


@mock.patch('tap_intercom.client.IntercomClient.__enter__', return_value=mock.MagicMock())
class TestIntercomInit(unittest.TestCase):
    @parameterized.expand([ # test_name, [discover_flag, catalog_flag], [discover_called, sync_called]
        ["discover_called", [True, False], [True, False]],
        ["sync_called", [False, True], [False, True]],
        ["discover_and_sync_called", [False, False], [True, True]]
    ])
    @mock.patch('singer.utils.parse_args')
    @mock.patch('tap_intercom._discover')
    @mock.patch('tap_intercom.sync')
    def test_init(self, test_name, test_data, exp, mock_sync, mock_discover, mock_args, mock_client):
        """Test init file for different flag scenarios"""

        # Return mocked args
        mock_args.return_value = Parse_Args(discover=test_data[0], catalog=test_data[1])
        main()

        self.assertEqual(mock_discover.called,exp[0])
        self.assertEqual(mock_sync.called,exp[1])

    def test_discover_coverage(self, mock_client):
        """Test discover returns catalog having an instance of singer Catalog"""

        catalog = discover(mock_client.return_value)
        self.assertIsInstance(catalog, Catalog)

    @mock.patch('singer.utils.parse_args')
    @mock.patch('tap_intercom._discover')
    @mock.patch('singer.catalog.Catalog.dump')
    def test_discover_raises_and_emits_no_catalog_when_no_access(
        self, mock_dump, mock_discover, mock_args, mock_client
    ):
        """
        When no streams are accessible, _discover raises IntercomForbiddenError.
        main() must propagate that error (in-process) rather than dumping an
        empty/partial catalog. See TestIntercomCLIProcessExit below for the
        actual non-zero process-exit-code assertion.
        """
        mock_args.return_value = Parse_Args(discover=True)
        mock_discover.side_effect = IntercomForbiddenError(
            "HTTP-error-code: 403, Error: The credentials do not have 'read' access to any supported streams."
        )

        with self.assertRaises(IntercomForbiddenError):
            main()

        mock_dump.assert_not_called()


class TestIntercomCLIProcessExit(unittest.TestCase):
    """
    Runs the tap in a real subprocess, exactly the way the installed
    `tap-intercom` console-script entry point invokes it (`sys.exit(main())`),
    to assert the *actual process exit code* and stdout content — not just
    that an exception is raised in-process.
    """

    def test_cli_exits_nonzero_and_emits_no_catalog_when_no_access(self):
        """
        Simulate every stream being inaccessible (_discover raises
        IntercomForbiddenError). The spawned process must:
          1. Exit with a non-zero status code.
          2. Emit no catalog (no 'streams' JSON) on stdout.
        """
        script = textwrap.dedent("""
            import sys
            from unittest import mock

            from tap_intercom.client import IntercomForbiddenError

            class _Args:
                discover = True
                state = None
                catalog = None
                config = {'access_token': 'test_token', 'user_agent': 'test_agent'}

            with mock.patch('singer.utils.parse_args', return_value=_Args()), \\
                 mock.patch(
                     'tap_intercom.client.IntercomClient.__enter__',
                     return_value=mock.MagicMock()
                 ), \\
                 mock.patch(
                     'tap_intercom._discover',
                     side_effect=IntercomForbiddenError(
                         "HTTP-error-code: 403, Error: The credentials do "
                         "not have 'read' access to any supported streams."
                     )
                 ):
                import tap_intercom
                # Mirrors the generated console-script wrapper: sys.exit(main())
                sys.exit(tap_intercom.main())
        """)

        result = subprocess.run(
            [sys.executable, '-c', script],
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertNotEqual(
            result.returncode, 0,
            "CLI process must exit non-zero when no streams are accessible. "
            "stdout={!r} stderr={!r}".format(result.stdout, result.stderr)
        )
        self.assertNotIn('"streams"', result.stdout)
        self.assertEqual(result.stdout.strip(), '')
