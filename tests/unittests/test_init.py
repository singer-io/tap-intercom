import unittest
from unittest import mock
from tap_intercom import main
from parameterized import parameterized
from tap_intercom import discover
from singer.catalog import Catalog


class Parse_Args:
    """Mocked Parse args class"""

    def __init__(self,discover=False,config = None,state = None, catalog = None) -> None:
        self.discover = discover
        self.config = config
        self.state = state
        self.catalog = catalog

class TestIntercomInit(unittest.TestCase):
    @parameterized.expand([ # test_name, [discover_flag, catalog_flag], [discover_called, sync_called]
        ["discover_called", [True,False], [True, False]],
        ["sync_called", [False,True], [False, True]],
        ["discover_and_sync_called", [False,False], [True, True]]
    ])
    @mock.patch('singer.utils.parse_args')
    @mock.patch('tap_intercom.discover')
    @mock.patch('tap_intercom.sync')
    def test_init(self,test_name,test_data,exp,mock_sync,mock_discover,mock_args):
        """Test init file for different flag scenarios"""

        # Return mocked args
        mock_args.return_value = Parse_Args(discover = test_data[0],catalog = test_data[1])
        main()

        self.assertEqual(mock_discover.called,exp[0])
        self.assertEqual(mock_sync.called,exp[1])
    
    def test_discover_coverage(self):
        """Test discover returns catalog having an instance of singer Catalog"""

        catalog = discover()
        self.assertIsInstance(catalog,Catalog)
        
