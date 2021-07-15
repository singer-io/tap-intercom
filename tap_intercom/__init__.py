#!/usr/bin/env python3

import singer

from singer import utils
from tap_intercom.discover import discover
from tap_intercom.sync import sync

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    'access_token',
    'start_date',
    'user_agent'
]

def do_discover():

    LOGGER.info('Starting discover')
    catalog = discover()
    catalog.dump()
    LOGGER.info('Finished discover')


@utils.handle_top_exception(LOGGER)
def main():
    '''
    Entrypoint function for tap.
    '''
    # Parse command line arguments
    parsed_args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if parsed_args.discover:
        do_discover()
    # Otherwise run in sync mode
    else:
        if parsed_args.catalog:
            catalog = parsed_args.catalog
        else:
            catalog = discover()
        sync(parsed_args.config, parsed_args.state, catalog)

if __name__ == '__main__':
    main()
