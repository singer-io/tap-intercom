#!/usr/bin/env python3

import singer
from tap_intercom.client import IntercomClient
from singer import utils

from tap_intercom.discover import discover
from tap_intercom.sync import sync

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    'access_token',
    'start_date',
    'user_agent'
]


def do_discover(client: IntercomClient):

    LOGGER.info('Starting discover')
    catalog = discover(client=client)
    catalog.dump()
    LOGGER.info('Finished discover')


@utils.handle_top_exception(LOGGER)
def main():
    '''
    Entrypoint function for tap.
    '''
    # Parse command line arguments
    parsed_args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # Use the Intercom client as a context manager which makes sure __enter__ is called to check the creds
    with IntercomClient(
        access_token=parsed_args.config["access_token"],
        user_agent=parsed_args.config.get("user_agent"),
        config_request_timeout=parsed_args.config.get("request_timeout")
    ) as client:
        # If discover flag was passed, run discovery mode and dump output to stdout
        if parsed_args.discover:
            do_discover(client=client)
        # Otherwise run in sync mode
        else:
            if parsed_args.catalog:
                catalog = parsed_args.catalog
            else:
                catalog = discover(client=client)
            sync(parsed_args.config, parsed_args.state, catalog)

if __name__ == '__main__':
    main()
