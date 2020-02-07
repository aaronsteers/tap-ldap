import singer
from singer import utils
from tap_ldap import catalog_spec, sync_spec


REQUIRED_CONFIG_KEYS = ["start_date", "username", "password"]
LOGGER = singer.get_logger()


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    if args.discover:
        # If discover flag is passed, run discovery mode and dump output to stdout
        catalog = catalog_spec.discover()
        catalog.dump()
    else:
        # Otherwise run in sync mode
        if args.catalog:
            LOGGER.info("Parsing catalog from command line argument...")
            catalog = args.catalog
        else:
            LOGGER.info("Parsing catalog from discovery...")
            catalog = catalog_spec.discover()
        sync_spec.sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()
