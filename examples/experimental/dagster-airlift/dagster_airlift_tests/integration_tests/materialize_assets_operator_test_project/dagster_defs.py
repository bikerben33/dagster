from dagster import Definitions, asset


@asset
def some_asset():
    return "asset_value"


@asset
def other_asset():
    return "other_asset_value"


defs = Definitions(assets=[some_asset, other_asset])
