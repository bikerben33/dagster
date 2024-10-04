import os
from typing import Any, Iterable, Mapping, Sequence, Union

import requests
from airflow.utils.context import Context

from dagster_airlift.in_airflow.base_asset_operator import BaseDagsterAssetsOperator


class BaseMaterializeAssetsOperator(BaseDagsterAssetsOperator):
    """An operator base class that proxies execution to a user-provided list of Dagster assets.
    Will throw an error at runtime if not all assets can be found on the corresponding Dagster instance.

    Args:
        asset_key_paths (Sequence[Union[str, Sequence[str]]]): A list of asset key paths to materialize.
            Each asset key is expected to be a bare string (which is converted to an asset key path with a single element)
            or a list of strings, which is considered to be a full path.
    """

    def __init__(self, asset_key_paths: Sequence[Union[str, Sequence[str]]], *args, **kwargs):
        self.asset_key_paths = [
            [path] if isinstance(path, str) else path for path in asset_key_paths
        ]
        super().__init__(*args, **kwargs)

    def filter_asset_nodes(
        self, context: Context, asset_nodes: Sequence[Mapping[str, Any]]
    ) -> Iterable[Mapping[str, Any]]:
        path_str_to_node = {
            get_asset_key_hash(node["assetKey"]["path"]): node for node in asset_nodes
        }
        if not all(get_asset_key_hash(path) in path_str_to_node for path in self.asset_key_paths):
            raise ValueError(
                f"Could not find all asset key paths {self.asset_key_paths} in the asset nodes. Found: {list(path_str_to_node.keys())}"
            )
        yield from [path_str_to_node[get_asset_key_hash(path)] for path in self.asset_key_paths]


def get_asset_key_hash(path: Sequence[str]) -> int:
    """Asset keys are represented by a sequence of strings. This function converts the sequence to a hashable object, which allows us to use it as a key in a dictionary."""
    return hash(tuple(path))


class BlankSessionAssetsOperator(BaseMaterializeAssetsOperator):
    """An assets operator which opens a blank session and expects the dagster URL to be set in the environment.
    The dagster url is expected to be set in the environment as DAGSTER_URL.
    """

    def get_dagster_session(self, context: Context) -> requests.Session:
        return requests.Session()

    def get_dagster_url(self, context: Context) -> str:
        return os.environ["DAGSTER_URL"]
