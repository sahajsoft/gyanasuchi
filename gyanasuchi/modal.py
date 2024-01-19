from typing import Dict

from modal import Image
from modal import NetworkFileSystem
from modal import Secret
from modal import Stub

from gyanasuchi.common import data_volume_dir
from gyanasuchi.scrapper.db import data_volume


def create_stub(name: str, *secret_names: str) -> Stub:
    return Stub(
        name=name,
        image=Image.debian_slim().poetry_install_from_file("pyproject.toml"),
        secrets=[Secret.from_name(name) for name in secret_names],
    )


def nfs_mapping() -> Dict[str, NetworkFileSystem]:
    return {data_volume_dir(): data_volume}
