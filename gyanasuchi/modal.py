from typing import Dict

from modal import Image
from modal import NetworkFileSystem
from modal import Stub

from gyanasuchi.scrapper.db import data_volume
from gyanasuchi.scrapper.db import data_volume_dir


def create_stub(name: str) -> Stub:
    return Stub(
        name=name,
        image=Image.debian_slim()
        .apt_install(
            "python3-dev",
            "default-libmysqlclient-dev",
            "build-essential",
            "pkg-config",
        )
        .poetry_install_from_file("pyproject.toml"),
    )


def nfs_mapping() -> Dict[str, NetworkFileSystem]:
    return {data_volume_dir: data_volume}
