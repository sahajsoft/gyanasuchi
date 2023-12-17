from modal import Stub, Image


def create_stub(name: str) -> Stub:
    return Stub(
        name=name,
        image=Image.debian_slim()
        .apt_install(
            "python3-dev",
            "default-libmysqlclient-dev",
            "build-essential",
            "pkg-config"
        )
        .poetry_install_from_file('pyproject.toml')
    )
