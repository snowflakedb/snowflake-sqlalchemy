from pathlib import Path

import tomlkit


def sync():
    pyproject = tomlkit.loads(Path("pyproject.toml").read_text())
    snyk_reqiurements = Path("snyk/requirements.txt")
    dependencies = pyproject.get("project", {}).get("dependencies", [])

    with snyk_reqiurements.open("w") as fh:
        fh.write("\n".join(dependencies))
        fh.write("\n")


if __name__ == "__main__":
    sync()
