# Develop Channels

See the [README](../README.md#build-and-install) for prerequisites and installation.

## Build and test

`make setup` installs the plugin editable after building its frontend. To reuse a compatible source checkout, run `make setup HOST_SOURCE=/absolute/path/to/agent-foundation`. The checkout must match `.service-ref`; its own virtual environment is left unchanged. `make verify` builds the frontend, wheel and sdist, checks Python code, installs the wheel and tests real entry-point discovery and packaged assets. Run `make setup` again to return to editable development after `make verify`.

| Command                                                   | Purpose                                                                 |
| --------------------------------------------------------- | ----------------------------------------------------------------------- |
| `make frontend`                                           | Build frontend assets into `src/a13n_channels/static/` |
| `make dev`                                                | Watch frontend source and rebuild the host-mounted bundle               |
| `make build`                                              | Create wheel and sdist in `dist/`                                       |
| `make check`                                              | Check Python formatting and lint                                        |
| `make test`                                               | Test the installed plugin                                               |
| `make install HOST_PYTHON=/path/to/host/.venv/bin/python` | Build and install into a compatible Service environment                 |

## Develop the Console frontend

For live frontend development, install this project editable into the prepared host environment with `uv pip install --python /path/to/host/.venv/bin/python -e .`, run `make dev` here, and reload the App page after each build. `make dev` emits the real host-mounted bundle; it does not run a separate React application. Restart Service after Python changes, installation or upgrades. Increment the version in `pyproject.toml` when changing shipped frontend bytes: Service versions its frontend URLs by the installed distribution version. The Makefile selects the wheel using the current manifest version even when older artifacts remain in `dist/`.

## Release

The GitHub Actions workflow runs these build/test steps on pushes and pull requests and uploads wheel/sdist artifacts. It also rebuilds a wheel from the sdist using its prebuilt frontend. The separate tag-triggered release workflow follows the same build-artifact → publish-job pattern as Service. Configure your GitHub `pypi` environment with a `PYPI_TOKEN` secret and any required approvals, choose a unique distribution name, then update `pyproject.toml` and push a matching `vX.Y.Z` tag to publish. Release tags must match the current project version. The publish job only downloads verified artifacts and runs `uv publish`; it does not rebuild. Ordinary pushes and pull requests never publish. Deployment remains owned by your project.

## Source map

| Path | Owns |
| --- | --- |
| [`src/a13n_channels/`](../src/a13n_channels/) | App APIs, delivery routes, and Agent tools |
| [`src/a13n_channels/channels/`](../src/a13n_channels/channels/) | Channel adapters |
| [`frontend/`](../frontend/) | Console pages and [channel icons](../frontend/src/icons/NOTICE.md) |
| [`pyproject.toml`](../pyproject.toml) | Entry points and package assets |

See [AGENTS.md](../AGENTS.md) for project boundaries and Conventional Commit guidance.
