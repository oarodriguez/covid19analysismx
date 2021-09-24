# Changelog

Versions follow [CalVer](https://calver.org).

## 2021.2.0.dev0 (Not yet released)

### Added

- Add support to identify code quality issues using `pre-commit` library.
- Update `duckdb`, but avoid version `0.2.9` due to a `Windows fatal exception:
  access violation` in the tests. TODO: Report issue in `duckdb` GitHub repo.
- Update other dependencies.

### Changed

TODO.

### Deprecated

TODO.

### Removed

TODO.

### Fixed

TODO.

---

## 2021.1.0 (2021-09-24)

### Changed

- Change the project name to `covid19mx`.
- Update development tasks script.
- Use `click` instead of `typer` to implement the project CLIs.
- Do not pin versions for any dependencies in pyproject.toml file.
- Use CalVer instead SemVer for versioning. 

## 0.1.0 [2021-04-08]

Project initialization.

### Added

- Define project directory and file layout.
