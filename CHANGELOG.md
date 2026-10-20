# Changelog

All notable changes to the DataBUS project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- New `vocabUploader` module (`src/DataBUS/vocabUploader/`) for uploading and validating vocabulary entries (taxa, publications) — BU-385.
- `vocab_uploader` function supports dry-run mode (validates against DB via `ts.inserttaxon` then rolls back) and upload mode (commits).
- `write_csv` helper in `neotomaHelpers` to serialise list-of-dicts results to CSV.
- `taxa_upload-example.py` example script demonstrating the taxa upload workflow with log file and CSV output.
- `bool`/`boolean` type support in `convert_value_by_type` and new `_convert_bool` helper in `utils.py`.
- `group_rows` helper in `neotomaHelpers` and the `groupby: true` template key on `ndb.analysisunits` entries: rows of a long file that share the key (e.g. depth) form one analysis unit and one sample.
- `is_blank_cell` helper in `utils.py`, shared by `clean_column` and `group_rows`.
- `run_step` helper in `neotomaHelpers`: runs one validation step through `safe_step`, stores its Response in `databus[key]` and logs it. `logging_response` is now exported from `neotomaHelpers`.

### Changed

- `vocab_uploader` signature now accepts a `conn` parameter to support explicit commit/rollback per row.
- Taxa insertion switched from a raw `INSERT` to the `ts.inserttaxon` stored procedure.
- `parse_arguments` updated to expose `--upload` flag consumed by the vocab uploader.
- `add_note_entry` in `utils.py` now correctly accepts and stores `clean_value` (previously ignored the argument).
- `valid_analysisunit` and `valid_sample` read the grouped rows (`groupby`), so a long file gives one unit and sample per depth instead of one per row.
- `valid_data` assigns each long-format datum the sample of its analysis unit (previously only a single-sample file got a `sampleid`). It reports an error, and inserts nothing, when the units and samples cannot be matched.
- `valid_dataset` reads `ndb.datasets.datasetname` from the data like other columns (a `value:` constant still works); a blank cell inserts NULL.

### Fixed

- `valid_data`: a row whose taxon, element or units name does not resolve is now reported invalid and skipped. Before, its datum was inserted under the `Variable` of the previous row.
- `pull_params.py`: `add_note_entry` call now passes `clean_valor` so notes values are actually recorded.
- `add_note_entry`: added missing `else` branch to assign `clean_value` when notes is not already a list.

## [2.0.0] - 2026-03-05

### Added

- Universal YAML template (`data/template_example.yml`).
- Example CSV data file (`data/data_example.csv`) demonstrating the full column set.
- Comprehensive test suite with coverage reporting via Codecov.
- CI pipeline with Ruff linting, pytest + coverage, and Codecov upload (`.github/workflows/ci.yml`).
- MkDocs documentation site with auto-generated API reference via mkdocstrings.
- Tutorials rewritten to reflect the actual two-pass workflow (`databus_example.py`).
- OpenSSF Best Practices badge tracking.

### Changed

- **Major refactor of the validation/upload architecture** (BU-334, BU-349): each validator now also handles insertion when a populated `databus` dict is supplied, eliminating the separate `neotomaUploader` module and reducing code duplication.
- Refactored `pull_params` into smaller, testable helper functions in `utils.py`, removing the dependency on pandas.
- Contact handling consolidated: all contact types (PI, collector, processor, analyst) now go through `valid_contact`, with chronology modeler assignment handled within `valid_chronologies`. This significantly reduces repeated code.
- Data upload now tracks inserted IDs so that data uncertainties can be linked correctly.
- Chronology handling improved to properly manage calendar years, default chronologies, and sample age linkage.
- Geopolitical unit insertion updated to handle entities like Scotland under the UK.
- Improved logging with `logging_dict` and per-file `.valid.log` output.
- Adopted Ruff as the sole linter and formatter, replacing previous tooling.
- Switched to `uv` for dependency management and script execution.

### Fixed

- Chron controls now handle calendar years properly.
- U-Th series insertion works correctly when the number of geochron indices differs from sample indices.
- Fixed dataset–publication and dataset–database linking during upload.
- Fixed collector insertion for NODE community datasets.
- Fixed variable validation to handle null values without comparing null against null.
- Numerous typos across `chroncontrols.py`, `sample.py`, `Chronology.py`, and others.

## [1.0.0] - 2025-11-27

### Added

- Support for speleothem datasets (SISAL community): U-Th series, external speleothem data, speleothem reference inserts, and entity samples.
- `ExternalSpeleothem` class and corresponding `valid_external_speleothem` validator.
- `UThSeries` class with independent insertion of U-series analytical data.
- Lead-210 (`210Pb`) community support with lead model classes and geochronology workflows.
- Ostracode surface sample support.
- Script for batch speleothem reference inserts after initial upload.
- `hash_file` and `check_file` helpers for file integrity verification before upload.
- `safe_step` wrapper for error-safe validation with automatic logging and rollback.
- `CITATION.cff` for academic citation.
- `code_of_conduct.md`.

### Changed

- Expanded contact name parsing to handle initials and periods in given names.
- Improved handling of diverse data groups across communities.

### Fixed

- Geochronology data handling for SISAL-specific dating methods.
- Entity cover insertion errors in the database layer.
- Various fixes for community-specific edge cases (NODE, 210Pb, SISAL).

## [] - 2023-11-15

### Added

- Initial release of DataBUS.
- Core data classes: `Site`, `CollectionUnit`, `AnalysisUnit`, `Sample`, `Dataset`, `Datum`, `Variable`, `Chronology`, `ChronControl`, `Geochron`, `GeochronControl`, `Contact`, `Geog`, `Hiatus`, `Response`.
- Validation framework with `neotomaValidator` module.
- Helper utilities: `template_to_dict`, `read_csv`, `pull_params`, `pull_required`.
- CLI argument parsing via `parse_arguments`.
- Basic pollen dataset upload workflow.

[2.0.0]: https://github.com/NeotomaDB/DataBUS/compare/v1.0.0...v2.0.0
[1.0.0]: https://github.com/NeotomaDB/DataBUS/compare/v0.0.1...v1.0.0
[]: https://github.com/NeotomaDB/DataBUS/releases/tag/v0.0.1
