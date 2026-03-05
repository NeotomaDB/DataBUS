This part of the project documentation focuses on an **information-oriented** approach. Use it as a
reference for the technical implementation of the `DataBUS` project code.

## Core Data Classes

Core classes representing the fundamental data models used throughout the DataBUS project.

::: DataBUS.AnalysisUnit
::: DataBUS.ChronControl
::: DataBUS.Chronology
::: DataBUS.CollectionUnit
::: DataBUS.Contact
::: DataBUS.Dataset
::: DataBUS.DatasetDatabase
::: DataBUS.DataUncertainty
::: DataBUS.Datum
::: DataBUS.Geochron
::: DataBUS.GeochronControl
::: DataBUS.Geog
::: DataBUS.Hiatus
::: DataBUS.LeadModel
::: DataBUS.Response
::: DataBUS.Sample
::: DataBUS.SampleAge
::: DataBUS.Site
::: DataBUS.Speleothem
::: DataBUS.UThSeries
::: DataBUS.Variable

## DataBUS Validator / Uploader

Validation and insertion modules for the `neotomaValidator` package. Each function validates the
corresponding Neotoma entity and, when a populated `databus` dict is supplied, also inserts the
record into the database within the active transaction.

::: DataBUS.neotomaValidator.valid_site
::: DataBUS.neotomaValidator.valid_geopolitical_units
::: DataBUS.neotomaValidator.valid_collunit
::: DataBUS.neotomaValidator.valid_speleothem
::: DataBUS.neotomaValidator.valid_external_speleothem
::: DataBUS.neotomaValidator.valid_analysisunit
::: DataBUS.neotomaValidator.valid_pbmodel
::: DataBUS.neotomaValidator.valid_dataset
::: DataBUS.neotomaValidator.valid_geochron_dataset
::: DataBUS.neotomaValidator.valid_chronologies
::: DataBUS.neotomaValidator.valid_chroncontrols
::: DataBUS.neotomaValidator.valid_hiatus
::: DataBUS.neotomaValidator.valid_sample
::: DataBUS.neotomaValidator.valid_sample_age
::: DataBUS.neotomaValidator.valid_geochron
::: DataBUS.neotomaValidator.valid_geochroncontrol
::: DataBUS.neotomaValidator.valid_uth_series
::: DataBUS.neotomaValidator.valid_contact
::: DataBUS.neotomaValidator.valid_dataset_database
::: DataBUS.neotomaValidator.valid_data
::: DataBUS.neotomaValidator.valid_datauncertainty
::: DataBUS.neotomaValidator.valid_publication
::: DataBUS.neotomaValidator.insert_final

## DataBUS Helpers

Utility functions in the `neotomaHelpers` package used for parameter extraction, file handling,
template parsing, and transaction management.

### Parameter Extraction

::: DataBUS.neotomaHelpers.pull_params
::: DataBUS.neotomaHelpers.pull_required
::: DataBUS.neotomaHelpers.pull_overwrite

### Template & File Utilities

::: DataBUS.neotomaHelpers.template_to_dict
::: DataBUS.neotomaHelpers.read_csv
::: DataBUS.neotomaHelpers.check_file
::: DataBUS.neotomaHelpers.hash_file
::: DataBUS.neotomaHelpers.excel_to_yaml

### Database & Contact Helpers

::: DataBUS.neotomaHelpers.get_contacts
::: DataBUS.neotomaHelpers.utils

### Transaction Management

::: DataBUS.neotomaHelpers.safe_step

### Logging

::: DataBUS.neotomaHelpers.logging_dict

### CLI

::: DataBUS.neotomaHelpers.parse_arguments

### Speleothem Reference Inserts

::: DataBUS.neotomaHelpers.speleothem_reference_inserts
