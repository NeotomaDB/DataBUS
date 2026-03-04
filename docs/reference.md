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
::: DataBUS.Publication
::: DataBUS.Repository
::: DataBUS.Response
::: DataBUS.Sample
::: DataBUS.SampleAge
::: DataBUS.Site
::: DataBUS.Speleothem
::: DataBUS.UThSeries
::: DataBUS.Variable

## DataBUS Validator/Uploader

Validation modules for ensuring data integrity and correctness before uploading to the Neotoma database.

::: DataBUS.neotomaValidator.check_file
::: DataBUS.neotomaValidator.valid_analysisunit
::: DataBUS.neotomaValidator.valid_chroncontrols
::: DataBUS.neotomaValidator.valid_chronologies
::: DataBUS.neotomaValidator.valid_collunit
::: DataBUS.neotomaValidator.valid_column
::: DataBUS.neotomaValidator.valid_contact
::: DataBUS.neotomaValidator.valid_csv
::: DataBUS.neotomaValidator.valid_data
::: DataBUS.neotomaValidator.valid_dataset
::: DataBUS.neotomaValidator.valid_dataset_database
::: DataBUS.neotomaValidator.valid_dataset_repository
::: DataBUS.neotomaValidator.valid_datauncertainty
::: DataBUS.neotomaValidator.valid_external_speleothem
::: DataBUS.neotomaValidator.valid_geochron
::: DataBUS.neotomaValidator.valid_geochron_dataset
::: DataBUS.neotomaValidator.valid_geochroncontrol
::: DataBUS.neotomaValidator.valid_geopolitical_units
::: DataBUS.neotomaValidator.valid_hiatus
::: DataBUS.neotomaValidator.valid_horizon
::: DataBUS.neotomaValidator.valid_pbmodel
::: DataBUS.neotomaValidator.valid_publication
::: DataBUS.neotomaValidator.valid_sample
::: DataBUS.neotomaValidator.valid_sample_age
::: DataBUS.neotomaValidator.valid_site
::: DataBUS.neotomaValidator.valid_speleothem
::: DataBUS.neotomaValidator.valid_units
::: DataBUS.neotomaValidator.valid_uth_series
::: DataBUS.neotomaValidator.validGeoPol

## Additional Modules

Additional specialized modules for specific functionality.

::: DataBUS.speleothem_reference_inserts -->