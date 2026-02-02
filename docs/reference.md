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
<!-- 
## Validators

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

## Uploaders

Modules for inserting validated data into the Neotoma database.

::: DataBUS.neotomaUploader.insert_analysisunit
::: DataBUS.neotomaUploader.insert_chroncontrols
::: DataBUS.neotomaUploader.insert_chronology
::: DataBUS.neotomaUploader.insert_collector
::: DataBUS.neotomaUploader.insert_collunit
::: DataBUS.neotomaUploader.insert_data
::: DataBUS.neotomaUploader.insert_data_processor
::: DataBUS.neotomaUploader.insert_dataset
::: DataBUS.neotomaUploader.insert_dataset_database
::: DataBUS.neotomaUploader.insert_dataset_pi
::: DataBUS.neotomaUploader.insert_dataset_repository
::: DataBUS.neotomaUploader.insert_datauncertainty
::: DataBUS.neotomaUploader.insert_external_speleothem
::: DataBUS.neotomaUploader.insert_final
::: DataBUS.neotomaUploader.insert_geochron
::: DataBUS.neotomaUploader.insert_geochron_dataset
::: DataBUS.neotomaUploader.insert_geochroncontrols
::: DataBUS.neotomaUploader.insert_geopolitical_units
::: DataBUS.neotomaUploader.insert_hiatus
::: DataBUS.neotomaUploader.insert_pbmodel
::: DataBUS.neotomaUploader.insert_publication
::: DataBUS.neotomaUploader.insert_sample
::: DataBUS.neotomaUploader.insert_sample_age
::: DataBUS.neotomaUploader.insert_sample_analyst
::: DataBUS.neotomaUploader.insert_sample_geochron
::: DataBUS.neotomaUploader.insert_site
::: DataBUS.neotomaUploader.insert_speleo_cu
::: DataBUS.neotomaUploader.insert_speleothem
::: DataBUS.neotomaUploader.insert_uth_series

## Helper Functions

Utility modules for common operations such as data processing, file handling, and argument parsing.

::: DataBUS.neotomaHelpers.clean_column
::: DataBUS.neotomaHelpers.excel_to_yaml
::: DataBUS.neotomaHelpers.get_contacts
::: DataBUS.neotomaHelpers.hash_file
::: DataBUS.neotomaHelpers.parse_arguments
::: DataBUS.neotomaHelpers.pull_overwrite
::: DataBUS.neotomaHelpers.pull_params
::: DataBUS.neotomaHelpers.pull_required
::: DataBUS.neotomaHelpers.read_csv
::: DataBUS.neotomaHelpers.retrieve_dict
::: DataBUS.neotomaHelpers.speleothem_reference_inserts
::: DataBUS.neotomaHelpers.template_to_dict

## Additional Modules

Additional specialized modules for specific functionality.

::: DataBUS.speleothem_reference_inserts -->