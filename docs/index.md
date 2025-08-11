# Neotoma DataBUS

## Introduction

The Neotoma Data Bulk Upload System (DataBUS) is a powerful tool to help research groups format, validate and upload data to Neotoma.
The project is intended to reduce the overhead for new data groups within Neotoma in re-formatting and uploading datasets by automating
much of the process.

Intended user groups include:

    * Reseach communities who have been self-hosting data, but are looking for a long-term storage and stewardship solution.
    * Individual researchers with a large number of paleoecological records to submit to Neotoma.
    * Data rescue and recovery groups who wish to submit legacy records to Neotoma.

Once records are in Neotoma, they are accessible through the `neotoma2` R package, through map-based searches using the Neotoma Explorer, and through the Neotoma API. All datasets submitted to Neotoma receive a DOI and are automatically indexed by DataCite and Google search engines. This provides a citable object for your individual datasets that can be tracked across publications and projects through the future.

A critical result of this project is the preparation and retention of each of the decision making processes that goes into integrating new data resources into Neotoma. We hope to provide data workers with the ability to examine each of the choices that went into deciding how an individual dataset was mapped to Neotoma, if they have concerns or questions about how that mapping might impact data representaiton within the database itself.

## Getting Started

The first step in the DataBUS is not programmatic. It requires mapping the data source to the Neotoma data model.  To help support teams looking to work through this process the Neotoma DataBUS team will work with the data contributors to identify and map the data between Neotoma and the source dataset.

![](assets/databus_workflow.svg)

The collaborative process helps the two teams, the data contributors and the Neotoma team, better understand the underlying data, and any important data fields that either are absent from Neotoma, or are implied, but not explicitly stated in the contributors data model. A significant barrier to some groups is the overwhelming number of columns within the [Neotoma data model](assets/tablecolumns.csv). At the time of writing Neotoma contained over 650 columns or variables to manage data. For this reason, we work collaboratively with the data contributors. Many columns are associated with data types that may be irrelevant to the research group, and some are very easily mapped (for example site names, to `ndb.sites.sitename`). As the team works through this data-alignment, it helps both groups understand the data more deeply, and should improve the usability of Neotoma tools for the contributing research group, by giving them a better understanding of the Neotoma Data model.

The colaborative process will result in a data export (or re-formatting) of the contributors data, usually to some form of flat file (`csv` or `yaml`), that has mappings to existing (or to-be-created) Neotoma columns. Notes are made about any data transformations required (*e.g.*, converting DMS coordinates to Decimal Degree for example), and any implied variables that may not be explicitly included in the contributors data (*e.g.*, spatial projection data, variable units).

Once the mapping is complete, we will write Python code to:

1. Export the contributors data to a format that can be easily read into the DataBUS (for example, if the data is in a SQL database, we will denormalize and save to `csv`)
2. Write Python code using the DataBUS to import each data record against a "test" version of the database to provide validation of the upload process.
3. Check and resolve any validation issues with the contributing team.
4. Upload the data to Neotoma.

Once data is in Neotoma, it will be provided with a DOI to make it searchable and citable, and it can be modified, updated or improved using standard data steward tools such as Tilia.

## Examples

Neotoma has partnered with several groups to date to implement the DataBUS workflow. This includes researchers at the [St. Croix Watersheds Research Station](https://new.smm.org/scwrs) ([DataBUS_210Pb](https://github.com/NeotomaDB/DataBUS_210Pb)) to upload ^210^Pb records from across the Midwestern United States, researchers from the Nonmarine Ostracod Distribution in Europe and East Asian databases (NODE, NODEA: [DataBUS_Ostracode](https://github.com/NeotomaDB/DataBUS_Ostracode)) and with the SISAL team for speleothems ([DataBUS_SISAL](https://github.com/NeotomaDB/DataBUS_SISAL)).

Within each of these repositories it is possible to find the [`template.csv`](https://github.com/NeotomaDB/DataBUS_SISAL/blob/main/sisal_tempate.csv) file, which shows how those teams mapped their variables against the Neotoma Data Model.
