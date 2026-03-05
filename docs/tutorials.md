# Tutorials

This section provides step-by-step guides to help you get started with DataBUS. The tutorials are grounded in the example files included in this repository (`data/data_example.csv` and `data/template_example.yml`) and in the reference workflow script `databus_example.py`.

---

## Tutorial 1: Setting Up Your Environment

### Prerequisites

- Python 3.11+ and [`uv`](https://docs.astral.sh/uv/) installed
- Access to a Neotoma database (test or production)
- A `.env` file with your database connection string (see `.env_example`)

### Installing DataBUS

Clone the repository and install dependencies with `uv`:

```bash
git clone https://github.com/NeotomaDB/DataBUS.git
cd DataBUS
uv sync --extra dev
```

### Configuring the Database Connection

DataBUS reads database credentials from a `.env` file. Copy the provided example and fill in your connection details:

```bash
cp .env_example .env
```

The `.env` file should contain a `PGDB_TANK` key with a JSON-encoded connection string:

```
PGDB_TANK={"host": "your_host", "dbname": "neotoma", "user": "your_user", "password": "your_password", "port": 5432}
```

**Important:** The `.env` file contains sensitive database credentials and is listed in `.gitignore` — it will never be committed to the repository. Do not share or commit this file.

This is loaded automatically in your script via:

```python
from dotenv import load_dotenv
load_dotenv()
connection = json.loads(os.getenv("PGDB_TANK"))
conn = psycopg2.connect(**connection, connect_timeout=5)
```

---

## Tutorial 2: Understanding the Input Files

DataBUS takes two inputs for every upload: a **CSV data file** and a **YAML template** that maps your CSV columns to Neotoma database fields. The repository includes a fully annotated example of each.

### The CSV Data File (`data/data_example.csv`)

Each row in the CSV represents one sample. Site-level metadata (name, coordinates, collection unit info) is repeated across all rows, while depth-varying fields like `Depth`, `Thickness`, and proxy values change per row.

The example file contains three rows for a fictional "Example Lake" site, with pollen counts for *Quercus*, *Betula*, and *Pinus* at depths 0.5, 1.5, and 2.5 cm:

```
SiteName, Latitude, Longitude, Altitude, ..., Depth, Thickness, ..., TaxonName, Value, ...
Example Lake, 45.1234, -90.5678, 300, ..., 0.5, 1.0, ..., Quercus, 45, ...
Example Lake, 45.1234, -90.5678, 300, ..., 1.5, 1.0, ..., Betula, 78, ...
Example Lake, 45.1234, -90.5678, 300, ..., 2.5, 1.0, ..., Pinus, 120, ...
```

The full column set covers the complete upload hierarchy: site → geopolitical units → collection unit → analysis units → chronology → chron controls → geochronology → sample ages → data values → contacts → publications.

### The YAML Template (`data/template_example.yml`)

The template declares how each CSV column maps to a Neotoma database field. Key concepts:

**`rowwise: false`** — this field is constant across all rows (site-level data). DataBUS reads that all rows have only one unique value.

**`rowwise: true`** — this field varies per sample row (depths, measurements).

**`required: true/false`** — whether the field must be present and non-null for validation to pass.

**`chronologyname`** — groups multiple columns into a single age model. All fields sharing the same `chronologyname` value are treated as part of the same chronology.

**`taxonname`** — links a data column to a specific Neotoma taxon. Every column mapped to `ndb.data.value` must have a `taxonname`, and its corresponding units column must carry the same name. DataBUS does **not** create new taxa — if a taxon is not already in Neotoma, validation will fail and the taxon must be added via Tilia or the database. We can assist to create uploading scripts too. The same is true for:
- Dataset Types
- Constituent Databases
- Publications
- Contacts
- Variable Units
- Variable Elements
- Variable Contexts
- Variables

DataBUS does not do this as only stewards are responsible for this process.

A minimal site block looks like this:

```yaml
- column: SiteName
  neotoma: ndb.sites.sitename
  required: true
  rowwise: false
  type: string

- column: Latitude
  neotoma: ndb.sites.geog.latitude
  required: true
  rowwise: false
  type: float

- column: Longitude
  neotoma: ndb.sites.geog.longitude
  required: true
  rowwise: false
  type: float
```

A data variable pair (value + units) looks like this:

```yaml
- column: Unsupported.210Pb
  neotoma: ndb.data.value
  taxonname: Excess 210Pb        # must exist in Neotoma
  rowwise: true
  type: float
  unitcolumn: Unsupported.210Pb.Units

- column: Unsupported.210Pb.Units
  neotoma: ndb.variables.variableunitsid
  taxonname: Excess 210Pb        # must match the data column above
  rowwise: true
  type: string
```

See `data/template_example.yml` for the complete annotated template covering all supported sections.

---

## Tutorial 3: The Two-Pass Workflow

DataBUS is designed to be run **twice**: first to validate your data without modifying the database, then to upload once everything passes. This prevents partial or corrupt submissions.

### Pass 1 — Validate Only

```bash
uv run databus_example.py \
  --data data/ \
  --template data/template_example.yml \
  --logs data/logs/ \
  --upload False # Defaulted to False if not passed
```

This runs all validation steps and writes a `.valid.log` file for each CSV file in `data/`. No data is written to the database.

### Pass 2 — Upload

```bash
uv run databus_example.py \
  --data data/ \
  --template data/template_example.yml \
  --logs data/logs/ \
  --upload True
```

This runs validation again and, only if **every** step passes, commits the data. If any step fails, the transaction is rolled back and nothing is written.

---

## Tutorial 4: What Happens Inside the Script

The `databus_example.py` script walks through as many validation steps for each CSV file. Understanding the structure helps you adapt it for your own dataset. Not all steps need to be run, for example, speleothem data may require to run the step `valid_speleothem.py` but this step is not needed for a pollen record.

### File Integrity Check

Before any validation, DataBUS checks whether the file has already been processed (via hash) and whether it exists in the expected location:

```python
hashcheck = nh.hash_file(filename)
filecheck = nh.check_file(filename, validation_files="data/")
```

If both checks fail, the file is skipped entirely.

### Validation Steps

Each step uses `nh.safe_step()`, which wraps the validator in error handling and logs the result. Results are collected in a `databus` dict that is passed forward to subsequent steps, so later steps can reference IDs produced by earlier ones.

The steps run in this order:

1. **Sites** — validates site name and coordinates
2. **Geopolitical Units** — country, state/province, county
3. **Collection Units** — core handle, collection type, depositional environment
4. **Analysis Units** — depth and thickness per sample row
5. **Datasets** — dataset name and type
6. **Geochron Datasets** — geochronological dataset metadata
7. **Chronologies** — age model name, type, and bounds
8. **Chron Controls** — individual age-depth control points
9. **Geochron** — individual radiometric dates
10. **Geochron Control** — links geochron dates to chron controls
11. **Contacts** — PI, collector, processor, analyst
12. **Database** — contributing database link
13. **Samples** — sample records per analysis unit
14. **Sample Ages** — assigned ages per sample
15. **Data** — proxy measurements (wide format, one column per variable)
16. **Publications** — DOI and citation

### Commit or Rollback

After all steps, DataBUS checks whether every step passed **and** the file hash was clean:

```python
all_true = all(databus[key].validAll for key in databus)
all_true = all_true and hashcheck

if args.upload:
    if all_true:
        databus["finalize"] = nv.insert_final(cur, databus=databus)
        conn.commit()
    else:
        conn.rollback()
```

The `insert_final` call inserts the record into the `datasetsubmissions` table, marking the upload as complete.

### Reading the Log

Each file produces a `<filename>.valid.log`. Each step appends its messages to the log, so you can trace exactly where validation failed. Messages use `✓`, `✗`, and `?` symbols for pass, fail, and informational messages respectively.

---

## Tutorial 5: Adapting the Template for Your Dataset

The `template_example.yml` is a universal template covering all supported fields. For your own dataset you will almost always use only a subset of it.

### Step 1 — Start from the example

Copy `data/template_example.yml` as a starting point. Remove sections that do not apply to your data type (e.g., remove the U-Th geochronology block if you are uploading pollen data).

### Step 2 — Set the required constants

Fill in `datasettypeid` and `databasename` — these are dataset-level constants with no corresponding CSV column:

```yaml
- column: datasettypeid
  neotoma: ndb.datasettypes.datasettypeid
  required: true
  value: pollen          # e.g. "Lead 210", "speleothem", "ostracode surface sample"

- column: databasename
  neotoma: ndb.datasetdatabases.databasename
  required: true
  value: Neotoma Paleoecology Database
```

### Step 3 — Define your data variables

Replace the placeholder `MyVariable1` / `MyVariable2` entries with your actual proxy columns. Each variable needs a value column and a units column, both sharing the same `taxonname`:

```yaml
- column: Quercus
  neotoma: ndb.data.value
  taxonname: Quercus           # must exist in Neotoma taxa table
  required: false
  rowwise: true
  type: float
  unitcolumn: Quercus.Units

- column: Quercus.Units
  neotoma: ndb.variables.variableunitsid
  taxonname: Quercus
  required: false
  rowwise: true
  type: string
```

### Step 4 — Match column names exactly

The `column:` value in the YAML must match the CSV header **exactly** (case-sensitive). A mismatch will cause that field to be silently ignored during validation.

---

## Dataset-Specific Examples

The DataBUS ecosystem includes several repositories that demonstrate the full workflow for specific proxy types. These are the best reference when adapting DataBUS for your own data:

### SISAL — Speleothem Isotope Records

[github.com/NeotomaDB/DataBUS_SISAL](https://github.com/NeotomaDB/DataBUS_SISAL)

A complete example for uploading speleothem stable isotope records (δ¹⁸O, δ¹³C) from the SISAL database. Includes examples of multiple chronologies within a single dataset and the U-Th geochronology workflow.

### Lead-210 Dating

[github.com/NeotomaDB/DataBUS_Pb210](https://github.com/NeotomaDB/DataBUS_Pb210)

Demonstrates uploading ²¹⁰Pb and ¹³⁷Cs radiometric data for recent sediment cores. Shows how to define wide-format activity columns with matching units columns, and how to link the `X210Pb` chronology model.

### Ostracode Surface Samples

[github.com/NeotomaDB/DataBUS_Ostracode](https://github.com/NeotomaDB/DataBUS_Ostracode)

An example for faunal surface sample data. Useful as a reference for datasets without depth-based chronologies, where the focus is on taxonomic counts mapped to sample locations.

---

## Next Steps

- Browse the [Reference](reference.md) for full documentation of all DataBUS classes and validator functions.
- Check the [How-To Guides](how-to-guide.md) for task-oriented recipes (To come soon...).
- If your taxon or variable is not in Neotoma, contact the database stewards or use Tilia to add it before running DataBUS.
