# CMS Hospitals Theme Downloader

Downloads every dataset tagged with the theme **"Hospitals"** from the CMS
provider-data metastore, normalizes CSV headers to `snake_case`, and tracks
run metadata so subsequent runs only re-download datasets that changed.

Source API:
`https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items`

## What it does

1. Queries the metastore for all datasets and keeps only those tagged with
   theme `"Hospitals"`.
2. Downloads every CSV distribution for those datasets in parallel (thread
   pool — this is an I/O-bound task).
3. Rewrites each CSV's header row so every column name is `snake_case`, e.g.
   `"Patients' rating of the facility linear mean score"` becomes
   `patients_rating_of_the_facility_linear_mean_score`.
4. Records the metastore's `modified` date for each dataset in a local state
   file (`.state.json`). On the next run, a dataset is only re-downloaded if
   its `modified` date changed — this makes the job idempotent and safe to
   schedule once a day.

## Requirements

- Python 3.9+
- `pip install -r requirements.txt` (just `requests`)

## Usage

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

python cms_hospitals_downloader.py [options]
```

### Options

| Flag | Description | Default |
|---|---|---|
| `--output-dir DIR` | Where CSVs are written | `./data/hospitals` |
| `--state-file FILE` | Where run metadata is tracked | `<output-dir>/.state.json` |
| `--theme NAME` | Metastore theme to filter on | `Hospitals` |
| `--workers N` | Max parallel download threads | `8` |
| `--force` | Ignore the state file and re-download everything | off |
| `-v`, `--verbose` | Debug logging | off |

## Output layout

```
data/hospitals/
  <dataset-identifier>/
    <Original_File_Name>.csv   # header row rewritten to snake_case
  .state.json                  # per-dataset modified date + downloaded files
```

See [sample_output/](sample_output) for example CSV and `state.json` output.

## Scheduling (run once a day)

Add a crontab entry, e.g. run every day at 03:00:

```
0 3 * * * /usr/bin/python3 /path/to/cms_hospitals_downloader.py >> /var/log/cms_hospitals.log 2>&1
```

The script is safe to re-run at any time: unchanged datasets are skipped.
Use `--force` to bypass the state file and re-download everything.

## Verifying it works

```bash
# Normal run: downloads only new/changed datasets
python cms_hospitals_downloader.py -v

# Force a full re-download to exercise the download path end-to-end
python cms_hospitals_downloader.py --force -v
```

A successful run exits with code `0` and logs a summary line, e.g.:

```
Run complete. Downloaded/updated: 0, skipped (unchanged): 75, failed: 0
```
