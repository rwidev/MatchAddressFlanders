# MatchAddressFlanders

Augment Belgium address CSV files with Adressenregister `adresmatch` results.

## Quick start ✅

1. (Optional but recommended) Create and activate a virtual environment:

```bash
python -m venv .venv
source .venv/bin/activate
```

2. Install dependencies:

```bash
pip install requests
```

3. Run the tool for a single file:

```bash
python match_adressen.py path/to/input.csv --output path/to/enriched.csv
```

Or run the tool on a directory of CSVs. By default the script reads from `./data/input` and writes to `./data/output`:

```bash
python match_adressen.py                # reads ./data/input/*.csv and writes to ./data/output/
python match_adressen.py ./data/input   # explicit input directory
```

Useful options:
- `--api-url` — custom adresmatch API endpoint (default: `https://api.basisregisters.vlaanderen.be/v2/adresmatch`)
- `--auth-token` — Bearer token for Authorization header (if required by your endpoint)
- `--timeout` — HTTP timeout in seconds (default: 20.0)
- `--rate-limit` — maximum requests per second (default: 25.0)
- `--delay` — extra sleep between calls after rate limiting
- `--force` — re-query rows that already have `adresmatch_status`
- `--max-rows` — limit how many rows are processed (useful for testing)
- `--output-dir` — directory to write outputs when processing an input directory (default: `./data/output`)
- `--progress-interval` — print a concise progress message every N rows considered (default: 100). Set to 0 to disable progress messages.

See full CLI help with:

```bash
python match_adressen.py --help
```

---

## Expected input columns

The script extracts the query parameters from these input column names. They must be present (or one of municipal/postal must exist) for a row to be queried:

- `LOM_MUN_NM` — municipality name (optional if `LOM_POSTAL_CD` provided)
- `LOM_ROAD_NM` — street name (required)
- `LOM_SOURCE_HNR` — house number (required)
- `LOM_BOXNR` — bus/box number (optional)
- `LOM_POSTAL_CD` — postal code (optional if `LOM_MUN_NM` provided)

If a row lacks the required values the script will mark it with `adresmatch_status = missing_input`.

---

## Output columns added

The following columns are appended (or ensured to exist) in the output CSV. They are populated by the script based on the API response:

- `adresmatch_status` — `matched`, `no_match`, `missing_input`, or `error`
- `adresmatch_score` — numeric match score
- `adresmatch_adres_uri` — identifier/URI of the matched address
- `adresmatch_adres_id` — object id or local id
- `adresmatch_identificator_namespace` — identificator namespace
- `adresmatch_identificator_version` — identificator version
- `adresmatch_gemeente` — municipality name (spelling)
- `adresmatch_straatnaam` — street name (spelling)
- `adresmatch_huisnummer` — house number
- `adresmatch_busnummer` — box/bus number
- `adresmatch_postcode` — postal code
- `adresmatch_toevoeging` — addition / suite
- `adresmatch_pos_method` — method used for geolocation
- `adresmatch_pos_lon` — longitude
- `adresmatch_pos_lat` — latitude
- `adresmatch_error` — error message if any

---

## Behavior notes

- The script updates rows in place unless `--output` is provided to write to a different file.
- It writes to a temporary file and atomically replaces the destination file on success.
- Rate limiting is performed using a simple time-based limiter; use `--rate-limit` and `--delay` to tune throughput.

---

## Troubleshooting

- If you get `ModuleNotFoundError: No module named 'requests'` make sure you activated your virtual environment and installed `requests`.
- If you get `ERROR: Repository not found` when pushing to GitHub, ensure your remote URL uses the correct SSH format `git@github.com:USERNAME/REPO.git` and that your SSH key is added to GitHub.

---

If you'd like, I can add a `requirements.txt` and some unit tests for the helper functions. Want me to add those?

---

## Match buildings — `match_buildings.py` 🏛️

Enrich an adres-enriched CSV with gebouwenregister (building) matches and geometries (WKT).

### Quick usage

Process a single file:

```bash
python match_buildings.py path/to/input_adresmatched.csv --output path/to/output_gebouwen.csv
```

The script will by default write to `<input>_gebouwen.csv` if `--output` is not provided.

### Important options

- `--gebouwen-url` — base URL for gebouwen API (default: `https://api.basisregisters.vlaanderen.be/v2/gebouwen`)
- `--gebouweenheden-url` — base URL for gebouweenheden (default: `https://api.basisregisters.vlaanderen.be/v2/gebouweenheden`)
- `--adres-id-field` — column containing the adres ID (default: `adresmatch_adres_id`)
- `--building-limit` — maximum number of gebouweenheden to request per adres (default: 5)
- `--include-historic` — allow historic (`gehistoreerd`) units when no active units are found
- `--retries` / `--retry-wait` — retry behaviour for API calls
- `--rate-limit` / `--delay` — throttle requests and pause between rows
- `--progress-interval` — print a concise progress message every N rows considered (default: 100). Set to 0 to disable progress messages.
- `--auth` — optional Authorization header value (e.g. `Bearer <token>`)

See full CLI help with:

```bash
python match_buildings.py --help
```

### Expected input columns

The tool expects a CSV containing adres IDs (produced e.g. by `match_adressen.py`). By default the adres ID is read from the column `adresmatch_adres_id`, but you can override that with `--adres-id-field`.

Ideally your input CSV is the output of `match_adressen.py`—it will already contain address fields like `adresmatch_status`, `adresmatch_score`, etc.—but only the adres ID column is strictly necessary.

### Output columns added

The following columns are added to the output CSV:

- `gebouwregister_status` — one of `matched`, `matched_no_geometry`, `no_match`, `missing_adres_id`, or `error`
- `gebouwregister_id` — identifier of the matched building
- `gebouwregister_wkt` — geometry in WKT format (POLYGON, MULTIPOLYGON, POINT) or empty
- `gebouwregister_error` — error message when relevant

### Behavior notes

- The tool will try to fetch gebouweenheden for the provided adres, select the best candidate (skipping historic ones unless `--include-historic` is used), then fetch building detail and extract geometry if available.
- If no geometry is available the status will be `matched_no_geometry` and `gebouwregister_wkt` will be empty.
- The script writes to `<input>_gebouwen.csv` by default.

---

## Running tests 🧪

Install the test dependencies (recommended in your virtual environment):

```bash
pip install -r requirements.txt
```

Run the test suite with pytest:

```bash
python -m pytest -q
```
