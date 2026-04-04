# Updated README
<!-- 1. `cd scraper`
2. Run:
`python exporters/handshake_export_to_excel.py`
`python exporters/tartanconnect_export_to_excel.py`
`python exporters/si_export_to_excel.py`
`python exporters/peer_tutoring_export_to_excel.py`
`python exporters/schedule_of_classes_export_to_excel.py` -->
## Development Environment
1. Activate the virtual environment at the root directory
2. Run the Schedule of Classes export from the **backend root** (`cmucal-backend`), not inside `scraper/`:

   ```bash
   python -m scraper.scripts.export_soc
   ```

   **Semester selection (no code edits needed):**

   - **Default (cron-friendly):** With no arguments, the script picks the current and next major term (Spring and Fall layouts) from today’s date. Logic lives in `scraper/helpers/semester.py` (`infer_soc_semester_labels`). Adjust start/end dates there if CMU’s calendar drifts.
   - **Explicit label(s):** Pass one or more labels, e.g. `python -m scraper.scripts.export_soc Spring_26` or `Spring_26 Fall_25`. Formats: `Spring_xx`, `Fall_xx`, `Summer1_xx`, `Summer2_xx` (same as `get_current_semester` in `semester.py`).
   - **Season flags (only when you are *not* passing labels):** `--spring` scrapes only the `Spring_*` entry from the automatic pair; `--fall` only `Fall_*`; neither flag (or both flags together) uses the full automatic pair.

     Examples:

     | Command | OK? |
     |--------|-----|
     | `python -m scraper.scripts.export_soc` | Yes — full automatic pair (same as cron). |
     | `python -m scraper.scripts.export_soc --spring` | Yes — only Spring from that pair. |
     | `python -m scraper.scripts.export_soc --fall` | Yes — only Fall from that pair. |
     | `python -m scraper.scripts.export_soc --spring --fall` | Yes — full pair again (explicit “both”). |
     | `python -m scraper.scripts.export_soc Spring_26` | Yes — exactly that semester (ignores season flags). |
     | `python -m scraper.scripts.export_soc Spring_26 Fall_25` | Yes — only those labels. |
     | `python -m scraper.scripts.export_soc Spring_26 --spring` | No — parser error; use either explicit labels **or** `--spring`/`--fall`, not both. |

   Use `python -m scraper.scripts.export_soc -h` for the full CLI help.

3. The script creates org and category for each SOC event if those don’t exist, then adds events, recurrence rules, and calls an endpoint to generate event occurrences. Generating all events can take around an hour; keep the terminal open during that time.

* If need to delete, run this:
```
curl -X DELETE http://localhost:5001/api/events/batch_delete_events_by_params \
-H "Content-Type: application/json" \
--data-raw '{"semester":"Spring_26","source_url":"https://enr-apps.as.cmu.edu/open/SOC/SOCServlet/completeSchedule"}'
```

## Production Environment
- Cron on Railway runs `python -m scraper.scripts.export_soc` with **no arguments**, so each run uses the automatic Spring/Fall pair for the current date. Override locally or in a one-off job with explicit labels or `--spring` / `--fall` if needed.

# Old README

To use, first create a virtual environment: `python3 -m venv <myenvname>`

Then activate it:

On windows: `.\env\Scripts\activate.bat`
On mac: `source venv/bin/activate`

Then install needed packages: `pip3 install -r requirements.txt`

Then create a file `config.ini` that contains the link to the MONGO database in the format seen in `config.ini.example`.

Then launch the scraper: `python3 main.py`

--------

Updates for the future:
- fix peertutoring cause they switched from the login method to SSO one, breaking the whole scraper...
- improve the Handshake/TC mechanisms to automatically update login cookie information cause it is static at the moment
- might need to abstract "resource_source" out of each scraper file so it's not one to one (i.e. one scraper file like OfficeHoursScraper can query 5 different google calendars, and write to 5 different resource sources. this would
involve updating the update_database function slightly )
- more office hours
- write a backend in flask that connects with mongo and returns a list of events given filters (will also expand out reoccurring events for whole semester)