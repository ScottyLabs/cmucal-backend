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
2. If running the schedule of classes scraper, make sure to change the semester_label in `scraper = ScheduleOfClassesScraper(db, semester_label="Spring_26")`. 
    - Acceptable formats include `Spring_xx`, `Fall_xx`, `Summer1_xx`, `Summer2_xx`.
    - Feel free to change the start and end dates of each semester in `scraper/helpers/semester.py` if needed.
3. Run `python -m scraper.scripts.export_soc` to scrape data and add events to the DB.
    - the script first creates org and category for each SOC event if those don't exist, then add events, recurrence_rules, and calls an endpoint to generate event occurrences.
    - generating all events could take around an hour, please keep the terminal open during that time.

* If need to delete, run this:
```
curl -X DELETE http://localhost:5001/api/events/batch_delete_events_by_params \
-H "Content-Type: application/json" \
--data-raw '{"semester":"Spring_26","source_url":"https://enr-apps.as.cmu.edu/open/SOC/SOCServlet/completeSchedule"}'
```

## Production Environment
- created a cron job on Railway that calls `python -m scraper.scripts.export_soc`

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