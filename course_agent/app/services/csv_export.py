# course_agent/app/services/csv_export.py
import csv
from pathlib import Path

def write_courses_csv(courses, filename):
    if not courses:
        print(f"Skipping {filename} (no rows)")
        return

    output_dir = Path("reports")
    output_dir.mkdir(exist_ok=True)

    path = output_dir / filename

    fieldnames = courses[0].keys()

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(courses)

    print(f"Saved {len(courses)} rows → {path}")
