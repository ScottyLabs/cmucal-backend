import os
import sys
import pandas as pd
from datetime import datetime

from scraper.monitors.academic import ScheduleOfClassesScraper



def run_scrape_and_export():
    """
    Runs the Schedule of Classes scraper and saves the output to an Excel file
    in the current working directory.
    """
    print("Initializing Schedule of Classes scraper...")
    try:
        soc_scraper = ScheduleOfClassesScraper(None)  # No DB connection needed
        
        print("Running scraper... This may take a moment.")
        resources = soc_scraper.scrape_data_only()
        
        if not resources:
            print("Scraper ran but found no data.")
            return

        print(f"Scraper found {len(resources)} resources. Converting to DataFrame...")

        # Convert list of resource objects to a pandas DataFrame
        df = pd.DataFrame([{
            'Course Num': resource.course_num,
            'Course Name': resource.course_name,
            'Lecture Session': resource.lecture_section,
            'Lecture Days': resource.lecture_days,
            'Lecture Time Start': resource.lecture_time_start,
            'Lecture Time End': resource.lecture_time_end,
            'Location': resource.location,
            'Semester': resource.semester
        } for resource in resources])

        # --- File Export ---
        # Define the output file name
        output_filename = "schedule_of_classes.xlsx"
        
        # This will save the file to the directory where you *run* the script
        # (your project's base folder)
        df.to_excel(output_filename, index=False)
        
        # Get the full path for the success message
        full_path = os.path.abspath(output_filename)
        print("\n-------------------------------------------------")
        print(f"✅ Success! Data exported to:")
        print(f"{full_path}")
        print("-------------------------------------------------")

    except Exception as e:
        print(f"\n❌ An error occurred during scraping or export:")
        print(f"{e}")
        import traceback
        traceback.print_exc()

# --- Main execution ---
if __name__ == "__main__":
    run_scrape_and_export()

