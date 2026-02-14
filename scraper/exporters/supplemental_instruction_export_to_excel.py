import os
import traceback

import pandas as pd

from scraper.monitors.academic import SupplementalInstructionScraper


def run_scrape_and_export():
    """
    Runs the Supplemental Instruction scraper and saves the output to an Excel file
    in the current working directory.
    """
    print("Initializing Supplemental Instruction scraper")
    try:
        si_scraper = SupplementalInstructionScraper(None)

        print("Running scraper")
        resources = si_scraper.scrape_data_only()

        if not resources:
            print("Scraper ran but found no data.")
            return

        print(f"Scraper found {len(resources)} resources. Converting to DataFrame")

        rows = []
        for resource in resources:
            professors_str = (
                ", ".join(resource.professors) if resource.professors else ""
            )
            si_leaders_str = (
                ", ".join(resource.si_leaders) if resource.si_leaders else ""
            )

            if resource.time_locations:
                for time_location in resource.time_locations:
                    rows.append(
                        {
                            "Course Number": resource.course_num,
                            "Course Name": resource.course_name,
                            "Professors": professors_str,
                            "SI Leaders": si_leaders_str,
                            "Recurrence Frequency": time_location.get(
                                "recurrence_frequency", ""
                            ),
                            "Recurrence Interval": time_location.get(
                                "recurrence_interval", ""
                            ),
                            "Recurrence By Day": time_location.get(
                                "recurrence_by_day", ""
                            ),
                            "Start Datetime": time_location.get("start_datetime", ""),
                            "End Datetime": time_location.get("end_datetime", ""),
                            "Location": time_location.get("location", ""),
                        }
                    )
            else:
                rows.append(
                    {
                        "Course Number": resource.course_num,
                        "Course Name": resource.course_name,
                        "Professors": professors_str,
                        "SI Leaders": si_leaders_str,
                        "Recurrence Frequency": "",
                        "Recurrence Interval": "",
                        "Recurrence By Day": "",
                        "Start Datetime": "",
                        "End Datetime": "",
                        "Location": "",
                    }
                )

        df = pd.DataFrame(rows)
        output_filename = "supplemental_instruction.xlsx"
        df.to_excel(output_filename, index=False)

        full_path = os.path.abspath(output_filename)
        print("\n-------------------------------------------------")
        print("✅ Success! Data exported to:")
        print(f"{full_path}")
        print("-------------------------------------------------")

    except Exception as e:
        print("\n❌ An error occurred during scraping or export:")
        print(f"{e}")
        traceback.print_exc()


if __name__ == "__main__":
    run_scrape_and_export()
