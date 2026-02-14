import os

from scraper.exporters.supplemental_instruction_export_to_excel import run_scrape_and_export  # noqa: E402


# Run with "python -m tests.raw_tests.test_SI_export" from base directory                                                                                                           ─╯
if __name__ == "__main__":
    print("Running Supplemental Instruction export script...")
    print(f"Working directory: {os.getcwd()}\n")
    run_scrape_and_export()
