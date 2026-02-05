
from pymongo import UpdateOne
import datetime

class BaseScraper:
    def __init__(self, db, monitor_name, resource_source):
        self.db = db
        self.monitor_name = monitor_name
        self.resource_source = resource_source
        self.headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "en-US,en;q=0.9",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36"
        }

    def __str__(self):
        return self.monitor_name

    def scrape(self):
        raise NotImplementedError("Scrape method must be implemented by subclasses.")
    
    def scrape_data_only(self):
        raise NotImplementedError("scrape_data_only method must be implemented by subclasses.")

    def get_next_weekday(self, weekday):
        """
        Given a weekday (0=Monday, 6=Sunday), return the next date with that weekday.
        """
        today = datetime.datetime.now().date()
        days_ahead = weekday - today.weekday()
        if days_ahead < 0:
            days_ahead += 7
        next_date = today + datetime.timedelta(days=days_ahead)
        return next_date