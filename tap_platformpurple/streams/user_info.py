import datetime
import pytz
import singer

from dateutil.parser import parse
from tap_platformpurple.streams.base import BaseDatePaginatedPlatformPurpleStream
from tap_framework.config import get_config_start_date
from tap_framework.state import get_last_record_value_for_table, incorporate, save_state

LOGGER = singer.get_logger()


class UserInfoStream(BaseDatePaginatedPlatformPurpleStream):
    TABLE = "user_info"
    KEY_PROPERTIES = []
    API_METHOD = "POST"

    def get_stream_data(self, data):
        return [self.transform_record(item) for item in data.get("data")]

    def get_time_for_state(self, item):
        return parse(item.get("startDateTime"))

    def get_url(self):
        return (
            "https://api-v4-staging.platformpurple.com/api/userInfo/records4Environment"
        )

    def get_time_for_state(self, item):
        return datetime.datetime.fromtimestamp(item.get("lastUpdated") / 1e3, pytz.UTC)

    def sync_data(self):
        table = self.TABLE
        done = False

        start_date = get_last_record_value_for_table(self.state, table)

        if start_date is None:
            start_date = get_config_start_date(self.config)
        else:
            start_date = start_date.replace(tzinfo=pytz.UTC)

        td = datetime.timedelta(hours=1)

        end_date = start_date + td

        while not done:
            max_date = start_date

            LOGGER.info("Querying {} starting at {}".format(table, start_date))

            body = {
                "filters": {
                    "environment": self.config.get("environment"),
                    "lastUpdated": {
                        "gte": int(start_date.timestamp() * 1000),
                        "lte": int(end_date.timestamp() * 1000),
                    },
                }
            }

            response = self.client.make_request(self.get_url(), "POST", body=body)

            to_write = self.get_stream_data(response)

            with singer.metrics.record_counter(endpoint=table) as ctr:
                singer.write_records(table, to_write)

                ctr.increment(amount=len(to_write))

                for item in to_write:
                    max_date = max(max_date, self.get_time_for_state(item))

            self.state = incorporate(self.state, table, "start_date", start_date)

            if max_date > datetime.datetime.now(pytz.UTC):
                done = True

            if len(to_write) == 0:
                LOGGER.info("Advancing one full interval.")

                if end_date > datetime.datetime.now(pytz.UTC):
                    done = True
                else:
                    start_date = end_date

            elif start_date == max_date:
                LOGGER.info("Advancing one second.")
                start_date = start_date + datetime.timedelta(seconds=1)

            else:
                LOGGER.info("Advancing by one page.")
                start_date = max_date

            end_date = start_date + td

            save_state(self.state)
