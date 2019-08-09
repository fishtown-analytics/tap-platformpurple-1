import datetime
import pytz
import singer

from dateutil.parser import parse
from tap_framework.streams import BaseStream
from tap_framework.config import get_config_start_date
from tap_framework.state import get_last_record_value_for_table, incorporate, save_state

LOGGER = singer.get_logger()


class BasePlatformPurpleStream(BaseStream):
    def get_stream_data(self, data):
        return [self.transform_record(item) for item in data]

    def sync_data(self):
        table = self.TABLE

        LOGGER.info("Querying {}.".format(table))

        response = self.client.make_request(
            self.get_url(), "POST", body=self.get_filters()
        )

        to_write = self.get_stream_data(response)

        with singer.metrics.record_counter(endpoint=table) as ctr:
            singer.write_records(table, to_write)

            ctr.increment(amount=len(to_write))


class BaseDatePaginatedPlatformPurpleStream(BasePlatformPurpleStream):
    def get_time_for_state(self, item):
        return parse(item.get("dateTime"))

    def sync_data(self):
        table = self.TABLE
        done = False

        filters = self.get_filters()
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
                "startMSeconds": int(start_date.timestamp() * 1000),
                "endMSeconds": int(end_date.timestamp() * 1000),
            }

            if filters is not None:
                body["filters"] = filters

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
                LOGGER.info("Advancing one millisecond.")
                start_date = start_date + datetime.timedelta(milliseconds=1)
            else:
                LOGGER.info("Advancing by one page.")
                start_date = max_date

            end_date = start_date + td

            save_state(self.state)
