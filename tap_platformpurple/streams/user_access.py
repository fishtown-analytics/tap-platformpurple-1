import singer

from tap_platformpurple.streams.base import BaseDatePaginatedPlatformPurpleStream
from dateutil.parser import parse

LOGGER = singer.get_logger()


class UserAccessStream(BaseDatePaginatedPlatformPurpleStream):
    TABLE = "user_access"
    KEY_PROPERTIES = ["userEmail", "productID", "startDateTime"]
    API_METHOD = "POST"

    def get_stream_data(self, data):
        out = []

        for item in data.get("data"):
            transformed_item = self.transform_record(item)

            if (
                transformed_item.get("startDateTime") is None
                or transformed_item.get("userEmail") is None
            ):
                LOGGER.warn(transformed_item)
                continue

            else:
                out.append(transformed_item)

        return out

    def get_time_for_state(self, item):
        return parse(item.get("startDateTime"))

    def get_url(self):
        return "https://api-v4-staging.platformpurple.com/api/stats/userAccess4Org"

    def get_filters(self):
        return None
