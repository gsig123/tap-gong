import time
from typing import Any, Dict, Optional, Union, List, Iterable
from singer_sdk import typing as th  # JSON Schema typing helpers
from tap_gong import config_helper as helper
from tap_gong.client import GongStream
from tap_gong.streams.calls import CallsStream


class CallTranscriptsStream(GongStream):
    """Define custom stream."""

    name = "call_transcripts"
    path = "/v2/calls/transcript"
    primary_keys = ["callId"]
    rest_method = "POST"
    records_jsonpath = "$.callTranscripts[*]"
    parent_stream_type = CallsStream
    ignore_parent_replication_key = False
    replication_key = "started"
    state_partitioning_keys = []

    schema = th.PropertiesList(
        th.Property("callId", th.StringType),
        th.Property("started", th.DateTimeType),
        th.Property(
            "transcript",
            th.ArrayType(
                th.ObjectType(
                    th.Property("speakerId", th.StringType),
                    th.Property("topic", th.StringType),
                    th.Property(
                        "sentences",
                        th.ArrayType(
                            th.ObjectType(
                                th.Property("start", th.IntegerType),
                                th.Property("end", th.IntegerType),
                                th.Property("text", th.StringType),
                            )
                        ),
                    ),
                )
            ),
        ),
    ).to_dict()

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        # Get the parent record (call) from context
        parent_record = context.get("parent_record", {})
        if not parent_record:
            raise ValueError("Parent record not found in context")

        # Copy the started field from parent call
        if "started" not in parent_record:
            raise ValueError("started field not found in parent record")

        row["started"] = parent_record["started"]
        return row

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:
        """Prepare the data payload for the REST API request."""
        # Get the starting timestamp from state for incremental sync
        starting_timestamp = self.get_starting_timestamp(context)
        if starting_timestamp:
            fromDateTime = helper.get_date_time_string(
                starting_timestamp, helper.date_time_format_string
            )
        else:
            # If no state, use the start_date from config
            fromDateTime = helper.get_date_time_string_from_config(
                self.config, helper.start_date_key, helper.date_time_format_string
            )

        # Always use end_date from config
        toDateTime = helper.get_date_time_string_from_config(
            self.config, helper.end_date_key, helper.date_time_format_string
        )

        # Ensure fromDateTime is before toDateTime
        if fromDateTime and toDateTime:
            from_dt = helper.get_date_time_value_from_config(
                {"date": fromDateTime}, "date"
            )
            to_dt = helper.get_date_time_value_from_config({"date": toDateTime}, "date")
            if from_dt >= to_dt:
                # If dates are equal or from is after to, adjust from to be one day before to
                from_dt = to_dt - helper.relativedelta(days=1)
                fromDateTime = helper.get_date_time_string(
                    from_dt, helper.date_time_format_string
                )

        request_body = {
            "cursor": next_page_token,
            "filter": {
                "callIds": [context["call_id"]],
                "fromDateTime": fromDateTime,
                "toDateTime": toDateTime,
            },
        }
        time.sleep(self.request_delay_seconds)
        return request_body
