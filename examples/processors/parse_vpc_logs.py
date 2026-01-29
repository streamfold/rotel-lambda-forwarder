import itertools
import json

from rotel_sdk.open_telemetry.common.v1 import AnyValue, KeyValue
from rotel_sdk.open_telemetry.logs.v1 import ResourceLogs

def process_logs(resource_logs: ResourceLogs):
    """
    Look for VPC flow logs and filter logs that have action == REJECT
    """

    if resource_logs.resource and resource_logs.resource.attributes:
        # Check if this is an AWS VPC Flow Log
        vpc_flow_log_found = False
        for attr in resource_logs.resource.attributes:
            if attr.key == "cloud.platform" and isinstance(attr.value.value, str) and attr.value.value == "aws_vpc_flow_log":
                vpc_flow_log_found = True
                break

        if not vpc_flow_log_found:
            return

    # Filter out VPC flow logs with action == "REJECT"
    for scope_log in resource_logs.scope_logs:
        filtered_log_records = []
        for log_record in scope_log.log_records:
            # Check if this log record has action == "REJECT"
            should_keep = True
            for attr in log_record.attributes:
                if attr.key == "action" and isinstance(attr.value.value, str) and attr.value.value == "REJECT":
                    should_keep = False
                    break

            if should_keep:
                filtered_log_records.append(log_record)

        # Replace the log_records with the filtered list
        scope_log.log_records = filtered_log_records
