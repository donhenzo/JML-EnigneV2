# Provisioning/package_requests.py
#
# Shared polling helper for Entra access package assignmentRequests.
#
# Currently used by leaver_http. mover_http has its own local copy of
# the same polling loop (_poll_request_until_terminal) predating this
# module — worth consolidating onto this one in a follow-up change.
# Left alone for now since that code is tested and working end-to-end.

import os
import time

from Provisioning.graph_client import JmlGraphClient

PACKAGE_POLL_MAX_ATTEMPTS     = int(os.environ.get("JML_PACKAGE_POLL_MAX_ATTEMPTS", "60"))
PACKAGE_POLL_INTERVAL_SECONDS = int(os.environ.get("JML_PACKAGE_POLL_INTERVAL_SECONDS", "5"))
TERMINAL_REQUEST_STATES = frozenset({"Delivered", "Denied", "Failed", "Canceled"})


def poll_request_until_terminal(
    graph_client:     JmlGraphClient,
    request_id:       str,
    max_attempts:     int = PACKAGE_POLL_MAX_ATTEMPTS,
    interval_seconds: int = PACKAGE_POLL_INTERVAL_SECONDS,
) -> str:
    """
    Poll an assignmentRequest until it reaches a terminal requestState.

    Returns the terminal requestState string (Delivered, Denied, Failed,
    Canceled) or "TimedOut" if the poll window is exhausted first.
    """
    for _ in range(max_attempts):
        status = graph_client.get_assignment_request_status(request_id)
        state  = status.get("requestState", "")
        if state in TERMINAL_REQUEST_STATES:
            return state
        time.sleep(interval_seconds)

    return "TimedOut"