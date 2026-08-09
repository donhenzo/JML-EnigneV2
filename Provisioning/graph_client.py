# provisioning/graph_client.py
#
# Thin Graph API client for the JML provisioning layer.
#
# WHY THIS EXISTS:
#   The Microsoft Graph SDK is async and speaks in SDK model objects.
#   The JML pipeline is synchronous and needs simple dicts back.
#   This file sits between the two — it wraps the exact Graph calls
#   Phase 1 needs, runs async SDK calls synchronously, and converts
#   responses into plain dicts the provisioner can work with.
#
#   It also owns the three custom exception types (GraphClientError,
#   UserNotFoundError, GraphThrottlingError) so the provisioner never
#   has to catch SDK-specific exceptions or inspect raw HTTP status
#   codes directly.
#
# WHAT IT COVERS:
#   User creation and lookup
#   Group membership check and assignment
#   RBAC role assignment check and creation
#   PIM group eligibility assignment
#   Access package assignment (entitlement management)
#
# IDEMPOTENCY:
#   Every write operation has a paired check operation (e.g.
#   check_group_membership → add_group_member). The provisioner calls
#   the check first so the pipeline is safe to retry from the beginning
#   without creating duplicate users, groups, or role assignments.
#
# RETRY LOGIC:
#   All Graph API calls are wrapped with @retry_on_throttle.
#   429 (Too Many Requests) → respects Retry-After header, up to max_retries
#   5xx (Server Error)      → exponential backoff up to max_retries
#   4xx (Client Error)      → fails immediately, no retry (except 429)
#   Other exceptions        → exponential backoff, treated as transient
#
#   IMPORTANT: Methods that catch SDK exceptions and re-raise as
#   GraphClientError must extract the status code before wrapping,
#   otherwise the decorator loses visibility of whether the failure
#   is retryable. See _extract_status_code() and the status_code
#   parameter on GraphClientError.
#
# AUTH:
#   Local dev  — ClientSecretCredential, reads from local.settings.json.
#   Production — swap build_graph_client() to DefaultAzureCredential
#                in Block G. Nothing else in this file changes.
#
# PERMISSIONS REQUIRED (app registration or Managed Identity):
#   User.ReadWrite.All
#   Group.ReadWrite.All
#   RoleManagement.ReadWrite.Directory
#   PrivilegedAccess.ReadWrite.AzureADGroup (Phase 2 PIM)
#   EntitlementManagement.ReadWrite.All (access packages)

import asyncio
import logging
import os
import time
from functools import wraps

from azure.identity import ClientSecretCredential
from msgraph.graph_service_client import GraphServiceClient
from msgraph.generated.models.user import User
from msgraph.generated.models.password_profile import PasswordProfile
from msgraph.generated.models.reference_create import ReferenceCreate

logger = logging.getLogger(__name__)


# Exceptions

class GraphClientError(Exception):
    """
    Base exception for all Graph API failures in this module.

    Wraps SDK-specific exceptions so the provisioner only needs to catch
    one type regardless of what the SDK throws internally.

    status_code is carried on the exception so the @retry_on_throttle
    decorator can classify the failure even after the original SDK exception
    has been replaced by this wrapper. Methods must extract the status code
    from the SDK exception before raising GraphClientError — do not lose it.
    """
    def __init__(self, message: str, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


class UserNotFoundError(GraphClientError):
    """
    Raised by get_user() when a UPN does not exist in Entra ID.

    Kept separate from GraphClientError so check_upn_exists() in
    validation_gate.py can distinguish a clean 404 from a real error
    without parsing exception messages.
    """
    pass


class GraphThrottlingError(GraphClientError):
    """
    Raised when Graph API throttling (429) persists after all retries.

    Kept separate so the provisioner can distinguish throttling failures
    from other transient or permanent errors and route accordingly.
    """
    pass


# Retry decorator

def retry_on_throttle(max_retries: int = 3, base_backoff: float = 2.0):
    """
    Decorator that wraps Graph API calls with retry logic for transient failures.

    Retry strategy:
        429 (Too Many Requests) → respect Retry-After header, up to max_retries
        5xx (Server Error)      → exponential backoff (2^attempt * base_backoff)
        4xx (Client Error)      → fail immediately, no retry (except 429)
        Other exceptions        → exponential backoff, treat as transient

    The decorator reads status_code from:
        - GraphClientError.status_code  (set by methods before re-raising)
        - SDK exceptions with .status_code attribute
        - httpx.HTTPStatusError with .response.status_code
        - String matching for 429/503/502 as a last resort

    This classification only works correctly if methods that catch SDK
    exceptions and re-raise as GraphClientError preserve the status code.
    See the status_code parameter on GraphClientError.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None

            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)

                except Exception as e:
                    last_exception = e
                    status_code = _extract_status_code(e)

                    # 429 — Throttled: respect Retry-After
                    if status_code == 429:
                        retry_after = _extract_retry_after(e)
                        if attempt < max_retries - 1:
                            logger.warning(
                                f"Graph API throttled (429) on attempt {attempt + 1}/{max_retries} "
                                f"— retrying after {retry_after}s — {func.__name__}"
                            )
                            time.sleep(retry_after)
                            continue
                        raise GraphThrottlingError(
                            f"Graph API throttling persisted after {max_retries} attempts: {e}"
                        )

                    # 5xx — Server error: exponential backoff
                    if status_code and 500 <= status_code < 600:
                        if attempt < max_retries - 1:
                            backoff = base_backoff * (2 ** attempt)
                            logger.warning(
                                f"Graph API server error ({status_code}) on attempt {attempt + 1}/{max_retries} "
                                f"— retrying after {backoff}s — {func.__name__}"
                            )
                            time.sleep(backoff)
                            continue
                        raise GraphClientError(
                            f"Graph API server error persisted after {max_retries} attempts: {e}",
                            status_code=status_code
                        )

                    # 4xx (except 429) — Client error: permanent, no retry
                    if status_code and 400 <= status_code < 500:
                        logger.error(
                            f"Graph API client error ({status_code}) — no retry — {func.__name__}: {e}"
                        )
                        raise

                    # Unknown exception — treat as transient: exponential backoff
                    if attempt < max_retries - 1:
                        backoff = base_backoff * (2 ** attempt)
                        logger.warning(
                            f"Graph API transient error on attempt {attempt + 1}/{max_retries} "
                            f"— retrying after {backoff}s — {func.__name__}: {e}"
                        )
                        time.sleep(backoff)
                        continue

                    raise

            raise last_exception

        return wrapper
    return decorator


def _extract_status_code(exception: Exception) -> int | None:
    """
    Extract HTTP status code from various exception shapes.

    Checks in priority order:
        1. GraphClientError.status_code  — set explicitly before re-raising
        2. SDK exception .status_code attribute
        3. httpx .response.status_code
        4. String matching for common codes as last resort
    """
    # GraphClientError with status_code set by the method before re-raising
    if isinstance(exception, GraphClientError) and exception.status_code is not None:
        return exception.status_code

    # msgraph SDK exceptions
    if hasattr(exception, 'status_code'):
        return exception.status_code

    # httpx.HTTPStatusError
    if hasattr(exception, 'response') and hasattr(exception.response, 'status_code'):
        return exception.response.status_code

    # String matching — last resort only
    exc_str = str(exception).lower()
    if "429" in exc_str or "too many requests" in exc_str:
        return 429
    if "503" in exc_str or "service unavailable" in exc_str:
        return 503
    if "502" in exc_str or "bad gateway" in exc_str:
        return 502

    return None


def _extract_retry_after(exception: Exception) -> int:
    """
    Extract Retry-After seconds from a 429 response.

    Checks exception.headers and exception.response.headers.
    Defaults to 60 seconds if the header is absent.
    """
    if hasattr(exception, 'headers') and exception.headers:
        value = exception.headers.get('Retry-After') or exception.headers.get('retry-after')
        if value:
            try:
                return int(value)
            except (ValueError, TypeError):
                pass

    if hasattr(exception, 'response') and hasattr(exception.response, 'headers'):
        value = exception.response.headers.get('Retry-After') or exception.response.headers.get('retry-after')
        if value:
            try:
                return int(value)
            except (ValueError, TypeError):
                pass

    return 60


# Client construction

def build_graph_client() -> tuple:
    """
    Build and return an authenticated GraphServiceClient and credential.

    Returns (GraphServiceClient, ClientSecretCredential) so the credential
    can be reused for direct HTTP calls that the SDK does not natively support,
    such as the PIM eligibility schedule endpoint.
    """
    tenant_id     = os.environ.get("AZURE_TENANT_ID", "")
    client_id     = os.environ.get("AZURE_CLIENT_ID", "")
    client_secret = os.environ.get("AZURE_CLIENT_SECRET", "")

    if not all([tenant_id, client_id, client_secret]):
        raise GraphClientError(
            "Missing Graph API credentials. Ensure AZURE_TENANT_ID, "
            "AZURE_CLIENT_ID, and AZURE_CLIENT_SECRET are set in "
            "local.settings.json."
        )

    credential = ClientSecretCredential(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret
    )

    return GraphServiceClient(credentials=credential), credential


# Graph client

class JmlGraphClient:
    """
    JML-scoped wrapper around GraphServiceClient.

    The Graph SDK is async. This class runs every SDK call synchronously
    via _run() so the provisioner stays simple and linear — no async/await
    anywhere outside this class.

    All methods raise GraphClientError (or its subclasses UserNotFoundError,
    GraphThrottlingError) on failure. The provisioner catches these and records
    which step failed without needing to know anything about the underlying SDK.

    STATUS CODE PRESERVATION:
    Methods that catch SDK exceptions and re-raise as GraphClientError must
    call _extract_status_code() on the original exception and pass the result
    as status_code= to GraphClientError. Without this the decorator loses
    the ability to classify 4xx vs 5xx and will incorrectly retry permanent
    client errors such as invalid domain (400) or unauthorised (401).

    Construct via:
        graph_service_client, credential = build_graph_client()
        client = JmlGraphClient(graph_service_client, credential)
    """

    def __init__(self, graph_client: GraphServiceClient, credential=None) -> None:
        self._client     = graph_client
        self._credential = credential

    def _run(self, coroutine):
        """
        Run an async Graph SDK coroutine synchronously.

        Tries the existing event loop first. If there is none (plain script
        or fresh thread), creates one and cleans it up after the call.
        """
        try:
            return asyncio.get_event_loop().run_until_complete(coroutine)
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                return loop.run_until_complete(coroutine)
            finally:
                loop.close()

    @retry_on_throttle(max_retries=3, base_backoff=2.0)
    def get_user(self, upn: str) -> dict:
        """
        Retrieve a user from Entra ID by UPN.

        Returns a dict with id, upn, display_name, account_enabled,
        department, and job_title.

        Raises UserNotFoundError if the UPN does not exist.
        Raises GraphClientError on any other failure, with status_code set
        so the retry decorator can classify it correctly.
        """
        from msgraph.generated.users.item.user_item_request_builder import UserItemRequestBuilder

        try:
            query_params = UserItemRequestBuilder.UserItemRequestBuilderGetQueryParameters(
                select=["id", "userPrincipalName", "displayName",
                        "accountEnabled", "department", "jobTitle"]
            )
            request_config = UserItemRequestBuilder.UserItemRequestBuilderGetRequestConfiguration(
                query_parameters=query_params
            )

            user = self._run(
                self._client.users.by_user_id(upn).get(
                    request_configuration=request_config
                )
            )

            if user is None:
                raise UserNotFoundError(f"User not found: {upn}", status_code=404)

            return {
                "id":              user.id,
                "upn":             user.user_principal_name,
                "display_name":    user.display_name,
                "account_enabled": user.account_enabled,
                "department":      user.department,
                "job_title":       user.job_title,
            }

        except (UserNotFoundError, GraphClientError):
            raise
        except Exception as e:
            if "not found" in str(e).lower() or "404" in str(e):
                raise UserNotFoundError(f"User not found: {upn}", status_code=404)
            status_code = _extract_status_code(e)
            raise GraphClientError(f"get_user failed for {upn}: {e}", status_code=status_code)

    @retry_on_throttle(max_retries=3, base_backoff=2.0)
    def create_user(self, payload) -> dict:
        """
        Create a new Entra ID user from a canonical IdentityPayload.

        Returns a dict with the new user's Entra object id and upn.
        Group and role assignments are handled separately by the provisioner.

        usage_location is hardcoded to "GB". Update if the tenant spans
        multiple regions with different licensing requirements.

        The temporary password is deterministic (derived from employee_id)
        so a retry after a crash produces the same value. Must-change-on-
        first-login is enforced, so this value is never long-lived.

        Raises GraphClientError with status_code set. A 400 here typically
        means an invalid UPN domain — the retry decorator will not retry it.
        """
        try:
            temp_password = _generate_temp_password(payload.employee_id)

            user                     = User()
            user.display_name        = payload.display_name
            user.user_principal_name = payload.upn
            user.mail_nickname       = payload.upn.split("@")[0]
            user.account_enabled     = True
            user.job_title           = payload.job_title
            user.department          = payload.department
            user.employee_id         = payload.employee_id
            user.usage_location      = "GB"
            user.employee_type       = payload.employment_type.value

            password_profile                                     = PasswordProfile()
            password_profile.password                            = temp_password
            password_profile.force_change_password_next_sign_in = True
            user.password_profile                                = password_profile

            created = self._run(self._client.users.post(user))

            if created is None:
                raise GraphClientError(
                    f"create_user returned None for {payload.upn} — "
                    "Graph API call may have succeeded but returned no object. "
                    "Check Entra ID before retrying."
                )

            logger.info(f"User created — upn={payload.upn}, object_id={created.id}")
            return {"id": created.id, "upn": created.user_principal_name}

        except GraphClientError:
            raise
        except Exception as e:
            status_code = _extract_status_code(e)
            raise GraphClientError(
                f"create_user failed for {payload.upn}: {e}",
                status_code=status_code
            )

    @retry_on_throttle(max_retries=3, base_backoff=2.0)
    def get_group(self, group_id: str) -> dict:
        """
        Retrieve a group by object ID.

        Returns group details including is_dynamic — the provisioner uses
        this to skip manual membership assignment for dynamic groups.

        Raises GraphClientError with status_code set.
        """
        try:
            group = self._run(self._client.groups.by_group_id(group_id).get())

            if group is None:
                raise GraphClientError(f"Group not found: {group_id}", status_code=404)

            return {
                "id":              group.id,
                "display_name":    group.display_name,
                "membership_rule": group.membership_rule,
                "is_dynamic":      bool(group.membership_rule),
            }

        except GraphClientError:
            raise
        except Exception as e:
            status_code = _extract_status_code(e)
            raise GraphClientError(
                f"get_group failed for {group_id}: {e}",
                status_code=status_code
            )

    @retry_on_throttle(max_retries=3, base_backoff=2.0)
    def check_group_membership(self, user_id: str, group_id: str) -> bool:
        """
        Return True if the user is already a member of the group.

        Always called before add_group_member() to keep assignment idempotent.
        Raises GraphClientError with status_code set.
        """
        try:
            members = self._run(
                self._client.groups.by_group_id(group_id).members.get()
            )

            if members and members.value:
                for member in members.value:
                    if member.id == user_id:
                        return True
            return False

        except GraphClientError:
            raise
        except Exception as e:
            status_code = _extract_status_code(e)
            raise GraphClientError(
                f"check_group_membership failed — user={user_id}, group={group_id}: {e}",
                status_code=status_code
            )

    @retry_on_throttle(max_retries=3, base_backoff=2.0)
    def add_group_member(self, user_id: str, group_id: str) -> None:
        """
        Add a user to an Entra ID group.

        Only called after check_group_membership() confirms the user is
        not already a member. Does not guard against duplicates itself.
        Raises GraphClientError with status_code set.
        """
        try:
            ref          = ReferenceCreate()
            ref.odata_id = (
                f"https://graph.microsoft.com/v1.0/directoryObjects/{user_id}"
            )

            self._run(
                self._client.groups.by_group_id(group_id).members.ref.post(ref)
            )

            logger.info(f"Group member added — user={user_id}, group={group_id}")

        except GraphClientError:
            raise
        except Exception as e:
            status_code = _extract_status_code(e)
            raise GraphClientError(
                f"add_group_member failed — user={user_id}, group={group_id}: {e}",
                status_code=status_code
            )

    @retry_on_throttle(max_retries=3, base_backoff=2.0)
    def check_rbac_assignment(
        self,
        user_id: str,
        role_definition_id: str,
        scope: str
    ) -> bool:
        """
        Return True if the RBAC role assignment already exists for this user.

        Always called before create_rbac_assignment() to prevent duplicate
        assignments on retry.
        Raises GraphClientError with status_code set.
        """
        try:
            assignments = self._run(
                self._client.role_management.directory.role_assignments.get()
            )

            if assignments and assignments.value:
                for assignment in assignments.value:
                    if (
                        assignment.principal_id       == user_id
                        and assignment.role_definition_id == role_definition_id
                    ):
                        return True
            return False

        except GraphClientError:
            raise
        except Exception as e:
            status_code = _extract_status_code(e)
            raise GraphClientError(
                f"check_rbac_assignment failed — user={user_id}: {e}",
                status_code=status_code
            )

    @retry_on_throttle(max_retries=3, base_backoff=2.0)
    def create_rbac_assignment(
        self,
        user_id: str,
        role_definition_id: str,
        scope: str
    ) -> None:
        """
        Assign an Entra ID directory role to a user.

        Only called after check_rbac_assignment() confirms the assignment
        does not already exist.

        Inputs:
            user_id            — Entra object ID of the provisioned user
            role_definition_id — ID of the role definition to assign
            scope              — directory scope, typically "/" for tenant-wide

        Raises GraphClientError with status_code set.
        """
        try:
            from msgraph.generated.models.unified_role_assignment import UnifiedRoleAssignment

            assignment                    = UnifiedRoleAssignment()
            assignment.principal_id       = user_id
            assignment.role_definition_id = role_definition_id
            assignment.directory_scope_id = scope

            self._run(
                self._client.role_management.directory.role_assignments.post(assignment)
            )

            logger.info(
                f"RBAC assignment created — user={user_id}, "
                f"role={role_definition_id}, scope={scope}"
            )

        except GraphClientError:
            raise
        except Exception as e:
            status_code = _extract_status_code(e)
            raise GraphClientError(
                f"create_rbac_assignment failed — user={user_id}: {e}",
                status_code=status_code
            )

    @retry_on_throttle(max_retries=3, base_backoff=2.0)
    def assign_pim_group_eligibility(
        self,
        user_id:       str,
        group_id:      str,
        justification: str,
    ) -> dict:
        """
        Add a user as an eligible member of a PIM-enabled security group.

        Uses the Graph privilegedAccess group eligibility API (Entra ID P2).
        The group must already have an eligible Entra role assignment configured
        in PIM — this method only creates the user's eligible membership.

        Returns a dict with schedule_id on success.
        Treats 409 Conflict as success — eligibility already exists (idempotent).

        This method uses raw httpx rather than the SDK because the PIM
        eligibility schedule endpoint is not fully modelled in the SDK.
        Status codes are handled explicitly here rather than relying on
        the decorator, since httpx raises differently from the SDK.

        Raises GraphClientError on failure.
        """
        import json as _json
        import httpx

        endpoint = (
            "https://graph.microsoft.com/v1.0"
            "/identityGovernance/privilegedAccess/group/eligibilityScheduleRequests"
        )

        body = {
            "accessId":    "member",
            "principalId": user_id,
            "groupId":     group_id,
            "action":      "adminAssign",
            "scheduleInfo": {
                "startDateTime": None,
                "expiration": {
                    "type": "noExpiration"
                }
            },
            "justification": justification
        }

        try:
            if self._credential is None:
                raise GraphClientError(
                    "No credential available for PIM HTTP call. "
                    "Ensure JmlGraphClient is constructed via build_graph_client()."
                )

            token = self._credential.get_token("https://graph.microsoft.com/.default")

            response = httpx.post(
                endpoint,
                headers={
                    "Authorization": f"Bearer {token.token}",
                    "Content-Type":  "application/json",
                },
                json=body,
                timeout=30,
            )

            # 409 — eligibility already exists, treat as success
            if response.status_code == 409:
                logger.info(
                    f"PIM eligibility already exists (idempotent) — "
                    f"user={user_id}, group={group_id}"
                )
                return {"schedule_id": "", "already_existed": True}

            if response.status_code not in (200, 201):
                raise GraphClientError(
                    f"PIM eligibility request failed — "
                    f"status={response.status_code}, body={response.text[:300]}",
                    status_code=response.status_code
                )

            data        = response.json()
            schedule_id = data.get("id", "")
            logger.info(
                f"PIM eligibility assigned — user={user_id}, "
                f"group={group_id}, schedule={schedule_id}"
            )
            return {"schedule_id": schedule_id, "already_existed": False}

        except GraphClientError:
            raise
        except Exception as e:
            raise GraphClientError(
                f"assign_pim_group_eligibility failed — "
                f"user={user_id}, group={group_id}: {e}"
            )

    @retry_on_throttle(max_retries=3, base_backoff=2.0)
    def remove_pim_group_eligibility(
        self,
        user_id:       str,
        group_id:      str,
        justification: str,
    ) -> dict:
        """
        Remove a user's eligible membership from a PIM-enabled security group.

        Mirrors assign_pim_group_eligibility() but uses action: "adminRemove".
        Only removes the eligible assignment — active sessions are left to
        expire naturally per ADR-003.

        Returns a dict with schedule_id of the removal request on success.
        Treats 404 as success — eligibility does not exist, nothing to remove
        (idempotent).

        Raises GraphClientError on failure.
        """
        import json as _json
        import httpx

        endpoint = (
            "https://graph.microsoft.com/v1.0"
            "/identityGovernance/privilegedAccess/group/eligibilityScheduleRequests"
        )

        body = {
            "accessId":      "member",
            "principalId":   user_id,
            "groupId":       group_id,
            "action":        "adminRemove",
            "justification": justification,
            "scheduleInfo": {
                "expiration": {
                    "type": "noExpiration"
                }
            },
        }

        try:
            if self._credential is None:
                raise GraphClientError(
                    "No credential available for PIM HTTP call. "
                    "Ensure JmlGraphClient is constructed via build_graph_client()."
                )

            token = self._credential.get_token("https://graph.microsoft.com/.default")

            response = httpx.post(
                endpoint,
                headers={
                    "Authorization": f"Bearer {token.token}",
                    "Content-Type":  "application/json",
                },
                json=body,
                timeout=30,
            )

            # 404 — eligibility does not exist, nothing to remove — treat as success
            if response.status_code == 404:
                logger.info(
                    f"PIM eligibility not found — nothing to remove (idempotent) — "
                    f"user={user_id}, group={group_id}"
                )
                return {"schedule_id": "", "already_absent": True}

            if response.status_code not in (200, 201):
                raise GraphClientError(
                    f"PIM eligibility removal failed — "
                    f"status={response.status_code}, body={response.text[:300]}",
                    status_code=response.status_code
                )

            data        = response.json()
            schedule_id = data.get("id", "")
            logger.info(
                f"PIM eligibility removed — user={user_id}, "
                f"group={group_id}, schedule={schedule_id}"
            )
            return {"schedule_id": schedule_id, "already_absent": False}

        except GraphClientError:
            raise
        except Exception as e:
            raise GraphClientError(
                f"remove_pim_group_eligibility failed — "
                f"user={user_id}, group={group_id}: {e}"
            )

    @retry_on_throttle(max_retries=3, base_backoff=2.0)
    def get_active_pim_sessions(
        self,
        user_id:  str,
        group_id: str,
    ) -> list[dict]:
        """
        Check whether an active PIM session exists for this user and group.

        Queries eligibilityScheduleInstances — these represent active,
        time-bounded PIM activations currently in progress.

        Returns a list of active session dicts. Empty list means no active
        session. Each dict carries group_id, role, and expiry timestamp.

        This is a READ ONLY call. It never cancels or modifies sessions.
        Per ADR-003, active sessions are allowed to expire naturally.
        The result goes to the audit trail only.

        Raises GraphClientError on failure.
        """
        import httpx

        endpoint = (
            f"https://graph.microsoft.com/v1.0"
            f"/identityGovernance/privilegedAccess/group/eligibilityScheduleInstances"
            f"?$filter=principalId eq '{user_id}' and groupId eq '{group_id}'"
        )

        try:
            if self._credential is None:
                raise GraphClientError(
                    "No credential available for PIM HTTP call. "
                    "Ensure JmlGraphClient is constructed via build_graph_client()."
                )

            token = self._credential.get_token("https://graph.microsoft.com/.default")

            response = httpx.get(
                endpoint,
                headers={"Authorization": f"Bearer {token.token}"},
                timeout=30,
            )

            if response.status_code == 404:
                return []

            if response.status_code != 200:
                raise GraphClientError(
                    f"get_active_pim_sessions failed — "
                    f"status={response.status_code}, body={response.text[:300]}",
                    status_code=response.status_code
                )

            data     = response.json()
            sessions = data.get("value", [])

            return [
                {
                    "group_id":   item.get("groupId", ""),
                    "access_id":  item.get("accessId", ""),
                    "end_date":   item.get("scheduleInfo", {})
                                     .get("expiration", {})
                                     .get("endDateTime", ""),
                }
                for item in sessions
            ]

        except GraphClientError:
            raise
        except Exception as e:
            raise GraphClientError(
                f"get_active_pim_sessions failed — "
                f"user={user_id}, group={group_id}: {e}"
            )

    @retry_on_throttle(max_retries=3, base_backoff=2.0)
    def check_package_assignment(
        self,
        user_id: str,
        access_package_id: str,
    ) -> dict | None:
        """
        Check whether the user already has an assignment (pending or
        delivered) for this access package.

        Always called before request_package_assignment() to keep
        assignment idempotent — mirrors check_group_membership().

        Returns the existing assignment dict if found, else None.
        Raises GraphClientError on failure.
        """
        import httpx

        endpoint = (
            "https://graph.microsoft.com/v1.0"
            "/identityGovernance/entitlementManagement/assignments"
            f"?$expand=target&$filter=accessPackage/id eq '{access_package_id}' "
            f"and target/objectid eq '{user_id}'"
        )

        try:
            if self._credential is None:
                raise GraphClientError(
                    "No credential available for entitlement management HTTP call. "
                    "Ensure JmlGraphClient is constructed via build_graph_client()."
                )

            token = self._credential.get_token("https://graph.microsoft.com/.default")

            response = httpx.get(
                endpoint,
                headers={"Authorization": f"Bearer {token.token}"},
                timeout=30,
            )

            if response.status_code != 200:
                raise GraphClientError(
                    f"check_package_assignment failed — "
                    f"status={response.status_code}, body={response.text[:300]}",
                    status_code=response.status_code
                )

            results = response.json().get("value", [])
            return results[0] if results else None

        except GraphClientError:
            raise
        except Exception as e:
            raise GraphClientError(
                f"check_package_assignment failed — "
                f"user={user_id}, package={access_package_id}: {e}"
            )

    @retry_on_throttle(max_retries=3, base_backoff=2.0)
    def request_package_assignment(
        self,
        user_id:                 str,
        access_package_id:       str,
        policy_id:               str,
        request_type:            str = "adminAdd",
        duration_override_days:  int | None = None,
    ) -> dict:
        """
        Submit an assignmentRequest for an access package (ADR-007).

        Only called after check_package_assignment() confirms no existing
        assignment. Returns the request dict — includes id and the initial
        requestState (typically "Submitted", not yet delivered). Caller
        must poll poll_package_assignment_request() to confirm delivery.

        duration_override_days is only set when a rule needs a different
        expiration than its assignment policy already defines. When None,
        no schedule is sent and the policy's own expiration applies —
        confirmed against a real assignment policy pulled from the tenant
        (afterDuration / P180D at the policy level).

        Raises GraphClientError on failure.
        """
        import httpx

        endpoint = (
            "https://graph.microsoft.com/v1.0"
            "/identityGovernance/entitlementManagement/assignmentRequests"
        )

        assignment = {
            "targetId":           user_id,
            "assignmentPolicyId": policy_id,
            "accessPackageId":    access_package_id,
        }

        if duration_override_days is not None:
            assignment["schedule"] = {
                "expiration": {
                    "type":     "afterDuration",
                    "duration": f"P{duration_override_days}D",
                }
            }

        body = {"requestType": request_type, "assignment": assignment}

        try:
            if self._credential is None:
                raise GraphClientError(
                    "No credential available for entitlement management HTTP call. "
                    "Ensure JmlGraphClient is constructed via build_graph_client()."
                )

            token = self._credential.get_token("https://graph.microsoft.com/.default")

            response = httpx.post(
                endpoint,
                headers={
                    "Authorization": f"Bearer {token.token}",
                    "Content-Type":  "application/json",
                },
                json=body,
                timeout=30,
            )

            if response.status_code not in (200, 201):
                raise GraphClientError(
                    f"request_package_assignment failed — "
                    f"status={response.status_code}, body={response.text[:300]}",
                    status_code=response.status_code
                )

            data = response.json()
            logger.info(
                f"Access package assignment requested — user={user_id}, "
                f"package={access_package_id}, request_id={data.get('id', '')}"
            )
            return data

        except GraphClientError:
            raise
        except Exception as e:
            raise GraphClientError(
                f"request_package_assignment failed — "
                f"user={user_id}, package={access_package_id}: {e}"
            )

    @retry_on_throttle(max_retries=3, base_backoff=2.0)
    def get_assignment_request_status(self, request_id: str) -> dict:
        """
        Fetch the current requestState of an assignmentRequest.

        requestState values include: Submitted, PendingApproval,
        Delivering, Delivered, Denied, Failed, Canceled, Scheduled.
        NOT "state" — that field does not exist on this resource; the
        actual property is requestState. Confirmed against Microsoft's
        own documented example response.

        Raises GraphClientError on failure.
        """
        import httpx

        endpoint = (
            "https://graph.microsoft.com/v1.0"
            f"/identityGovernance/entitlementManagement/assignmentRequests/{request_id}"
        )

        try:
            if self._credential is None:
                raise GraphClientError(
                    "No credential available for entitlement management HTTP call. "
                    "Ensure JmlGraphClient is constructed via build_graph_client()."
                )

            token = self._credential.get_token("https://graph.microsoft.com/.default")

            response = httpx.get(
                endpoint,
                headers={"Authorization": f"Bearer {token.token}"},
                timeout=30,
            )

            if response.status_code != 200:
                raise GraphClientError(
                    f"get_assignment_request_status failed — "
                    f"status={response.status_code}, body={response.text[:300]}",
                    status_code=response.status_code
                )

            return response.json()

        except GraphClientError:
            raise
        except Exception as e:
            raise GraphClientError(
                f"get_assignment_request_status failed — request_id={request_id}: {e}"
            )

    @retry_on_throttle(max_retries=3, base_backoff=2.0)
    def get_current_access_package_assignments(self, user_id: str) -> list[dict]:
        """
        Fetch all currently delivered access package assignments for a user.

        Used at Mover Step 1 to establish current-state before delta analysis.
        Replaces the memberOf fetch — the unit of current state is now the
        access package assignment, not raw group membership. Queries the
        assignments resource (state: delivered/expired/...), not
        assignmentRequests (requestState: Submitted/Delivered/...) — these
        are different resources with different field names.

        Returns a list of assignment dicts, each expanded with its
        accessPackage (id, displayName). Raises GraphClientError on failure.
        """
        import httpx

        endpoint = (
            "https://graph.microsoft.com/v1.0"
            "/identityGovernance/entitlementManagement/assignments"
            "?$expand=accessPackage($select=id,displayName),assignmentPolicy($select=id)"
            f"&$filter=target/objectid eq '{user_id}' and state eq 'delivered'"
        )

        try:
            if self._credential is None:
                raise GraphClientError(
                    "No credential available for entitlement management HTTP call. "
                    "Ensure JmlGraphClient is constructed via build_graph_client()."
                )

            token = self._credential.get_token("https://graph.microsoft.com/.default")

            response = httpx.get(
                endpoint,
                headers={"Authorization": f"Bearer {token.token}"},
                timeout=30,
            )

            if response.status_code != 200:
                raise GraphClientError(
                    f"get_current_access_package_assignments failed — "
                    f"status={response.status_code}, body={response.text[:300]}",
                    status_code=response.status_code
                )

            return response.json().get("value", [])

        except GraphClientError:
            raise
        except Exception as e:
            raise GraphClientError(
                f"get_current_access_package_assignments failed — user={user_id}: {e}"
            )

    @retry_on_throttle(max_retries=3, base_backoff=2.0)
    def disable_user(self, user_id: str) -> None:
        """
        Disable an Entra ID account (Leaver Step 2, ADR-015).
 
        Sets accountEnabled to False via a standard user PATCH — same
        SDK pattern as create_user(), just flipping one field. This is
        the first Leaver action for a reason: if anything later in the
        pipeline fails, the account is already locked out (ADR-015).
 
        Raises GraphClientError with status_code set.
        """
        try:
            user_patch = User()
            user_patch.account_enabled = False
 
            self._run(
                self._client.users.by_user_id(user_id).patch(user_patch)
            )
 
            logger.info(f"User disabled — user_id={user_id}")
 
        except GraphClientError:
            raise
        except Exception as e:
            status_code = _extract_status_code(e)
            raise GraphClientError(
                f"disable_user failed — user_id={user_id}: {e}",
                status_code=status_code
            )
 
    @retry_on_throttle(max_retries=3, base_backoff=2.0)
    def revoke_sessions(self, user_id: str) -> None:
        """
        Force sign-out on all devices (Leaver Step 3, ADR-015).
 
        Disabling the account does not invalidate tokens already
        issued — this is what actually ends an active session.
        Uses raw HTTP rather than an SDK action builder, consistent
        with how this file already handles endpoints not fully
        modelled in the SDK (PIM, entitlement management).
 
        Raises GraphClientError on failure.
        """
        import httpx
 
        endpoint = (
            f"https://graph.microsoft.com/v1.0/users/{user_id}/revokeSignInSessions"
        )
 
        try:
            if self._credential is None:
                raise GraphClientError(
                    "No credential available for revokeSignInSessions HTTP "
                    "call. Ensure JmlGraphClient is constructed via "
                    "build_graph_client()."
                )
 
            token = self._credential.get_token("https://graph.microsoft.com/.default")
 
            response = httpx.post(
                endpoint,
                headers={"Authorization": f"Bearer {token.token}"},
                timeout=30,
            )
 
            if response.status_code not in (200, 204):
                raise GraphClientError(
                    f"revoke_sessions failed — status={response.status_code}, "
                    f"body={response.text[:300]}",
                    status_code=response.status_code
                )
 
            logger.info(f"Sessions revoked — user_id={user_id}")
 
        except GraphClientError:
            raise
        except Exception as e:
            raise GraphClientError(
                f"revoke_sessions failed — user_id={user_id}: {e}"
            )
 
    @retry_on_throttle(max_retries=3, base_backoff=2.0)
    def delete_user(self, user_id: str) -> None:
        """
        Soft delete a user (Leaver Step 6, ADR-015).
 
        Standard Graph DELETE — Entra moves the object to the
        deleted-users container, recoverable for 30 days by default.
        This is the one Leaver action treated as non-urgent: everything
        before it in the pipeline has already cut off access, so a
        configurable hold before this call (see leaver_http Step 6) is
        safe.
 
        Raises GraphClientError with status_code set.
        """
        try:
            self._run(self._client.users.by_user_id(user_id).delete())
            logger.info(f"User soft-deleted — user_id={user_id}")
 
        except GraphClientError:
            raise
        except Exception as e:
            status_code = _extract_status_code(e)
            raise GraphClientError(
                f"delete_user failed — user_id={user_id}: {e}",
                status_code=status_code
            )
 
    @retry_on_throttle(max_retries=3, base_backoff=2.0)
    def get_active_pim_assignments_for_user(self, user_id: str) -> list[dict]:
        """
        Discover every active (activated) PIM group assignment for a
        user, tenant-wide — not scoped to a single group.
 
        Queries assignmentScheduleInstances, which reflects live
        activated sessions, as distinct from get_active_pim_sessions()'s
        eligibilityScheduleInstances (eligible-to-activate, not
        activated). Leaver Step 5 (ADR-016) needs the former: it must
        find and terminate whatever is actually live right now, without
        already knowing which groups to check — the Leaver has no
        entitlement resolution (ADR-014) to draw a candidate group list
        from, so discovery has to come from the tenant's live state
        instead of from rules.json.
 
        Returns a list of dicts with group_id, access_id, and end_date.
        Empty list means no active session anywhere. Raises
        GraphClientError on failure.
        """
        import httpx
 
        endpoint = (
            "https://graph.microsoft.com/v1.0"
            "/identityGovernance/privilegedAccess/group/assignmentScheduleInstances"
            f"?$filter=principalId eq '{user_id}'"
        )
 
        try:
            if self._credential is None:
                raise GraphClientError(
                    "No credential available for PIM HTTP call. "
                    "Ensure JmlGraphClient is constructed via build_graph_client()."
                )
 
            token = self._credential.get_token("https://graph.microsoft.com/.default")
 
            response = httpx.get(
                endpoint,
                headers={"Authorization": f"Bearer {token.token}"},
                timeout=30,
            )
 
            if response.status_code == 404:
                return []
 
            if response.status_code != 200:
                raise GraphClientError(
                    f"get_active_pim_assignments_for_user failed — "
                    f"status={response.status_code}, body={response.text[:300]}",
                    status_code=response.status_code
                )
 
            data = response.json().get("value", [])
            return [
                {
                    "group_id":  item.get("groupId", ""),
                    "access_id": item.get("accessId", ""),
                    "end_date":  item.get("scheduleInfo", {})
                                     .get("expiration", {})
                                     .get("endDateTime", ""),
                }
                for item in data
            ]
 
        except GraphClientError:
            raise
        except Exception as e:
            raise GraphClientError(
                f"get_active_pim_assignments_for_user failed — user_id={user_id}: {e}"
            )
 
    @retry_on_throttle(max_retries=3, base_backoff=2.0)
    def cancel_pim_session(
        self,
        user_id:       str,
        group_id:      str,
        justification: str,
    ) -> dict:
        """
        Terminate an active PIM group session immediately (Leaver
        Step 5, ADR-016 — a deliberate departure from ADR-003's
        "let it expire naturally" behaviour on the Mover).
 
        Distinct from remove_pim_group_eligibility(): that call revokes
        future eligibility to activate, this ends a session that is
        activated right now. Posts to assignmentScheduleRequests (the
        active-session resource) rather than eligibilityScheduleRequests.
 
        Returns a dict with schedule_id on success. Treats 404 as
        success — no active session to cancel (idempotent).
 
        Raises GraphClientError on failure.
        """
        import httpx
 
        endpoint = (
            "https://graph.microsoft.com/v1.0"
            "/identityGovernance/privilegedAccess/group/assignmentScheduleRequests"
        )
 
        body = {
            "accessId":      "member",
            "principalId":   user_id,
            "groupId":       group_id,
            "action":        "adminRemove",
            "justification": justification,
        }
 
        try:
            if self._credential is None:
                raise GraphClientError(
                    "No credential available for PIM HTTP call. "
                    "Ensure JmlGraphClient is constructed via build_graph_client()."
                )
 
            token = self._credential.get_token("https://graph.microsoft.com/.default")
 
            response = httpx.post(
                endpoint,
                headers={
                    "Authorization": f"Bearer {token.token}",
                    "Content-Type":  "application/json",
                },
                json=body,
                timeout=30,
            )
 
            if response.status_code == 404:
                logger.info(
                    f"No active PIM session to cancel (idempotent) — "
                    f"user={user_id}, group={group_id}"
                )
                return {"schedule_id": "", "already_absent": True}
 
            if response.status_code not in (200, 201):
                raise GraphClientError(
                    f"cancel_pim_session failed — "
                    f"status={response.status_code}, body={response.text[:300]}",
                    status_code=response.status_code
                )
 
            data        = response.json()
            schedule_id = data.get("id", "")
            logger.info(
                f"PIM session cancelled — user={user_id}, "
                f"group={group_id}, schedule={schedule_id}"
            )
            return {"schedule_id": schedule_id, "already_absent": False}
 
        except GraphClientError:
            raise
        except Exception as e:
            raise GraphClientError(
                f"cancel_pim_session failed — user={user_id}, group={group_id}: {e}"
            )

# Helpers

def _generate_temp_password(employee_id: str) -> str:
    """
    Produce a deterministic temporary password for new user creation.

    The same employee_id always produces the same password. This matters
    for retries: if the user was created but the pipeline crashed before
    recording the object ID, a retry can call create_user() again with
    the same temp password rather than generating a new unknown value.

    Meets Entra ID complexity requirements (uppercase, lowercase, digit,
    special character). force_change_password_next_sign_in is set to True
    in create_user(), so this value is discarded after first login.
    """
    import hashlib
    suffix = hashlib.sha256(employee_id.encode()).hexdigest()[:8].upper()
    return f"JmlTmp!{suffix}1a"