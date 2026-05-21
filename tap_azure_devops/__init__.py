import argparse
import base64
import difflib
import json
import os
import time

import backoff
import requests
import singer
from singer import metadata, metrics

from tap_azure_devops.nango import get_nango_token

logger = singer.get_logger()
session = requests.Session()

BASE_URL = "https://dev.azure.com"
VSAEX_BASE_URL = "https://vsaex.dev.azure.com"
API_VERSION = "7.1"
REQUEST_TIMEOUT = 300
REQUIRED_CONFIG_KEYS = ["start_date", "organization"]

# Set once in main(), used throughout
config_data = {}
is_nango_token = False

KEY_PROPERTIES = {
    "repositories": ["id"],
    "commits": ["commitId"],
    "pull_requests": ["id"],
    "pull_request_commits": ["id"],
    "pull_request_threads": ["id"],
    "pull_request_files": ["id"],
    "pull_request_reviews": ["id"],
    "pull_request_comments": ["id"],
    "work_items": ["id"],
    "builds": ["id"],
    "pipelines": ["id"],
    "user_entitlements": ["id"],
    "group_entitlements": ["id"],
    "teams": ["id"],
    "team_members": ["id"],
}

SUB_STREAMS = {
    "pull_requests": ["pull_request_commits", "pull_request_threads", "pull_request_files", "pull_request_reviews", "pull_request_comments"],
    "teams": ["team_members"],
}

PROJECT_SCOPED_STREAMS = {"repositories", "work_items", "builds", "pipelines", "teams"}
ORG_SCOPED_STREAMS = {"user_entitlements", "group_entitlements"}


# ─── Exceptions ──────────────────────────────────────────────────────────────

class AzureDevOpsException(Exception):
    pass

class BadCredentialsException(AzureDevOpsException):
    pass

class AuthException(AzureDevOpsException):
    pass

class NotFoundException(AzureDevOpsException):
    pass

class BadRequestException(AzureDevOpsException):
    pass

class InternalServerError(AzureDevOpsException):
    pass

class UnprocessableError(AzureDevOpsException):
    pass

class NotModifiedError(AzureDevOpsException):
    pass

class ConflictError(AzureDevOpsException):
    pass

class APIRateLimitExceededError(AzureDevOpsException):
    pass

class RetriableServerError(AzureDevOpsException):
    pass

class DependencyException(Exception):
    pass


ERROR_CODE_EXCEPTION_MAPPING = {
    304: {"raise_exception": NotModifiedError, "message": "Not Modified."},
    400: {"raise_exception": BadRequestException, "message": "The request is missing or has a bad parameter."},
    401: {"raise_exception": BadCredentialsException, "message": "Invalid access token. Please check your credentials."},
    403: {"raise_exception": AuthException, "message": "User doesn't have permission to access the resource."},
    404: {"raise_exception": NotFoundException, "message": "The resource you have specified cannot be found."},
    409: {"raise_exception": ConflictError, "message": "The request could not be completed due to a conflict."},
    422: {"raise_exception": UnprocessableError, "message": "The request was not able to process right now."},
    429: {"raise_exception": APIRateLimitExceededError, "message": "Request rate limit exceeded."},
    500: {"raise_exception": InternalServerError, "message": "An error has occurred at Azure DevOps's end."},
    502: {"raise_exception": RetriableServerError, "message": "Azure DevOps server error."},
}


# ─── Helpers ─────────────────────────────────────────────────────────────────

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), path)


def get_request_timeout():
    return float(config_data.get("request_timeout", REQUEST_TIMEOUT))


def get_auth_header():
    access_token = config_data["access_token"]
    if is_nango_token:
        return {"Authorization": f"Bearer {access_token}"}
    token = base64.b64encode(f":{access_token}".encode()).decode()
    return {"Authorization": f"Basic {token}"}


def raise_for_error(resp, source):
    error_code = resp.status_code
    try:
        response_json = resp.json()
    except Exception:
        response_json = {}

    if error_code == 404:
        logger.warning("404 for %s — resource not found, skipping", source)
        raise NotFoundException(f"404 for {source}")

    message = ERROR_CODE_EXCEPTION_MAPPING.get(error_code, {}).get(
        "message", f"Unknown Error ({error_code}) for {source}"
    )
    exc = ERROR_CODE_EXCEPTION_MAPPING.get(error_code, {}).get(
        "raise_exception", AzureDevOpsException
    )
    raise exc(f"{message} | URL: {resp.url} | Response: {response_json}")


def calculate_wait_time(response):
    retry_after = response.headers.get("Retry-After")
    if retry_after:
        try:
            return int(retry_after)
        except ValueError:
            pass
    reset_header = response.headers.get("X-RateLimit-Reset")
    if reset_header:
        try:
            return max(1, int(reset_header) - int(time.time()) + 5)
        except ValueError:
            pass
    return 60


@backoff.on_exception(
    backoff.expo,
    (
        requests.Timeout,
        requests.ConnectionError,
        ConnectionRefusedError,
        ConnectionResetError,
        APIRateLimitExceededError,
        RetriableServerError,
        InternalServerError,
    ),
    max_tries=10,
    factor=2,
)
def authed_get(source, url, params=None, method="get", json_body=None):
    with metrics.http_request_timer(source) as timer:
        session.headers.update(get_auth_header())
        logger.info("Making %s request to %s", method.upper(), url)
        resp = session.request(
            method=method,
            url=url,
            params=params,
            json=json_body,
            timeout=get_request_timeout(),
        )
        logger.info("Response status: %s", resp.status_code)
        timer.tags[metrics.Tag.http_status_code] = resp.status_code
        if resp.status_code == 429:
            wait = calculate_wait_time(resp)
            logger.info("Rate limited. Waiting %s seconds", wait)
            time.sleep(wait)
            raise APIRateLimitExceededError(f"Rate limit exceeded, waited {wait}s before retry")
        if resp.status_code not in (200, 201):
            raise_for_error(resp, source)
        return resp


def authed_get_all_pages(source, url, params=None):
    params = dict(params or {})
    top = int(params.get("$top", 100))
    skip = 0
    seen_continuation_token = False

    while True:
        r = authed_get(source, url, params)
        yield r

        continuation_token = r.headers.get("x-ms-continuationtoken")
        if continuation_token:
            seen_continuation_token = True
            params["continuationToken"] = continuation_token
            params.pop("$skip", None)
            continue

        if seen_continuation_token:
            break

        try:
            count = r.json().get("count", 0)
            if count < top:
                break
            skip += top
            params["$skip"] = skip
        except (ValueError, AttributeError):
            break


# ─── Config helpers ───────────────────────────────────────────────────────────

def get_all_repos_in_project(project):
    org = config_data["organization"]
    url = f"{BASE_URL}/{org}/{project}/_apis/git/repositories"
    params = {"api-version": API_VERSION}
    repos = []
    try:
        for response in authed_get_all_pages("repositories_discovery", url, params):
            for repo in response.json().get("value", []):
                repos.append(f"{project}/{repo['name']}")
    except NotFoundException:
        logger.warning("Project %s not found during repo discovery", project)
    return repos


def extract_repos_from_config(config):
    repo_paths = []
    repo_paths = list(filter(None, config.get("repository", "").split(" ")))

    repo_paths = [r for r in repo_paths if "/" in r]
    wildcards = [r for r in repo_paths if r.split("/")[1] == "*"]
    if wildcards:
        repo_paths = [r for r in repo_paths if r not in wildcards]
        for wildcard in wildcards:
            project = wildcard.split("/")[0]
            repo_paths.extend(get_all_repos_in_project(project))

    # Deduplicate while preserving order
    return list(dict.fromkeys(repo_paths))


# ─── Schema / catalog helpers ─────────────────────────────────────────────────

def load_schemas():
    schemas = {}
    schemas_path = get_abs_path("schemas")
    for filename in os.listdir(schemas_path):
        if filename.endswith(".json"):
            stream_name = filename.replace(".json", "")
            with open(os.path.join(schemas_path, filename), encoding="utf-8") as f:
                schemas[stream_name] = json.load(f)
    return schemas


def populate_metadata(schema_name, schema):
    mdata = metadata.new()
    mdata = metadata.write(mdata, (), "table-key-properties", KEY_PROPERTIES[schema_name])
    for field_name in schema["properties"].keys():
        if field_name in KEY_PROPERTIES[schema_name]:
            mdata = metadata.write(mdata, ("properties", field_name), "inclusion", "automatic")
        else:
            mdata = metadata.write(mdata, ("properties", field_name), "inclusion", "available")
    return mdata


def get_catalog():
    raw_schemas = load_schemas()
    streams = []
    for schema_name, schema in raw_schemas.items():
        mdata = populate_metadata(schema_name, schema)
        streams.append({
            "stream": schema_name,
            "tap_stream_id": schema_name,
            "schema": schema,
            "metadata": metadata.to_list(mdata),
            "key_properties": KEY_PROPERTIES[schema_name],
        })
    return {"streams": streams}


def get_selected_streams(catalog):
    selected = []
    for stream in catalog["streams"]:
        mdata = metadata.to_map(stream["metadata"])
        if mdata.get((), {}).get("selected") is True:
            selected.append(stream["tap_stream_id"])
    if not selected:
        selected = [s["tap_stream_id"] for s in catalog["streams"]]
    return selected


def validate_dependencies(selected_stream_ids):
    for parent, children in SUB_STREAMS.items():
        if parent not in selected_stream_ids:
            for child in children:
                if child in selected_stream_ids:
                    raise DependencyException(
                        f"Cannot select '{child}' without selecting '{parent}'"
                    )


def get_stream_from_catalog(stream_id, catalog):
    for stream in catalog["streams"]:
        if stream["tap_stream_id"] == stream_id:
            return stream
    return None


def get_bookmark(state, scope, stream, key, default=None):
    return state.get("bookmarks", {}).get(scope, {}).get(stream, {}).get(key, default)


def translate_state(state, catalog, scopes):
    if "bookmarks" not in state:
        state["bookmarks"] = {}
    for scope in scopes:
        if scope not in state["bookmarks"]:
            state["bookmarks"][scope] = {}
    return state


# ─── Sync functions ───────────────────────────────────────────────────────────

def get_all_repositories(schema, project, state, mdata, start_date):
    org = config_data["organization"]
    url = f"{BASE_URL}/{org}/{project}/_apis/git/repositories"
    params = {"api-version": API_VERSION}

    with metrics.record_counter("repositories") as counter:
        try:
            for response in authed_get_all_pages("repositories", url, params):
                extraction_time = singer.utils.now()
                for repo in response.json().get("value", []):
                    repo["_sdc_repository"] = f"{project}/{repo['name']}"
                    repo["_sdc_project"] = project
                    repo["project_id"] = repo.get("project", {}).get("id")
                    repo["project_name"] = repo.get("project", {}).get("name")
                    repo["inserted_at"] = singer.utils.strftime(extraction_time)
                    with singer.Transformer() as transformer:
                        rec = transformer.transform(repo, schema, metadata=metadata.to_map(mdata))
                    singer.write_record("repositories", rec, time_extracted=extraction_time)
                    counter.increment()
        except NotFoundException:
            logger.warning("Project %s not found, skipping repositories", project)
    return state


def get_all_commits(schema, repo_path, state, mdata, start_date):
    project, repo_name = repo_path.split("/", 1)
    org = config_data["organization"]

    bookmark = get_bookmark(state, repo_path, "commits", "since", start_date)
    params = {"api-version": API_VERSION, "$top": 100}
    if bookmark:
        params["searchCriteria.fromDate"] = bookmark

    url = f"{BASE_URL}/{org}/{project}/_apis/git/repositories/{repo_name}/commits"

    max_commit_date = None
    with metrics.record_counter("commits") as counter:
        try:
            for response in authed_get_all_pages("commits", url, params):
                extraction_time = singer.utils.now()
                for commit in response.json().get("value", []):
                    commit["_sdc_repository"] = repo_path
                    commit["inserted_at"] = singer.utils.strftime(extraction_time)
                    commit_date = (
                        (commit.get("author") or {}).get("date")
                        or (commit.get("committer") or {}).get("date")
                    )
                    if commit_date and (max_commit_date is None or commit_date > max_commit_date):
                        max_commit_date = commit_date
                    with singer.Transformer() as transformer:
                        rec = transformer.transform(commit, schema, metadata=metadata.to_map(mdata))
                    singer.write_record("commits", rec, time_extracted=extraction_time)
                    counter.increment()
        except NotFoundException:
            logger.warning("Repository %s not found, skipping commits", repo_path)
    if max_commit_date:
        singer.write_bookmark(state, repo_path, "commits", {"since": max_commit_date})
    return state


def get_pr_commits_for_pr(pr_id, pr_number, repo_path, schema, mdata):
    project, repo_name = repo_path.split("/", 1)
    org = config_data["organization"]
    url = f"{BASE_URL}/{org}/{project}/_apis/git/repositories/{repo_name}/pullRequests/{pr_number}/commits"
    params = {"api-version": API_VERSION}

    try:
        for response in authed_get_all_pages("pull_request_commits", url, params):
            for commit in response.json().get("value", []):
                commit["_sdc_repository"] = repo_path
                commit["pr_id"] = pr_id
                commit["pr_number"] = pr_number
                commit["id"] = f"{pr_id}-{commit['commitId']}"
                commit["inserted_at"] = singer.utils.strftime(singer.utils.now())
                with singer.Transformer() as transformer:
                    rec = transformer.transform(commit, schema, metadata=metadata.to_map(mdata))
                yield rec
    except NotFoundException:
        logger.warning("PR %s commits not found, skipping", pr_number)


def get_pr_threads_for_pr(pr_id, pr_number, repo_path, schema, mdata):
    project, repo_name = repo_path.split("/", 1)
    org = config_data["organization"]
    url = f"{BASE_URL}/{org}/{project}/_apis/git/repositories/{repo_name}/pullRequests/{pr_number}/threads"
    params = {"api-version": API_VERSION}

    try:
        for response in authed_get_all_pages("pull_request_threads", url, params):
            for thread in response.json().get("value", []):
                thread["thread_id"] = thread["id"]
                thread["id"] = f"{pr_id}-{thread['id']}"
                thread["_sdc_repository"] = repo_path
                thread["pr_id"] = pr_id
                thread["pr_number"] = pr_number
                thread["inserted_at"] = singer.utils.strftime(singer.utils.now())
                thread["threadType"] = (thread.get("properties") or {}).get("CodeReviewThreadType", {}).get("$value")
                thread["vote"] = (thread.get("properties") or {}).get("CodeReviewVoteResult", {}).get("$value")
                with singer.Transformer() as transformer:
                    rec = transformer.transform(thread, schema, metadata=metadata.to_map(mdata))
                yield rec
    except NotFoundException:
        logger.warning("PR %s threads not found, skipping", pr_number)


def get_pr_reviews_for_pr(pr_id, pr_number, repo_path, schema, mdata):
    project, repo_name = repo_path.split("/", 1)
    org = config_data["organization"]
    url = f"{BASE_URL}/{org}/{project}/_apis/git/repositories/{repo_name}/pullRequests/{pr_number}/reviewers?api-version={API_VERSION}"

    try:
        response = authed_get("pull_request_reviews", url)
        for reviewer in response.json().get("value", []):
            record = {
                "id": f"{pr_id}-{reviewer.get('id')}",
                "pr_id": pr_id,
                "pr_number": pr_number,
                "reviewer_id": reviewer.get("id"),
                "display_name": reviewer.get("displayName"),
                "unique_name": reviewer.get("uniqueName"),
                "image_url": reviewer.get("imageUrl"),
                "vote": reviewer.get("vote"),
                "has_declined": reviewer.get("hasDeclined"),
                "is_required": reviewer.get("isRequired"),
                "is_flagged": reviewer.get("isFlagged"),
                "reviewer_url": reviewer.get("reviewerUrl"),
                "_sdc_repository": repo_path,
                "inserted_at": singer.utils.strftime(singer.utils.now()),
            }
            with singer.Transformer() as transformer:
                rec = transformer.transform(record, schema, metadata=metadata.to_map(mdata))
            yield rec
    except NotFoundException:
        logger.warning("PR %s reviewers not found, skipping", pr_number)


def get_pr_comments_for_pr(pr_id, pr_number, repo_path, schema, mdata):
    project, repo_name = repo_path.split("/", 1)
    org = config_data["organization"]
    url = f"{BASE_URL}/{org}/{project}/_apis/git/repositories/{repo_name}/pullRequests/{pr_number}/threads?api-version={API_VERSION}"

    try:
        for response in authed_get_all_pages("pull_request_comments", url):
            for thread in response.json().get("value", []):
                thread_id = thread.get("id")
                thread_status = thread.get("status")
                file_path = (thread.get("threadContext") or {}).get("filePath")
                for comment in thread.get("comments", []):
                    author = comment.get("author") or {}
                    record = {
                        "id": f"{pr_id}-{thread_id}-{comment.get('id')}",
                        "pr_id": pr_id,
                        "pr_number": pr_number,
                        "thread_id": thread_id,
                        "comment_id": comment.get("id"),
                        "parent_comment_id": comment.get("parentCommentId"),
                        "content": comment.get("content"),
                        "comment_type": comment.get("commentType"),
                        "published_date": comment.get("publishedDate"),
                        "last_updated_date": comment.get("lastUpdatedDate"),
                        "last_content_updated_date": comment.get("lastContentUpdatedDate"),
                        "author_id": author.get("id"),
                        "author_display_name": author.get("displayName"),
                        "author_unique_name": author.get("uniqueName"),
                        "thread_status": thread_status,
                        "file_path": file_path,
                        "_sdc_repository": repo_path,
                        "inserted_at": singer.utils.strftime(singer.utils.now()),
                    }
                    with singer.Transformer() as transformer:
                        rec = transformer.transform(record, schema, metadata=metadata.to_map(mdata))
                    yield rec
    except NotFoundException:
        logger.warning("PR %s threads not found for comments, skipping", pr_number)


def get_blob_content(repo_path, object_id):
    project, repo_name = repo_path.split("/", 1)
    org = config_data["organization"]
    url = f"{BASE_URL}/{org}/{project}/_apis/git/repositories/{repo_name}/blobs/{object_id}?api-version={API_VERSION}&$format=text"
    try:
        resp = authed_get("blob_content", url)
        return resp.text
    except Exception as e:
        logger.warning("Could not fetch blob %s: %s", object_id, e)
        return None


def get_pr_files_for_pr(pr_id, pr_number, repo_path, schema, mdata):
    project, repo_name = repo_path.split("/", 1)
    org = config_data["organization"]
    base = f"{BASE_URL}/{org}/{project}/_apis/git/repositories/{repo_name}/pullRequests/{pr_number}"

    # Fetch iterations to get the last one
    try:
        iter_resp = authed_get(
            "pull_request_iterations",
            f"{base}/iterations?api-version={API_VERSION}",
        )
    except NotFoundException:
        logger.warning("PR %s iterations not found, skipping files", pr_number)
        return

    iterations = iter_resp.json().get("value", [])
    if not iterations:
        return

    last_iteration = max(iterations, key=lambda it: it["id"])
    last_iteration_id = last_iteration["id"]
    iteration_reason = last_iteration.get("reason")

    # Fetch file changes for the last iteration
    try:
        changes_resp = authed_get(
            "pull_request_files",
            f"{base}/iterations/{last_iteration_id}/changes?api-version={API_VERSION}",
        )
    except NotFoundException:
        logger.warning("PR %s iteration %s changes not found, skipping", pr_number, last_iteration_id)
        return

    for entry in changes_resp.json().get("changeEntries", []):
        item = entry.get("item") or {}
        object_id = item.get("objectId")
        is_folder = item.get("isFolder", False)
        is_deleted = entry.get("changeType") in ("delete", "Delete")

        # Fetch before/after blob content and compute unified diff
        file_content = None
        file_diff = None
        if not is_folder:
            original_object_id = item.get("originalObjectId")
            if object_id and not is_deleted:
                file_content = get_blob_content(repo_path, object_id)
            before = get_blob_content(repo_path, original_object_id) if original_object_id else ""
            after = file_content or ""
            if before or after:
                file_path = item.get("path", "")
                diff_lines = list(difflib.unified_diff(
                    (before or "").splitlines(keepends=True),
                    (after or "").splitlines(keepends=True),
                    fromfile=f"a{file_path}",
                    tofile=f"b{file_path}",
                ))
                file_diff = "".join(diff_lines) if diff_lines else None

        additions, deletions = 0, 0
        if file_diff:
            for line in file_diff.splitlines():
                if line.startswith("+") and not line.startswith("+++"):
                    additions += 1
                elif line.startswith("-") and not line.startswith("---"):
                    deletions += 1

        record = {
            "id": f"{pr_id}-{last_iteration_id}-{entry.get('changeId')}",
            "pr_id": pr_id,
            "pr_number": pr_number,
            "iteration_id": last_iteration_id,
            "change_id": entry.get("changeId"),
            "change_type": entry.get("changeType"),
            "iteration_reason": iteration_reason,
            "original_path": entry.get("originalPath"),
            "file_content": file_content,
            "file_diff": file_diff,
            "additions": additions,
            "deletions": deletions,
            "item": {
                "objectId": item.get("objectId"),
                "originalObjectId": item.get("originalObjectId"),
                "gitObjectType": item.get("gitObjectType"),
                "commitId": item.get("commitId"),
                "path": item.get("path"),
                "isFolder": item.get("isFolder"),
                "url": item.get("url"),
            },
            "_sdc_repository": repo_path,
            "inserted_at": singer.utils.strftime(singer.utils.now()),
        }
        with singer.Transformer() as transformer:
            rec = transformer.transform(record, schema, metadata=metadata.to_map(mdata))
        yield rec


def get_all_pull_requests(schemas, repo_path, state, mdata, start_date):
    project, repo_name = repo_path.split("/", 1)
    org = config_data["organization"]

    bookmark = get_bookmark(state, repo_path, "pull_requests", "since", start_date)
    url = f"{BASE_URL}/{org}/{project}/_apis/git/repositories/{repo_name}/pullrequests"
    seen_pr_numbers = set()

    def emit_pr(pr, extraction_time, counter):
        pr_number = pr.get("pullRequestId")
        if pr_number in seen_pr_numbers:
            return
        seen_pr_numbers.add(pr_number)

        pr["id"] = f"{project}:{repo_name}:{pr_number}"
        pr["pr_number"] = pr_number
        pr["repo_url"] = pr.get("repository", {}).get("remoteUrl")
        pr["browser_url"] = f"https://dev.azure.com/{org}/{project}/_git/{repo_name}/pullrequest/{pr_number}"
        pr["_sdc_repository"] = repo_path
        pr["inserted_at"] = singer.utils.strftime(extraction_time)
        pr_id = pr["id"]

        with singer.Transformer() as transformer:
            rec = transformer.transform(
                pr,
                schemas["pull_requests"],
                metadata=metadata.to_map(mdata["pull_requests"]),
            )
        singer.write_record("pull_requests", rec, time_extracted=extraction_time)
        counter.increment()

        if schemas.get("pull_request_commits"):
            for commit_rec in get_pr_commits_for_pr(
                pr_id, pr_number, repo_path,
                schemas["pull_request_commits"],
                mdata["pull_request_commits"],
            ):
                singer.write_record(
                    "pull_request_commits", commit_rec, time_extracted=extraction_time
                )

        if schemas.get("pull_request_threads"):
            for thread_rec in get_pr_threads_for_pr(
                pr_id, pr_number, repo_path,
                schemas["pull_request_threads"],
                mdata["pull_request_threads"],
            ):
                singer.write_record(
                    "pull_request_threads", thread_rec, time_extracted=extraction_time
                )

        if schemas.get("pull_request_files") and pr.get("status") == "completed":
            for file_rec in get_pr_files_for_pr(
                pr_id, pr_number, repo_path,
                schemas["pull_request_files"],
                mdata["pull_request_files"],
            ):
                singer.write_record(
                    "pull_request_files", file_rec, time_extracted=extraction_time
                )

        if schemas.get("pull_request_reviews"):
            for review_rec in get_pr_reviews_for_pr(
                pr_id, pr_number, repo_path,
                schemas["pull_request_reviews"],
                mdata["pull_request_reviews"],
            ):
                singer.write_record(
                    "pull_request_reviews", review_rec, time_extracted=extraction_time
                )

        if schemas.get("pull_request_comments"):
            for comment_rec in get_pr_comments_for_pr(
                pr_id, pr_number, repo_path,
                schemas["pull_request_comments"],
                mdata["pull_request_comments"],
            ):
                singer.write_record(
                    "pull_request_comments", comment_rec, time_extracted=extraction_time
                )

        singer.write_bookmark(
            state, repo_path, "pull_requests",
            {"since": singer.utils.strftime(extraction_time)},
        )

    with metrics.record_counter("pull_requests") as counter:
        try:
            # Call 1: all PRs created since bookmark
            params = {
                "api-version": API_VERSION,
                "searchCriteria.status": "all",
                "$top": 100,
            }
            if bookmark:
                params["searchCriteria.minTime"] = bookmark

            for response in authed_get_all_pages("pull_requests", url, params):
                extraction_time = singer.utils.now()
                for pr in response.json().get("value", []):
                    emit_pr(pr, extraction_time, counter)

            # Call 2: completed PRs closed since bookmark (catches old PRs closed recently)
            if bookmark:
                closed_params = {
                    "api-version": API_VERSION,
                    "searchCriteria.status": "all",
                    "searchCriteria.queryTimeRangeType": "closed",
                    "searchCriteria.minTime": bookmark,
                    "$top": 100,
                }
                for response in authed_get_all_pages("pull_requests", url, closed_params):
                    extraction_time = singer.utils.now()
                    for pr in response.json().get("value", []):
                        emit_pr(pr, extraction_time, counter)

        except NotFoundException:
            logger.warning("Repository %s not found, skipping pull requests", repo_path)
    return state


def get_all_work_items(schema, project, state, mdata, start_date):
    org = config_data["organization"]
    bookmark = get_bookmark(state, project, "work_items", "since", start_date)
    since_date = bookmark or start_date or "1970-01-01T00:00:00Z"

    wiql_url = f"{BASE_URL}/{org}/{project}/_apis/wit/wiql"
    wiql_query = {
        "query": (
            f"SELECT [System.Id] FROM WorkItems "
            f"WHERE [System.TeamProject] = '{project}' "
            f"AND [System.ChangedDate] >= '{since_date}' "
            f"ORDER BY [System.ChangedDate] DESC"
        )
    }

    try:
        wiql_response = authed_get(
            "work_items_wiql", wiql_url,
            params={"api-version": API_VERSION},
            method="post",
            json_body=wiql_query,
        )
    except (NotFoundException, BadRequestException) as e:
        logger.warning("Could not query work items for project %s: %s", project, e)
        return state

    work_item_refs = wiql_response.json().get("workItems", [])
    if not work_item_refs:
        logger.info("No work items found for project %s since %s", project, since_date)
        return state

    logger.info("Fetching %d work items for project %s", len(work_item_refs), project)

    all_ids = [wi["id"] for wi in work_item_refs]

    wi_fields_list = [
        "System.Id", "System.Rev", "System.WorkItemType", "System.State",
        "System.Title", "System.Description", "System.AreaPath",
        "System.TeamProject", "System.IterationPath", "System.CreatedDate",
        "System.ChangedDate", "System.CreatedBy", "System.ChangedBy",
        "System.AssignedTo", "System.Tags", "System.Parent", "System.BoardColumn",
        "Microsoft.VSTS.Common.Priority", "Microsoft.VSTS.Scheduling.StoryPoints",
        "Microsoft.VSTS.Common.ClosedDate", "Microsoft.VSTS.Scheduling.TargetDate",
    ]

    max_changed_date = None
    with metrics.record_counter("work_items") as counter:
        for i in range(0, len(all_ids), 200):
            batch_ids = all_ids[i:i + 200]
            wi_url = f"{BASE_URL}/{org}/{project}/_apis/wit/workitemsbatch"
            wi_body = {"ids": batch_ids, "fields": wi_fields_list, "errorPolicy": "omit"}
            try:
                response = authed_get("work_items", wi_url, params={"api-version": API_VERSION}, method="post", json_body=wi_body)
                extraction_time = singer.utils.now()
                for wi in response.json().get("value", []):
                    fields = wi.get("fields", {})
                    record = {
                        "id": wi["id"],
                        "url": wi.get("url"),
                        "_sdc_repository": project,
                        "inserted_at": singer.utils.strftime(extraction_time),
                        "system_work_item_type": fields.get("System.WorkItemType"),
                        "system_state": fields.get("System.State"),
                        "system_title": fields.get("System.Title"),
                        "system_description": fields.get("System.Description"),
                        "system_area_path": fields.get("System.AreaPath"),
                        "system_team_project": fields.get("System.TeamProject"),
                        "system_iteration_path": fields.get("System.IterationPath"),
                        "system_created_date": fields.get("System.CreatedDate"),
                        "system_changed_date": fields.get("System.ChangedDate"),
                        "system_created_by_display_name": (fields.get("System.CreatedBy") or {}).get("displayName"),
                        "system_created_by_unique_name": (fields.get("System.CreatedBy") or {}).get("uniqueName"),
                        "system_changed_by_display_name": (fields.get("System.ChangedBy") or {}).get("displayName"),
                        "system_changed_by_unique_name": (fields.get("System.ChangedBy") or {}).get("uniqueName"),
                        "system_assigned_to_display_name": (fields.get("System.AssignedTo") or {}).get("displayName"),
                        "system_assigned_to_unique_name": (fields.get("System.AssignedTo") or {}).get("uniqueName"),
                        "system_tags": fields.get("System.Tags"),
                        "system_parent": fields.get("System.Parent"),
                        "system_board_column": fields.get("System.BoardColumn"),
                        "microsoft_vsts_common_priority": fields.get("Microsoft.VSTS.Common.Priority"),
                        "microsoft_vsts_scheduling_story_points": fields.get("Microsoft.VSTS.Scheduling.StoryPoints"),
                        "microsoft_vsts_common_closed_date": fields.get("Microsoft.VSTS.Common.ClosedDate"),
                        "microsoft_vsts_scheduling_target_date": fields.get("Microsoft.VSTS.Scheduling.TargetDate"),
                    }
                    with singer.Transformer() as transformer:
                        rec = transformer.transform(record, schema, metadata=metadata.to_map(mdata))
                    singer.write_record("work_items", rec, time_extracted=extraction_time)
                    changed_date = fields.get("System.ChangedDate")
                    if changed_date and (max_changed_date is None or changed_date > max_changed_date):
                        max_changed_date = changed_date
                    counter.increment()
            except Exception as e:
                logger.exception("Failed to fetch work items batch for project %s: %s", project, e)
    if max_changed_date:
        singer.write_bookmark(state, project, "work_items", {"since": max_changed_date})
    return state


def get_all_builds(schema, project, state, mdata, start_date):
    org = config_data["organization"]
    bookmark = get_bookmark(state, project, "builds", "since", start_date)

    url = f"{BASE_URL}/{org}/{project}/_apis/build/builds"
    params = {"api-version": API_VERSION, "$top": 1000, "queryOrder": "finishTimeDescending"}
    if bookmark:
        params["minTime"] = bookmark

    max_finish_time = None
    with metrics.record_counter("builds") as counter:
        try:
            for response in authed_get_all_pages("builds", url, params):
                extraction_time = singer.utils.now()
                for build in response.json().get("value", []):
                    build["_sdc_repository"] = project
                    build["inserted_at"] = singer.utils.strftime(extraction_time)
                    finish_time = build.get("finishTime")
                    if finish_time and (max_finish_time is None or finish_time > max_finish_time):
                        max_finish_time = finish_time
                    with singer.Transformer() as transformer:
                        rec = transformer.transform(build, schema, metadata=metadata.to_map(mdata))
                    singer.write_record("builds", rec, time_extracted=extraction_time)
                    counter.increment()
        except NotFoundException:
            logger.warning("Project %s not found, skipping builds", project)
    if max_finish_time:
        singer.write_bookmark(state, project, "builds", {"since": max_finish_time})
    return state


def get_all_pipelines(schema, project, state, mdata, start_date):
    org = config_data["organization"]
    url = f"{BASE_URL}/{org}/{project}/_apis/pipelines"
    params = {"api-version": API_VERSION, "$top": 100}

    with metrics.record_counter("pipelines") as counter:
        try:
            for response in authed_get_all_pages("pipelines", url, params):
                extraction_time = singer.utils.now()
                for pipeline in response.json().get("value", []):
                    pipeline["_sdc_repository"] = project
                    pipeline["inserted_at"] = singer.utils.strftime(extraction_time)
                    with singer.Transformer() as transformer:
                        rec = transformer.transform(pipeline, schema, metadata=metadata.to_map(mdata))
                    singer.write_record("pipelines", rec, time_extracted=extraction_time)
                    counter.increment()
        except NotFoundException:
            logger.warning("Project %s not found, skipping pipelines", project)
    return state


def get_all_user_entitlements(schema, org, state, mdata, start_date):
    url = f"{VSAEX_BASE_URL}/{org}/_apis/userentitlements"
    params = {"api-version": API_VERSION, "top": 100}

    with metrics.record_counter("user_entitlements") as counter:
        while True:
            try:
                response = authed_get("user_entitlements", url, params)
            except NotFoundException:
                logger.warning("User entitlements not found for org %s, skipping", org)
                break

            body = response.json()
            extraction_time = singer.utils.now()

            for member in body.get("items", []):
                user = member.get("user") or {}
                access = member.get("accessLevel") or {}
                record = {
                    "id": member.get("id"),
                    "user_id": user.get("id"),
                    "principal_name": user.get("principalName"),
                    "mail_address": user.get("mailAddress"),
                    "display_name": user.get("displayName"),
                    "descriptor": user.get("descriptor"),
                    "subject_kind": user.get("subjectKind"),
                    "origin": user.get("origin"),
                    "origin_id": user.get("originId"),
                    "domain": user.get("domain"),
                    "is_deleted_in_origin": user.get("isDeletedInOrigin"),
                    "license_display_name": access.get("licenseDisplayName"),
                    "account_license_type": access.get("accountLicenseType"),
                    "licensing_source": access.get("licensingSource"),
                    "assignment_source": access.get("assignmentSource"),
                    "license_status": access.get("status"),
                    "last_accessed_date": member.get("lastAccessedDate"),
                    "date_created": member.get("dateCreated"),
                    "_sdc_organization": org,
                    "inserted_at": singer.utils.strftime(extraction_time),
                }
                with singer.Transformer() as transformer:
                    rec = transformer.transform(record, schema, metadata=metadata.to_map(mdata))
                singer.write_record("user_entitlements", rec, time_extracted=extraction_time)
                counter.increment()

            continuation_token = body.get("continuationToken")
            if not continuation_token:
                break
            params["continuationToken"] = continuation_token

    return state


def get_all_group_entitlements(schema, org, state, mdata, start_date):
    url = f"{VSAEX_BASE_URL}/{org}/_apis/groupentitlements"
    params = {"api-version": API_VERSION}

    with metrics.record_counter("group_entitlements") as counter:
        while True:
            try:
                response = authed_get("group_entitlements", url, params)
            except NotFoundException:
                logger.warning("Group entitlements not found for org %s, skipping", org)
                break

            body = response.json()
            extraction_time = singer.utils.now()

            for entry in body.get("value", []):
                group = entry.get("group") or {}
                access = entry.get("licenseRule") or entry.get("accessLevel") or {}
                record = {
                    "id": entry.get("id"),
                    "group_display_name": group.get("displayName"),
                    "group_description": group.get("description"),
                    "group_origin": group.get("origin"),
                    "group_origin_id": group.get("originId"),
                    "group_subject_kind": group.get("subjectKind"),
                    "group_domain": group.get("domain"),
                    "group_principal_name": group.get("principalName"),
                    "group_descriptor": group.get("descriptor"),
                    "license_display_name": access.get("licenseDisplayName"),
                    "account_license_type": access.get("accountLicenseType"),
                    "licensing_source": access.get("licensingSource"),
                    "assignment_source": access.get("assignmentSource"),
                    "license_status": access.get("status"),
                    "member_count": entry.get("memberCount"),
                    "last_executed": entry.get("lastExecuted"),
                    "_sdc_organization": org,
                    "inserted_at": singer.utils.strftime(extraction_time),
                }
                with singer.Transformer() as transformer:
                    rec = transformer.transform(record, schema, metadata=metadata.to_map(mdata))
                singer.write_record("group_entitlements", rec, time_extracted=extraction_time)
                counter.increment()

            continuation_token = body.get("continuationToken")
            if not continuation_token:
                break
            params["continuationToken"] = continuation_token

    return state


def get_team_members_for_team(team_id, schema, org, project, mdata):
    url = f"{BASE_URL}/{org}/_apis/projects/{project}/teams/{team_id}/members?api-version={API_VERSION}&$top=100"
    params = {}

    try:
        for response in authed_get_all_pages("team_members", url, params):
            for member in response.json().get("value", []):
                identity = member.get("identity") or {}
                record = {
                    "id": f"{team_id}-{identity.get('id')}",
                    "team_id": team_id,
                    "member_id": identity.get("id"),
                    "display_name": identity.get("displayName"),
                    "unique_name": identity.get("uniqueName"),
                    "url": identity.get("url"),
                    "image_url": identity.get("imageUrl"),
                    "descriptor": identity.get("descriptor"),
                    "is_team_admin": member.get("isTeamAdmin"),
                    "_sdc_repository": project,
                    "inserted_at": singer.utils.strftime(singer.utils.now()),
                }
                with singer.Transformer() as transformer:
                    rec = transformer.transform(record, schema, metadata=metadata.to_map(mdata))
                yield rec
    except NotFoundException:
        logger.warning("Members not found for team %s, skipping", team_id)


def get_all_teams(schemas, project, state, mdata, start_date):
    org = config_data["organization"]
    url = f"{BASE_URL}/{org}/_apis/projects/{project}/teams?api-version={API_VERSION}&$top=100"
    params = {}

    with metrics.record_counter("teams") as counter:
        try:
            for response in authed_get_all_pages("teams", url, params):
                extraction_time = singer.utils.now()
                for team in response.json().get("value", []):
                    team["_sdc_repository"] = project
                    team["inserted_at"] = singer.utils.strftime(extraction_time)
                    team_id = team["id"]

                    with singer.Transformer() as transformer:
                        rec = transformer.transform(
                            team, schemas["teams"], metadata=metadata.to_map(mdata["teams"])
                        )
                    singer.write_record("teams", rec, time_extracted=extraction_time)
                    counter.increment()

                    if schemas.get("team_members"):
                        for member_rec in get_team_members_for_team(
                            team_id, schemas["team_members"], org, project, mdata["team_members"]
                        ):
                            singer.write_record(
                                "team_members", member_rec, time_extracted=extraction_time
                            )
        except NotFoundException:
            logger.warning("Teams not found for project %s, skipping", project)
    return state


# ─── Dispatch tables ──────────────────────────────────────────────────────────

SYNC_FUNCTIONS = {
    "repositories": get_all_repositories,
    "commits": get_all_commits,
    "pull_requests": get_all_pull_requests,
    "work_items": get_all_work_items,
    "builds": get_all_builds,
    "pipelines": get_all_pipelines,
    "user_entitlements": get_all_user_entitlements,
    "group_entitlements": get_all_group_entitlements,
    "teams": get_all_teams,
}


# ─── Discover / sync orchestration ───────────────────────────────────────────

def do_discover():
    catalog = get_catalog()
    print(json.dumps(catalog, indent=2))


def do_sync(config, state, catalog):
    start_date = config.get("start_date")
    selected_stream_ids = get_selected_streams(catalog)
    validate_dependencies(selected_stream_ids)

    org = config["organization"]
    repositories = extract_repos_from_config(config)
    projects = list(dict.fromkeys(r.split("/")[0] for r in repositories)) if repositories else []
    state = translate_state(state, catalog, repositories + projects + [org])
    singer.write_state(state)

    for stream in catalog["streams"]:
        stream_id = stream["tap_stream_id"]
        if stream_id not in selected_stream_ids:
            continue
        if stream_id not in SYNC_FUNCTIONS:
            continue

        sub_stream_ids = SUB_STREAMS.get(stream_id, [])
        stream_schemas = {stream_id: stream["schema"]}
        stream_mdata = {stream_id: stream["metadata"]}

        singer.write_schema(stream_id, stream["schema"], stream["key_properties"])

        for sub_id in sub_stream_ids:
            if sub_id in selected_stream_ids:
                sub = get_stream_from_catalog(sub_id, catalog)
                if sub:
                    stream_schemas[sub_id] = sub["schema"]
                    stream_mdata[sub_id] = sub["metadata"]
                    singer.write_schema(sub_id, sub["schema"], sub["key_properties"])

        sync_func = SYNC_FUNCTIONS[stream_id]
        if stream_id in ORG_SCOPED_STREAMS:
            scopes = [org]
        elif stream_id in PROJECT_SCOPED_STREAMS:
            scopes = projects
        else:
            scopes = repositories

        if not scopes:
            logger.warning("Skipping %s — no repositories configured", stream_id)
            continue

        for scope in scopes:
            logger.info("Syncing %s for scope: %s", stream_id, scope)
            if sub_stream_ids:
                state = sync_func(stream_schemas, scope, state, stream_mdata, start_date)
            else:
                state = sync_func(stream["schema"], scope, state, stream["metadata"], start_date)
            singer.write_state(state)


# ─── Entry point ─────────────────────────────────────────────────────────────

@singer.utils.handle_top_exception(logger)
def main():
    global config_data, is_nango_token

    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=str, default="config.json")
    path_args, _ = parser.parse_known_args()

    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
    config_data = args.config

    nango_connection_id = config_data.get("nango_connection_id")
    nango_secret_key = config_data.get("nango_secret_key")

    if nango_connection_id and nango_secret_key:
        logger.info("Fetching access token from Nango...")
        config_data["access_token"] = get_nango_token(config_data)
        is_nango_token = True

    if not config_data.get("access_token"):
        raise BadCredentialsException("No access_token provided. Set it in config or via Nango.")

    if args.discover:
        do_discover()
    else:
        catalog = args.properties if args.properties else get_catalog()
        do_sync(config_data, args.state, catalog)


if __name__ == "__main__":
    main()
