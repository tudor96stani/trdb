#!/usr/bin/env python3
"""Azure DevOps Boards helper: read, query, create and edit work items via the REST API.

Standard library only. Organization and project come from ../config.json (overridable with
AZDO_ORG / AZDO_PROJECT). The PAT comes from the AZDO_PAT environment variable, or from a
KEY=VALUE file outside the repo (AZDO_ENV_FILE, default ~/.config/azdo/.env). The PAT is never
printed, logged or accepted as a command-line argument.

Output is JSON on stdout. Errors go to stderr with a nonzero exit code.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

API_VERSION = "7.1"
COMMENTS_API_VERSION = "7.1-preview.4"
SKILL_DIR = Path(__file__).resolve().parent.parent
DEFAULT_ENV_FILE = Path.home() / ".config" / "azdo" / ".env"

# Short names accepted by --field, mapped to field reference names.
FIELD_ALIASES = {
    "title": "System.Title",
    "description": "System.Description",
    "state": "System.State",
    "reason": "System.Reason",
    "iteration": "System.IterationPath",
    "tags": "System.Tags",
    "history": "System.History",
    "priority": "Microsoft.VSTS.Common.Priority",
    "acceptance-criteria": "Microsoft.VSTS.Common.AcceptanceCriteria",
    "story-points": "Microsoft.VSTS.Scheduling.StoryPoints",
    "effort": "Microsoft.VSTS.Scheduling.Effort",
    "remaining-work": "Microsoft.VSTS.Scheduling.RemainingWork",
    "repro-steps": "Microsoft.VSTS.TCM.ReproSteps",
}

SUMMARY_FIELDS = [
    "System.Id",
    "System.WorkItemType",
    "System.Title",
    "System.State",
    "System.AssignedTo",
    "System.IterationPath",
    "System.Tags",
]

PARENT_REL = "System.LinkTypes.Hierarchy-Reverse"

# Every item created by this script, and every item whose fields it edits, carries this tag.
AI_TAG = "AI"
# Changing only these does not count as editing the item's content, so it does not add AI_TAG.
NON_CONTENT_FIELDS = {"System.Tags", "System.History"}
# Created in the team's current iteration unless --iteration is given. Tasks follow their
# parent; other types (Feature, Epic, ...) get no iteration unless one is given.
STORY_LEVEL_TYPES = {"user story", "bug"}


class AzdoError(Exception):
    """A configuration or API failure, reported to the user without a traceback."""


def load_config() -> tuple[str, str, str]:
    config: dict = {}
    config_path = SKILL_DIR / "config.json"
    if config_path.is_file():
        config = json.loads(config_path.read_text(encoding="utf-8"))
    org = os.environ.get("AZDO_ORG") or config.get("organization", "")
    project = os.environ.get("AZDO_PROJECT") or config.get("project", "")
    if not org or org.startswith("<") or not project or project.startswith("<"):
        raise AzdoError(
            f"organization/project not configured: set them in {config_path} "
            "or via AZDO_ORG / AZDO_PROJECT"
        )
    # Every created item goes to this area; defaults to the project's root area.
    area = config.get("area") or project
    return org, project, area


def load_pat() -> str:
    pat = os.environ.get("AZDO_PAT", "").strip()
    if pat:
        return pat
    env_file = Path(os.environ.get("AZDO_ENV_FILE") or DEFAULT_ENV_FILE).expanduser()
    if env_file.is_file():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            if key.strip() == "AZDO_PAT":
                pat = value.strip().strip('"').strip("'")
                if pat:
                    return pat
    raise AzdoError(
        f"no PAT found: set AZDO_PAT or put AZDO_PAT=... in {env_file} "
        "(see .env.example in the skill folder)"
    )


class Client:
    def __init__(self, org: str, project: str, pat: str):
        self.org = org
        self.project = project
        self.area = project
        self.org_url = f"https://dev.azure.com/{urllib.parse.quote(org)}"
        self.project_url = f"{self.org_url}/{urllib.parse.quote(project)}"
        token = base64.b64encode(f":{pat}".encode()).decode()
        self._auth = f"Basic {token}"

    def request(
        self,
        method: str,
        url: str,
        body=None,
        content_type: str = "application/json",
        api_version: str = API_VERSION,
    ):
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}api-version={api_version}"
        data = json.dumps(body).encode("utf-8") if body is not None else None
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Authorization", self._auth)
        req.add_header("Accept", "application/json")
        if data is not None:
            req.add_header("Content-Type", content_type)
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = resp.read()
        except urllib.error.HTTPError as e:
            detail = e.read().decode("utf-8", errors="replace")
            try:
                detail = json.loads(detail).get("message", detail)
            except (ValueError, AttributeError):
                pass
            if e.code in (401, 203):
                detail = "authentication failed: PAT missing, expired or lacking Work Items scope"
            raise AzdoError(f"HTTP {e.code} {method} {url.split('?')[0]}: {detail}") from None
        except urllib.error.URLError as e:
            raise AzdoError(f"network error: {e.reason}") from None
        # A sign-in page instead of JSON means the PAT was rejected.
        if raw.lstrip().startswith(b"<"):
            raise AzdoError("authentication failed: received a sign-in page; check the PAT")
        return json.loads(raw) if raw else None

    def me(self) -> str:
        """The PAT owner's account name, as accepted by System.AssignedTo."""
        data = self.request(
            "GET", f"{self.org_url}/_apis/connectionData", api_version="7.1-preview"
        )
        user = data.get("authenticatedUser") or {}
        account = (user.get("properties") or {}).get("Account", {}).get("$value")
        name = account or user.get("providerDisplayName")
        if not name:
            raise AzdoError("could not determine the PAT owner's identity")
        return name

    def current_iteration(self) -> str:
        """Path of the default team's current iteration."""
        data = self.request(
            "GET", f"{self.project_url}/_apis/work/teamsettings/iterations?$timeframe=current"
        )
        iterations = data.get("value") or []
        if not iterations:
            raise AzdoError("the team has no current iteration; pass --iteration explicitly")
        return iterations[0]["path"]

    def work_item_url(self, item_id: int) -> str:
        return f"{self.org_url}/_apis/wit/workItems/{item_id}"

    def get_items(self, ids: list[int], fields: list[str] | None, relations: bool) -> list:
        items = []
        # The API accepts at most 200 ids per call.
        for i in range(0, len(ids), 200):
            chunk = ",".join(str(x) for x in ids[i : i + 200])
            url = f"{self.project_url}/_apis/wit/workitems?ids={chunk}"
            # The API rejects fields and $expand together.
            if relations:
                url += "&$expand=relations"
            elif fields:
                url += "&fields=" + urllib.parse.quote(",".join(fields))
            items.extend(self.request("GET", url)["value"])
        return items


def resolve_field(name: str) -> str:
    return FIELD_ALIASES.get(name.lower(), name)


def parse_field_args(pairs: list[str] | None) -> dict[str, str]:
    fields = {}
    for pair in pairs or []:
        if "=" not in pair:
            raise AzdoError(f"--field expects NAME=VALUE, got {pair!r}")
        name, value = pair.split("=", 1)
        fields[resolve_field(name.strip())] = value
    return fields


def read_text_arg(value: str | None) -> str | None:
    """Values starting with @ are read from a file, so long HTML need not go on the command line."""
    if value is not None and value.startswith("@"):
        return Path(value[1:]).read_text(encoding="utf-8")
    return value


def collect_fields(client: Client, args) -> dict[str, str]:
    fields = parse_field_args(args.field)
    assigned_to = None
    if args.assign_to_me:
        assigned_to = client.me()
    elif args.unassign:
        assigned_to = ""
    direct = {
        "System.Title": args.title,
        "System.Description": read_text_arg(args.description),
        "System.State": getattr(args, "state", None),
        "System.AssignedTo": assigned_to,
        "System.IterationPath": args.iteration,
        "System.Tags": args.tags,
        "Microsoft.VSTS.Common.AcceptanceCriteria": read_text_arg(args.acceptance_criteria),
        "Microsoft.VSTS.Common.Priority": args.priority,
    }
    for ref, value in direct.items():
        if value is not None:
            fields[ref] = value
    return fields


def summarize(item: dict) -> dict:
    f = item.get("fields", {})
    assigned = f.get("System.AssignedTo")
    if isinstance(assigned, dict):
        assigned = assigned.get("uniqueName") or assigned.get("displayName")
    out = {
        "id": item.get("id"),
        "rev": item.get("rev"),
        "type": f.get("System.WorkItemType"),
        "title": f.get("System.Title"),
        "state": f.get("System.State"),
        "assignedTo": assigned,
        "iteration": f.get("System.IterationPath"),
        "tags": f.get("System.Tags"),
    }
    parent = f.get("System.Parent")
    if parent:
        out["parent"] = parent
    return {k: v for k, v in out.items() if v is not None}


def web_url(client: Client, item_id: int) -> str:
    return f"{client.project_url}/_workitems/edit/{item_id}"


def emit(obj) -> None:
    json.dump(obj, sys.stdout, indent=2, ensure_ascii=False)
    sys.stdout.write("\n")


def cmd_get(client: Client, args) -> None:
    fields = [resolve_field(f) for f in args.fields.split(",")] if args.fields else None
    if args.full or fields or args.relations:
        emit(client.get_items(args.ids, fields, args.relations))
    else:
        # System.Parent is only returned when requested explicitly.
        items = client.get_items(args.ids, SUMMARY_FIELDS + ["System.Parent"], relations=False)
        emit([summarize(i) for i in items])


def cmd_query(client: Client, args) -> None:
    if args.wiql:
        wiql = read_text_arg(args.wiql)
    else:
        clauses = ["[System.TeamProject] = @project"]
        if args.type:
            clauses.append(f"[System.WorkItemType] = '{escape_wiql(args.type)}'")
        if args.state:
            clauses.append(f"[System.State] = '{escape_wiql(args.state)}'")
        if args.mine:
            clauses.append("[System.AssignedTo] = @me")
        elif args.unassigned:
            clauses.append("[System.AssignedTo] = ''")
        if args.text:
            clauses.append(f"[System.Title] CONTAINS '{escape_wiql(args.text)}'")
        if args.iteration:
            clauses.append(f"[System.IterationPath] UNDER '{escape_wiql(args.iteration)}'")
        wiql = (
            "SELECT [System.Id] FROM WorkItems WHERE "
            + " AND ".join(clauses)
            + " ORDER BY [System.ChangedDate] DESC"
        )
    url = f"{client.project_url}/_apis/wit/wiql?$top={args.top}"
    result = client.request("POST", url, {"query": wiql})
    ids = [w["id"] for w in result.get("workItems", [])][: args.top]
    if not ids:
        emit([])
        return
    items = client.get_items(ids, SUMMARY_FIELDS + ["System.Parent"], relations=False)
    by_id = {i["id"]: i for i in items}
    emit([summarize(by_id[i]) for i in ids if i in by_id])


def escape_wiql(value: str) -> str:
    return value.replace("'", "''")


def merge_tags(existing: str, add: list[str], remove: list[str]) -> str:
    """Apply removals then additions to a semicolon-separated tag string, case-insensitively."""
    drop = {t.lower() for t in remove}
    tags = [t.strip() for t in existing.split(";") if t.strip() and t.strip().lower() not in drop]
    seen = {t.lower() for t in tags}
    for t in add:
        if t.lower() not in seen:
            tags.append(t)
            seen.add(t.lower())
    return "; ".join(tags)


def build_patch(fields: dict[str, str], op: str = "add") -> list[dict]:
    return [{"op": op, "path": f"/fields/{ref}", "value": value} for ref, value in fields.items()]


def parent_ops(client: Client, parent: int) -> list[dict]:
    return [
        {
            "op": "add",
            "path": "/relations/-",
            "value": {"rel": PARENT_REL, "url": client.work_item_url(parent)},
        }
    ]


def send_patch(client: Client, method: str, url: str, patch: list[dict], dry_run: bool):
    if dry_run:
        emit({"dryRun": True, "method": method, "url": url, "patch": patch})
        return None
    return client.request(method, url, patch, content_type="application/json-patch+json")


def cmd_create(client: Client, args) -> None:
    fields = collect_fields(client, args)
    if "System.Title" not in fields:
        raise AzdoError("create requires --title")
    fields["System.Tags"] = merge_tags(fields.get("System.Tags", ""), [AI_TAG], [])
    fields["System.AreaPath"] = client.area

    wi_type_lower = args.type.lower()
    if wi_type_lower == "task":
        # A task always lives in the same iteration as its parent story.
        if not args.parent:
            raise AzdoError("a Task must be created under a parent (--parent)")
        if "System.IterationPath" in fields:
            raise AzdoError("a Task always takes its parent's iteration; drop --iteration")
        parent = client.get_items([args.parent], ["System.IterationPath"], relations=False)[0]
        fields["System.IterationPath"] = parent["fields"]["System.IterationPath"]
    elif wi_type_lower in STORY_LEVEL_TYPES and "System.IterationPath" not in fields:
        fields["System.IterationPath"] = client.current_iteration()

    patch = build_patch(fields)
    if args.parent:
        patch += parent_ops(client, args.parent)
    wi_type = urllib.parse.quote(args.type)
    url = f"{client.project_url}/_apis/wit/workitems/${wi_type}"
    item = send_patch(client, "POST", url, patch, args.dry_run)
    if item:
        emit({**summarize(item), "url": web_url(client, item["id"])})


def cmd_update(client: Client, args) -> None:
    fields = collect_fields(client, args)
    if args.comment:
        fields["System.History"] = read_text_arg(args.comment)
    content_changed = any(ref not in NON_CONTENT_FIELDS for ref in fields)

    current = None
    if args.add_tag or args.remove_tag or content_changed or args.parent is not None:
        current = client.get_items([args.id], None, relations=True)[0]

    if args.add_tag or args.remove_tag or content_changed:
        current_tags = current["fields"].get("System.Tags", "")
        add = (args.add_tag or []) + ([AI_TAG] if content_changed else [])
        tags = merge_tags(fields.get("System.Tags", current_tags), add, args.remove_tag or [])
        if "System.Tags" in fields or tags != merge_tags(current_tags, [], []):
            fields["System.Tags"] = tags

    patch = []
    if args.rev is not None:
        # Optimistic concurrency: the update fails if someone changed the item since this rev.
        patch.append({"op": "test", "path": "/rev", "value": args.rev})
    patch += build_patch(fields)

    if args.parent is not None and args.parent == current["fields"].get("System.Parent", 0):
        args.parent = None  # already the parent (or already parentless): nothing to relink
    if args.parent is not None:
        relations = current.get("relations") or []
        parent_idx = [i for i, rel in enumerate(relations) if rel.get("rel") == PARENT_REL]
        # Indices shift as relations are removed, so remove the highest first.
        for idx in reversed(parent_idx):
            patch.append({"op": "remove", "path": f"/relations/{idx}"})
        if args.parent > 0:
            patch += parent_ops(client, args.parent)

    if not patch or all(p["op"] == "test" for p in patch):
        raise AzdoError("nothing to update")
    url = f"{client.project_url}/_apis/wit/workitems/{args.id}"
    item = send_patch(client, "PATCH", url, patch, args.dry_run)
    if item:
        emit({**summarize(item), "url": web_url(client, item["id"])})


def cmd_comments(client: Client, args) -> None:
    url = f"{client.project_url}/_apis/wit/workItems/{args.id}/comments?$top={args.top}"
    result = client.request("GET", url, api_version=COMMENTS_API_VERSION)
    emit(
        [
            {
                "id": c.get("id"),
                "author": (c.get("createdBy") or {}).get("uniqueName"),
                "date": c.get("createdDate"),
                "text": c.get("text"),
            }
            for c in result.get("comments", [])
        ]
    )


def cmd_types(client: Client, args) -> None:
    if args.type:
        wi_type = urllib.parse.quote(args.type)
        states = client.request(
            "GET", f"{client.project_url}/_apis/wit/workitemtypes/{wi_type}/states"
        )
        fields = client.request(
            "GET", f"{client.project_url}/_apis/wit/workitemtypes/{wi_type}/fields"
        )
        emit(
            {
                "type": args.type,
                "states": [
                    {"name": s["name"], "category": s.get("category")} for s in states["value"]
                ],
                "fields": [
                    {
                        "name": f.get("name"),
                        "referenceName": f.get("referenceName"),
                        "required": f.get("alwaysRequired", False),
                    }
                    for f in fields["value"]
                ],
            }
        )
    else:
        result = client.request("GET", f"{client.project_url}/_apis/wit/workitemtypes")
        emit([t["name"] for t in result["value"] if not t.get("isDisabled")])


def add_field_options(p: argparse.ArgumentParser, with_state: bool) -> None:
    p.add_argument("--title")
    p.add_argument("--description", help="HTML; prefix with @ to read from a file")
    if with_state:
        p.add_argument("--state")
    assign = p.add_mutually_exclusive_group()
    assign.add_argument("--assign-to-me", action="store_true", help="assign to the PAT owner")
    assign.add_argument("--unassign", action="store_true")
    p.add_argument("--iteration", help="iteration path, e.g. Project\\Sprint 5")
    p.add_argument("--tags", help="replaces all tags; semicolon-separated")
    p.add_argument("--acceptance-criteria", help="HTML; prefix with @ to read from a file")
    p.add_argument("--priority", type=int)
    p.add_argument(
        "--field",
        action="append",
        metavar="NAME=VALUE",
        help="any field by reference name or alias (repeatable)",
    )
    p.add_argument("--dry-run", action="store_true", help="print the patch without sending it")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("get", help="read work items by id")
    p.add_argument("ids", type=int, nargs="+")
    p.add_argument("--fields", help="comma-separated field names or aliases")
    p.add_argument("--relations", action="store_true", help="include links (all fields)")
    p.add_argument("--full", action="store_true", help="all fields, not just a summary")
    p.set_defaults(func=cmd_get)

    p = sub.add_parser("query", help="find work items (filters or raw WIQL)")
    p.add_argument("--wiql", help="raw WIQL; prefix with @ to read from a file")
    p.add_argument("--type")
    p.add_argument("--state")
    assignee = p.add_mutually_exclusive_group()
    assignee.add_argument("--mine", action="store_true", help="assigned to the PAT owner")
    assignee.add_argument("--unassigned", action="store_true")
    p.add_argument("--text", help="title contains")
    p.add_argument("--iteration", help="iteration path (includes children)")
    p.add_argument("--top", type=int, default=50)
    p.set_defaults(func=cmd_query)

    p = sub.add_parser("create", help="create a work item")
    p.add_argument("--type", required=True, help='work item type, e.g. "User Story", Bug, Task')
    p.add_argument("--parent", type=int, help="parent work item id")
    add_field_options(p, with_state=True)
    p.set_defaults(func=cmd_create)

    p = sub.add_parser("update", help="edit a work item")
    p.add_argument("id", type=int)
    p.add_argument("--comment", help="add a discussion comment; prefix with @ to read a file")
    p.add_argument("--add-tag", action="append", metavar="TAG")
    p.add_argument("--remove-tag", action="append", metavar="TAG")
    p.add_argument("--parent", type=int, help="set parent id; 0 removes the parent")
    p.add_argument("--rev", type=int, help="fail if the item changed since this revision")
    add_field_options(p, with_state=True)
    p.set_defaults(func=cmd_update)

    p = sub.add_parser("comments", help="read discussion comments")
    p.add_argument("id", type=int)
    p.add_argument("--top", type=int, default=20)
    p.set_defaults(func=cmd_comments)

    p = sub.add_parser("types", help="list work item types, or states and fields of one type")
    p.add_argument("type", nargs="?")
    p.set_defaults(func=cmd_types)

    return parser


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    args = build_parser().parse_args()
    try:
        org, project, area = load_config()
        client = Client(org, project, load_pat())
        client.area = area
        args.func(client, args)
    except AzdoError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
