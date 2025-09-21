from __future__ import annotations
import base64
import json
import os
import subprocess
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Mapping, Optional
from urllib.parse import quote

import httpx


class GitHubAppError(RuntimeError):
    """Raised when interactions with the GitHub App fail."""


@dataclass
class AuthToken:
    value: str
    scheme: str  # "token" for PAT/installation tokens, "Bearer" for JWT
    source: str


@dataclass
class ClientSettings:
    repository: str
    base: str = "main"
    head_owner: Optional[str] = None
    push_url: Optional[str] = None
    push_remote: Optional[str] = None
    force_push: bool = True
    token: Optional[str] = None
    app_id: Optional[str] = None
    installation_id: Optional[str] = None
    private_key: Optional[str] = None
    api_url: str = "https://api.github.com"
    git_server: str = "https://github.com"
    http_timeout: float = 30.0
    user_agent: str = "multi-ai-orchestrator"
    pr_title_template: Optional[str] = None
    pr_body_template: Optional[str] = None
    pr_body_path: Optional[str] = None
    pr_body_literal: Optional[str] = None
    attestation_upload_url: Optional[str] = None
    attestation_upload_method: str = "PUT"
    attestation_upload_headers: Dict[str, str] = field(default_factory=dict)
    attestation_upload_token: Optional[str] = None
    dry_run: bool = False


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_BASE = "main"
DEFAULT_PUSH_METHOD = "PUT"
DEFAULT_TIMEOUT = 30.0


def push_branch(
    branch: str,
    *,
    attestation_path: Optional[str] = None,
    title: Optional[str] = None,
    body: Optional[str] = None,
) -> None:
    """Push a prepared branch and open a PR using the configured GitHub credentials."""

    if not branch or not branch.strip():
        raise GitHubAppError("branch name is required for push")
    branch = branch.strip()

    settings = _load_settings()
    owner, repo = _split_repository(settings.repository)
    head_owner = settings.head_owner or owner

    token = _obtain_access_token(settings)

    context: Dict[str, Any] = {
        "branch": branch,
        "repository": settings.repository,
        "owner": owner,
        "repo": repo,
        "base": settings.base or DEFAULT_BASE,
        "title": title or "",
        "description": body or "",
        "attestation_path": attestation_path or "",
        "attestation_name": Path(attestation_path).name if attestation_path else "",
        "attestation_relative": _relative_to_repo(attestation_path),
        "attestation_url": "",
    }

    if attestation_path and settings.attestation_upload_url:
        uploaded_url = _upload_attestation(attestation_path, settings, token)
        context["attestation_url"] = uploaded_url or ""

    pr_title = _render_pr_title(settings, title, context)
    context["title"] = pr_title
    pr_body = _resolve_pr_body(settings, body, context)
    context["body"] = pr_body or ""

    if settings.dry_run:
        payload = {
            "branch": branch,
            "repository": settings.repository,
            "base": settings.base,
            "title": pr_title,
            "body": pr_body,
            "attestation": context.get("attestation_url") or attestation_path,
        }
        print("github_app.dry_run " + json.dumps(payload, ensure_ascii=False))
        return

    _push_git_branch(branch, settings, token, owner, repo)
    _ensure_pull_request(branch, pr_title, pr_body, settings, token, owner, repo, head_owner)


def _load_settings() -> ClientSettings:
    config_path = os.environ.get("MULTIAI_GITHUB_CONFIG")
    config_data: Dict[str, Any] = {}
    if config_path:
        path = Path(config_path)
        if not path.exists():
            raise GitHubAppError(f"GitHub config file not found: {config_path}")
        try:
            config_data = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise GitHubAppError(f"Failed to parse GitHub config file '{config_path}': {exc}") from exc

    def _cfg(*keys: str, default: Any = None) -> Any:
        cur: Any = config_data
        for key in keys:
            if not isinstance(cur, Mapping) or key not in cur:
                return default
            cur = cur[key]
        return cur

    def _env(name: str, fallback: Any = None) -> Any:
        return os.environ.get(name, fallback)

    repository = _env(
        "MULTIAI_GITHUB_REPOSITORY",
        _cfg("repository") or _derive_repository_from_git(),
    )
    if not repository:
        raise GitHubAppError(
            "Missing repository configuration; set MULTIAI_GITHUB_REPOSITORY or provide a config file."
        )
    repository = repository.strip()

    base = _env("MULTIAI_GITHUB_BASE", _cfg("base", default=DEFAULT_BASE)) or DEFAULT_BASE
    head_owner = _env("MULTIAI_GITHUB_HEAD_OWNER", _cfg("head_owner"))

    push_url = _env("MULTIAI_GITHUB_PUSH_URL", _cfg("push", "url"))
    push_remote = _env("MULTIAI_GITHUB_PUSH_REMOTE", _cfg("push", "remote"))
    force_push = _coerce_bool(_env("MULTIAI_GITHUB_FORCE_PUSH", _cfg("push", "force", default=True)), default=True)

    token = _env("MULTIAI_GITHUB_TOKEN", _cfg("token"))
    api_url = _env("MULTIAI_GITHUB_API_URL", _cfg("api_url", default="https://api.github.com"))
    git_server = _env("MULTIAI_GITHUB_SERVER_URL", _cfg("git_server", default="https://github.com"))
    http_timeout = _coerce_float(
        _env("MULTIAI_GITHUB_HTTP_TIMEOUT", _cfg("http_timeout", default=DEFAULT_TIMEOUT)),
        default=DEFAULT_TIMEOUT,
    )
    user_agent = _env("MULTIAI_GITHUB_USER_AGENT", _cfg("user_agent", default="multi-ai-orchestrator"))
    dry_run = _coerce_bool(_env("MULTIAI_GITHUB_DRY_RUN", _cfg("dry_run", default=False)), default=False)

    pr_title_template = _env("MULTIAI_GITHUB_PR_TITLE_TEMPLATE", _cfg("pr", "title_template"))
    pr_body_template = _env("MULTIAI_GITHUB_PR_BODY_TEMPLATE", _cfg("pr", "body_template"))
    pr_body_path = _env("MULTIAI_GITHUB_PR_BODY_PATH", _cfg("pr", "body_path"))
    pr_body_literal = _env("MULTIAI_GITHUB_PR_BODY", _cfg("pr", "body"))

    attestation_upload_url = _env("MULTIAI_ATTESTATION_UPLOAD_URL", _cfg("attestation_upload", "url"))
    attestation_upload_method = (
        _env("MULTIAI_ATTESTATION_UPLOAD_METHOD", _cfg("attestation_upload", "method", default=DEFAULT_PUSH_METHOD))
        or DEFAULT_PUSH_METHOD
    )
    attestation_headers = _env("MULTIAI_ATTESTATION_UPLOAD_HEADERS", None)
    if attestation_headers is None:
        headers_cfg = _cfg("attestation_upload", "headers")
    else:
        try:
            headers_cfg = json.loads(attestation_headers)
        except json.JSONDecodeError as exc:
            raise GitHubAppError("MULTIAI_ATTESTATION_UPLOAD_HEADERS must be valid JSON") from exc

    headers: Dict[str, str] = {}
    if isinstance(headers_cfg, Mapping):
        headers = {str(k): str(v) for k, v in headers_cfg.items()}

    attestation_upload_token = _env(
        "MULTIAI_ATTESTATION_UPLOAD_TOKEN", _cfg("attestation_upload", "token")
    )

    app_id = _env("MULTIAI_GITHUB_APP_ID", _cfg("app", "id"))
    installation_id = _env("MULTIAI_GITHUB_INSTALLATION_ID", _cfg("app", "installation_id"))
    private_key_value = _env("MULTIAI_GITHUB_APP_PRIVATE_KEY", _cfg("app", "private_key"))
    private_key_path = _env("MULTIAI_GITHUB_APP_PRIVATE_KEY_PATH", _cfg("app", "private_key_path"))
    private_key = _load_private_key(private_key_value, private_key_path)

    return ClientSettings(
        repository=repository,
        base=base,
        head_owner=head_owner,
        push_url=push_url,
        push_remote=push_remote,
        force_push=force_push,
        token=token,
        app_id=app_id,
        installation_id=installation_id,
        private_key=private_key,
        api_url=api_url,
        git_server=git_server,
        http_timeout=http_timeout,
        user_agent=user_agent,
        pr_title_template=pr_title_template,
        pr_body_template=pr_body_template,
        pr_body_path=pr_body_path,
        pr_body_literal=pr_body_literal,
        attestation_upload_url=attestation_upload_url,
        attestation_upload_method=attestation_upload_method.upper(),
        attestation_upload_headers=headers,
        attestation_upload_token=attestation_upload_token,
        dry_run=dry_run,
    )


def _obtain_access_token(settings: ClientSettings) -> AuthToken:
    if settings.token:
        token_value = settings.token.strip()
        if not token_value:
            raise GitHubAppError("GitHub token is empty; check MULTIAI_GITHUB_TOKEN")
        return AuthToken(token_value, "token", "env")

    if settings.app_id and settings.installation_id and settings.private_key:
        jwt_token = _generate_app_jwt(settings.app_id, settings.private_key)
        headers = {
            "Authorization": f"Bearer {jwt_token}",
            "Accept": "application/vnd.github+json",
            "User-Agent": settings.user_agent,
        }
        url = settings.api_url.rstrip("/") + f"/app/installations/{settings.installation_id}/access_tokens"
        try:
            response = httpx.post(url, headers=headers, timeout=settings.http_timeout)
        except httpx.HTTPError as exc:
            raise GitHubAppError(f"Failed to request installation token: {exc}") from exc
        if response.status_code >= 400:
            raise GitHubAppError(
                f"Installation token request failed ({response.status_code}): {response.text.strip()}"
            )
        token_value = response.json().get("token")
        if not token_value:
            raise GitHubAppError("Installation token response missing 'token'")
        return AuthToken(token_value, "token", "installation")

    raise GitHubAppError(
        "No GitHub credentials provided; set MULTIAI_GITHUB_TOKEN or provide app credentials."
    )


def _generate_app_jwt(app_id: str, private_key: str) -> str:
    now = int(time.time())
    payload = {
        "iat": now - 60,
        "exp": now + 600,
        "iss": app_id,
    }
    header_bytes = json.dumps({"alg": "RS256", "typ": "JWT"}, separators=(",", ":")).encode("utf-8")
    payload_bytes = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    header = _b64url(header_bytes)
    body = _b64url(payload_bytes)
    signing_input = f"{header}.{body}".encode("ascii")

    try:
        with tempfile.NamedTemporaryFile("w", delete=False) as handle:
            handle.write(private_key)
            key_path = handle.name
    except OSError as exc:
        raise GitHubAppError(f"Failed to write temporary key file: {exc}") from exc

    try:
        proc = subprocess.run(
            ["openssl", "dgst", "-sha256", "-sign", key_path],
            input=signing_input,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
    except FileNotFoundError as exc:
        raise GitHubAppError("OpenSSL is required to sign GitHub App JWTs") from exc
    finally:
        try:
            os.unlink(key_path)
        except OSError:
            pass

    if proc.returncode != 0:
        raise GitHubAppError(f"openssl failed to sign JWT: {proc.stderr.decode('utf-8', 'ignore').strip()}")

    signature = _b64url(proc.stdout)
    return f"{header}.{body}.{signature}"


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _push_git_branch(
    branch: str,
    settings: ClientSettings,
    token: AuthToken,
    owner: str,
    repo: str,
) -> None:
    push_target = settings.push_remote
    extra_env = os.environ.copy()
    extra_env.setdefault("GIT_TERMINAL_PROMPT", "0")

    remote_url = None
    if not push_target:
        remote_url = settings.push_url or _build_remote_url(settings, token, owner, repo)
        push_target = remote_url

    cmd = ["git", "push"]
    if settings.force_push:
        cmd.append("--force-with-lease")
    cmd.extend([push_target, f"HEAD:refs/heads/{branch}"])

    try:
        result = subprocess.run(
            cmd,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=extra_env,
            text=True,
        )
    except OSError as exc:
        raise GitHubAppError(f"git push failed: {exc}") from exc

    if result.returncode != 0:
        sanitized_remote = _sanitize_remote(push_target, remote_url)
        stderr = result.stderr.strip()
        stdout = result.stdout.strip()
        message = stderr or stdout or "git push failed"
        raise GitHubAppError(f"git push to {sanitized_remote} failed: {message}")


def _build_remote_url(settings: ClientSettings, token: AuthToken, owner: str, repo: str) -> str:
    server = settings.git_server.rstrip("/")
    encoded_token = quote(token.value, safe="")
    if "://" in server:
        scheme, rest = server.split("://", 1)
        return f"{scheme}://x-access-token:{encoded_token}@{rest}/{owner}/{repo}.git"
    return f"https://x-access-token:{encoded_token}@{server}/{owner}/{repo}.git"


def _sanitize_remote(target: str, raw_remote: Optional[str]) -> str:
    if raw_remote and raw_remote == target:
        return _mask_token(raw_remote)
    return _mask_token(target)


def _mask_token(value: str) -> str:
    if "@" not in value:
        return value
    prefix, rest = value.split("@", 1)
    if ":" in prefix:
        prefix = prefix.split(":", 1)[0] + ":***"
    else:
        prefix = "***"
    return prefix + "@" + rest


def _ensure_pull_request(
    branch: str,
    title: str,
    body: Optional[str],
    settings: ClientSettings,
    token: AuthToken,
    owner: str,
    repo: str,
    head_owner: str,
) -> None:
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": settings.user_agent,
    }
    if token.scheme.lower() == "token":
        headers["Authorization"] = f"token {token.value}"
    else:
        headers["Authorization"] = f"{token.scheme} {token.value}"

    base_url = settings.api_url.rstrip("/")
    pulls_url = f"{base_url}/repos/{owner}/{repo}/pulls"
    payload: Dict[str, Any] = {
        "title": title,
        "head": f"{head_owner}:{branch}",
        "base": settings.base or DEFAULT_BASE,
    }
    if body is not None:
        payload["body"] = body

    try:
        response = httpx.post(pulls_url, json=payload, headers=headers, timeout=settings.http_timeout)
    except httpx.HTTPError as exc:
        raise GitHubAppError(f"Failed to create pull request: {exc}") from exc

    if response.status_code == 422 and _pr_already_exists(response):
        existing = _find_existing_pr(branch, head_owner, owner, repo, headers, settings)
        if existing is None:
            raise GitHubAppError("A pull request already exists for this branch but could not be retrieved")
        update_payload: Dict[str, Any] = {"title": title}
        if body is not None:
            update_payload["body"] = body
        pr_url = f"{base_url}/repos/{owner}/{repo}/pulls/{existing['number']}"
        try:
            update = httpx.patch(pr_url, json=update_payload, headers=headers, timeout=settings.http_timeout)
        except httpx.HTTPError as exc:
            raise GitHubAppError(f"Failed to update existing pull request: {exc}") from exc
        if update.status_code >= 400:
            raise GitHubAppError(
                f"Updating existing pull request failed ({update.status_code}): {update.text.strip()}"
            )
        return

    if response.status_code >= 400:
        raise GitHubAppError(
            f"Creating pull request failed ({response.status_code}): {response.text.strip()}"
        )


def _pr_already_exists(response: httpx.Response) -> bool:
    try:
        data = response.json()
    except ValueError:
        return False
    message = str(data.get("message", "")).lower()
    if "already exists" in message:
        return True
    for error in data.get("errors", []):
        msg = str(error.get("message", "")).lower()
        if "already exists" in msg:
            return True
    return False


def _find_existing_pr(
    branch: str,
    head_owner: str,
    owner: str,
    repo: str,
    headers: Mapping[str, str],
    settings: ClientSettings,
) -> Optional[Mapping[str, Any]]:
    base_url = settings.api_url.rstrip("/")
    params = {"head": f"{head_owner}:{branch}", "state": "open"}
    try:
        response = httpx.get(
            f"{base_url}/repos/{owner}/{repo}/pulls",
            params=params,
            headers=headers,
            timeout=settings.http_timeout,
        )
    except httpx.HTTPError:
        return None
    if response.status_code >= 400:
        return None
    try:
        pulls = response.json()
    except ValueError:
        return None
    if not isinstance(pulls, list) or not pulls:
        return None
    return pulls[0]


def _upload_attestation(
    attestation_path: str,
    settings: ClientSettings,
    token: AuthToken,
) -> Optional[str]:
    path = Path(attestation_path)
    if not path.exists():
        raise GitHubAppError(f"Attestation file not found: {attestation_path}")

    url = settings.attestation_upload_url
    if not url:
        return None

    headers = dict(settings.attestation_upload_headers)
    if settings.attestation_upload_token:
        headers.setdefault("Authorization", settings.attestation_upload_token)
    elif token and token.scheme == "token":
        headers.setdefault("Authorization", f"token {token.value}")
    headers.setdefault("Content-Type", "application/json")

    try:
        response = httpx.request(
            settings.attestation_upload_method or DEFAULT_PUSH_METHOD,
            url,
            headers=headers,
            content=path.read_bytes(),
            timeout=settings.http_timeout,
        )
    except httpx.HTTPError as exc:
        raise GitHubAppError(f"Failed to upload attestation: {exc}") from exc

    if response.status_code >= 400:
        raise GitHubAppError(
            f"Attestation upload failed ({response.status_code}): {response.text.strip()}"
        )

    return response.headers.get("Location") or url


def _render_pr_title(settings: ClientSettings, title: Optional[str], context: Mapping[str, Any]) -> str:
    if settings.pr_title_template:
        return settings.pr_title_template.format_map(_SafeDict(context))
    if title:
        return title
    return f"Update {context.get('branch', '')}".strip()


def _resolve_pr_body(settings: ClientSettings, description: Optional[str], context: Mapping[str, Any]) -> Optional[str]:
    sections = []
    if description and description.strip():
        sections.append(description.strip())
    elif settings.pr_body_literal:
        sections.append(settings.pr_body_literal.strip())

    if settings.pr_body_path:
        path = Path(settings.pr_body_path)
        if not path.is_absolute():
            path = (REPO_ROOT / path).resolve()
        if not path.exists():
            raise GitHubAppError(f"PR body template file not found: {path}")
        sections.append(path.read_text(encoding="utf-8").strip())

    base_body = "\n\n".join(part for part in sections if part)
    if settings.pr_body_template:
        merged_context = dict(context)
        merged_context.setdefault("body", base_body)
        return settings.pr_body_template.format_map(_SafeDict(merged_context))
    return base_body if base_body else None


def _coerce_bool(value: Any, *, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on", "y"}
    return default


def _coerce_float(value: Any, *, default: float) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _load_private_key(value: Optional[str], path_value: Optional[str]) -> Optional[str]:
    raw = value or ""
    if raw:
        raw = raw.strip().replace("\\n", "\n")
        if "-----BEGIN" in raw:
            return raw
        candidate = Path(raw)
        if candidate.exists():
            return candidate.read_text(encoding="utf-8")
    if path_value:
        path = Path(path_value)
        if not path.is_absolute():
            path = (REPO_ROOT / path).resolve()
        if not path.exists():
            raise GitHubAppError(f"GitHub App private key file not found: {path}")
        return path.read_text(encoding="utf-8")
    return None


def _derive_repository_from_git() -> Optional[str]:
    try:
        result = subprocess.run(
            ["git", "config", "--get", "remote.origin.url"],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    except OSError:
        return None
    if result.returncode != 0:
        return None
    url = result.stdout.strip()
    if not url:
        return None
    repo = _extract_repo_from_url(url)
    return repo


def _extract_repo_from_url(url: str) -> Optional[str]:
    cleaned = url.strip()
    if cleaned.endswith(".git"):
        cleaned = cleaned[:-4]
    if cleaned.startswith("git@"):  # git@github.com:owner/repo
        _, _, remainder = cleaned.partition(":")
        return remainder or None
    if "://" in cleaned:
        remainder = cleaned.split("://", 1)[1]
        # strip possible credentials
        if "@" in remainder:
            remainder = remainder.split("@", 1)[1]
        parts = remainder.split("/", 1)
        if len(parts) == 2:
            return parts[1]
    return None


def _split_repository(repository: str) -> tuple[str, str]:
    if "/" not in repository:
        raise GitHubAppError(f"Invalid repository '{repository}'. Expected 'owner/repo'.")
    owner, repo = repository.split("/", 1)
    if not owner or not repo:
        raise GitHubAppError(f"Invalid repository '{repository}'. Expected 'owner/repo'.")
    return owner, repo


def _relative_to_repo(path: Optional[str]) -> str:
    if not path:
        return ""
    try:
        rel = Path(path).resolve().relative_to(REPO_ROOT)
        return str(rel)
    except Exception:
        return path


class _SafeDict(dict):
    def __missing__(self, key: str) -> str:  # pragma: no cover - defensive fallback
        return ""


__all__ = ["push_branch", "GitHubAppError"]
