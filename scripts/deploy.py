#!/usr/bin/env python3
"""Install, update, or remove p2p-transfer's rendezvousd on Ubuntu 24+.

Idempotent end-to-end: every step checks current state before acting, so this
script is safe to re-run any time to pull the latest branch, rebuild, and
restart the service — or to wipe everything cleanly.

Usage:
    sudo python3 deploy.py install   <dest> [--branch <name>] [--prune-build]
    sudo python3 deploy.py uninstall          [--purge-repo <dest>]
    sudo python3 deploy.py clean-build <dest>

Examples:
    sudo python3 deploy.py install /opt/p2p
    sudo python3 deploy.py install /opt/p2p --branch main --prune-build
    sudo python3 deploy.py clean-build /opt/p2p
    sudo python3 deploy.py uninstall --purge-repo /opt/p2p

Notes:
    * Builds as $SUDO_USER when possible (so cargo state lives under the
      invoking user's HOME), else as root.
    * `--prune-build` removes <dest>/target/ after a successful install to
      reclaim disk (a small VPS rebuild needs ~1.5 GB during compilation but
      only the 5 MB installed binary at /usr/local/bin afterwards).
    * A later `install` run is robust to a missing target/ — cargo rebuilds
      from scratch, the resulting binary's SHA256 is compared against the
      installed copy, and the service is only restarted if it actually
      changed.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import pwd
import shlex
import shutil
import subprocess
import sys
from pathlib import Path

# ---- configuration ----------------------------------------------------------

REPO_URL = "https://github.com/cdcseacave/P2PFileTransfer.git"
INSTALL_PATH = Path("/usr/local/bin/rendezvousd")
SERVICE_USER = "rendezvous"
SERVICE_NAME = "rendezvousd"
SERVICE_PATH = Path(f"/etc/systemd/system/{SERVICE_NAME}.service")
LISTEN_TCP = 14570
LISTEN_UDP = 14571
MAX_RELAY_MBPS = 50

APT_PACKAGES = ["build-essential", "pkg-config", "curl", "git", "ca-certificates"]

SERVICE_UNIT = f"""[Unit]
Description=p2p-transfer rendezvous server
After=network-online.target
Wants=network-online.target

[Service]
ExecStart={INSTALL_PATH} --bind 0.0.0.0:{LISTEN_TCP} --relay-bind 0.0.0.0:{LISTEN_UDP} --max-relay-mbps {MAX_RELAY_MBPS}
User={SERVICE_USER}
Group={SERVICE_USER}
Restart=on-failure
RestartSec=3s

NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true
PrivateDevices=true
ProtectKernelTunables=true
ProtectKernelModules=true
ProtectControlGroups=true
RestrictAddressFamilies=AF_INET AF_INET6
LockPersonality=true
MemoryDenyWriteExecute=true
RestrictNamespaces=true
RestrictRealtime=true
SystemCallArchitectures=native

[Install]
WantedBy=multi-user.target
"""

# ---- pretty output ----------------------------------------------------------

def info(msg: str) -> None: print(f"\033[36m[..]\033[0m {msg}", flush=True)
def ok(msg: str)   -> None: print(f"\033[32m[ok]\033[0m {msg}", flush=True)
def warn(msg: str) -> None: print(f"\033[33m[!!]\033[0m {msg}", flush=True)
def err(msg: str)  -> None: print(f"\033[31m[xx]\033[0m {msg}", flush=True)

# ---- subprocess helpers -----------------------------------------------------

def run(cmd, *, check=True, capture=False, cwd=None, env=None):
    return subprocess.run(
        cmd, check=check, text=True, capture_output=capture,
        cwd=str(cwd) if cwd else None, env=env,
    )

def run_as(user: str, cmd, *, cwd: Path | None = None):
    """Run a command as `user` with a login env so PATH picks up ~/.cargo/bin."""
    quoted = " ".join(shlex.quote(a) for a in cmd)
    prefix = f"cd {shlex.quote(str(cwd))} && " if cwd else ""
    return run(["sudo", "-u", user, "-H", "bash", "-lc", prefix + quoted])

def sha256(path: Path) -> str | None:
    if not path.exists():
        return None
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()

def require_root() -> None:
    if os.geteuid() != 0:
        err("must run as root — try: sudo python3 deploy.py <command> ...")
        sys.exit(1)

def pick_build_user() -> str:
    name = os.environ.get("SUDO_USER")
    if name and name != "root":
        return name
    return "root"

# ---- install steps ----------------------------------------------------------

def check_ubuntu() -> None:
    rel = Path("/etc/os-release")
    if not rel.exists():
        err("/etc/os-release missing — refusing to continue")
        sys.exit(1)
    data = {}
    for line in rel.read_text().splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            data[k] = v.strip('"')
    distro = data.get("ID", "")
    version = data.get("VERSION_ID", "0")
    if distro != "ubuntu":
        warn(f"distro is {distro!r}, not ubuntu — proceeding anyway")
        return
    try:
        major = int(version.split(".")[0])
    except ValueError:
        major = 0
    if major < 24:
        warn(f"Ubuntu {version} detected — script targets 24.04+, proceeding")
    else:
        ok(f"Ubuntu {version}")

def ensure_apt_packages() -> None:
    missing = []
    for pkg in APT_PACKAGES:
        r = run(["dpkg", "-s", pkg], check=False, capture=True)
        if r.returncode != 0:
            missing.append(pkg)
    if not missing:
        ok("apt packages already installed")
        return
    info(f"installing apt packages: {' '.join(missing)}")
    run(["apt-get", "update"])
    run(["apt-get", "install", "-y", *missing])
    ok("apt packages installed")

def ensure_rust(build_user: str) -> Path:
    home = Path(pwd.getpwnam(build_user).pw_dir)
    cargo = home / ".cargo" / "bin" / "cargo"
    if cargo.exists():
        ok(f"rust toolchain already present ({cargo})")
        run_as(build_user, ["bash", "-lc", "rustup update stable >/dev/null 2>&1 || true"])
        return cargo
    info(f"installing rust toolchain for user {build_user}")
    run_as(build_user, [
        "bash", "-lc",
        "curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs "
        "| sh -s -- -y --default-toolchain stable --profile minimal",
    ])
    if not cargo.exists():
        err("rust install reported success but cargo binary not found")
        sys.exit(1)
    ok("rust toolchain installed")
    return cargo

def ensure_repo(dest: Path, branch: str, build_user: str) -> None:
    if not dest.exists():
        info(f"creating {dest}")
        dest.parent.mkdir(parents=True, exist_ok=True)
        run(["install", "-d", "-o", build_user, "-g", build_user, str(dest)])
    git_dir = dest / ".git"
    if not git_dir.exists():
        info(f"cloning {REPO_URL} into {dest}")
        run_as(build_user, ["git", "clone", REPO_URL, str(dest)])
    else:
        info(f"updating existing checkout at {dest}")
        run_as(build_user, ["git", "fetch", "--all", "--prune"], cwd=dest)
    info(f"checking out branch '{branch}'")
    run_as(build_user, ["git", "checkout", branch], cwd=dest)
    run_as(build_user, ["git", "pull", "--ff-only", "origin", branch], cwd=dest)
    ok(f"repo on branch {branch}")

def cargo_build(dest: Path, build_user: str, cargo: Path) -> Path:
    target_dir = dest / "target" / "release"
    if not target_dir.exists():
        info("no target/release/ yet — full rebuild from scratch (several minutes)")
    else:
        info("building release binary (incremental — fast if nothing changed)")
    run_as(build_user, [
        str(cargo), "build", "--release",
        "-p", "p2p-rendezvous", "--bin", "rendezvousd",
    ], cwd=dest)
    out = target_dir / "rendezvousd"
    if not out.exists():
        err(f"build succeeded but binary not at {out}")
        sys.exit(1)
    ok(f"built {out}")
    return out

def ensure_service_user() -> None:
    try:
        pwd.getpwnam(SERVICE_USER)
        ok(f"service user '{SERVICE_USER}' exists")
    except KeyError:
        info(f"creating service user '{SERVICE_USER}'")
        run([
            "useradd", "--system", "--no-create-home",
            "--shell", "/usr/sbin/nologin", SERVICE_USER,
        ])
        ok("service user created")

def install_binary(src: Path) -> bool:
    """Install the binary if it differs from what's already on disk. Returns True if changed."""
    if sha256(src) == sha256(INSTALL_PATH):
        ok(f"{INSTALL_PATH} already up to date")
        return False
    info(f"installing binary to {INSTALL_PATH}")
    run(["install", "-m", "0755", str(src), str(INSTALL_PATH)])
    ok("binary installed/updated")
    return True

def install_service_unit() -> bool:
    """Write the unit file if missing or differs. Returns True if changed."""
    current = SERVICE_PATH.read_text() if SERVICE_PATH.exists() else None
    if current == SERVICE_UNIT:
        ok(f"{SERVICE_PATH} already up to date")
        return False
    info(f"writing {SERVICE_PATH}")
    SERVICE_PATH.write_text(SERVICE_UNIT)
    SERVICE_PATH.chmod(0o644)
    ok("systemd unit written")
    return True

def systemd_enable_and_start(unit_changed: bool, binary_changed: bool) -> None:
    if unit_changed:
        info("reloading systemd")
        run(["systemctl", "daemon-reload"])
    run(["systemctl", "enable", SERVICE_NAME], capture=True)
    is_active = run(["systemctl", "is-active", "--quiet", SERVICE_NAME], check=False).returncode == 0
    if not is_active:
        info(f"starting {SERVICE_NAME}")
        run(["systemctl", "start", SERVICE_NAME])
        ok("service started")
    elif unit_changed or binary_changed:
        info(f"restarting {SERVICE_NAME} (binary or unit changed)")
        run(["systemctl", "restart", SERVICE_NAME])
        ok("service restarted")
    else:
        ok(f"{SERVICE_NAME} already running and up to date")

def configure_firewall() -> None:
    if not shutil.which("ufw"):
        warn("ufw not installed — skipping firewall config")
        return
    status = run(["ufw", "status"], check=False, capture=True)
    if status.returncode != 0:
        warn("`ufw status` failed — skipping firewall config")
        return
    if "Status: active" not in status.stdout:
        warn("ufw installed but inactive — skipping firewall config")
        return
    for rule in (f"{LISTEN_TCP}/tcp", f"{LISTEN_UDP}/udp"):
        if rule in status.stdout:
            ok(f"ufw rule for {rule} already present")
            continue
        info(f"adding ufw rule: allow {rule}")
        run(["ufw", "allow", rule])

def report_status() -> None:
    print()
    info("final service status:")
    run(["systemctl", "--no-pager", "--full", "status", SERVICE_NAME], check=False)

# ---- clean-build / uninstall ------------------------------------------------

def clean_build(dest: Path, build_user: str) -> None:
    """Remove <dest>/target/ to reclaim disk. The installed binary at
    /usr/local/bin keeps the service running; a later `install` will simply
    rebuild target/ from scratch and the SHA256 compare will skip the
    pointless restart when nothing has actually changed."""
    target = dest / "target"
    if not target.exists():
        ok(f"{target} already absent — nothing to clean")
        return
    info(f"removing {target} (build artifacts)")
    run_as(build_user, ["rm", "-rf", str(target)])
    ok("build artifacts cleaned")

def uninstall(purge_repo: Path | None) -> None:
    unit_files = run(["systemctl", "list-unit-files", f"{SERVICE_NAME}.service"],
                     check=False, capture=True)
    if SERVICE_NAME in unit_files.stdout:
        info(f"stopping {SERVICE_NAME}")
        run(["systemctl", "stop", SERVICE_NAME], check=False)
        info(f"disabling {SERVICE_NAME}")
        run(["systemctl", "disable", SERVICE_NAME], check=False, capture=True)
        ok("service stopped + disabled")
    else:
        ok(f"{SERVICE_NAME} not registered with systemd — nothing to stop")

    if SERVICE_PATH.exists():
        info(f"removing {SERVICE_PATH}")
        SERVICE_PATH.unlink()
        run(["systemctl", "daemon-reload"])
        ok("systemd unit removed")
    else:
        ok(f"{SERVICE_PATH} already absent")

    if INSTALL_PATH.exists():
        info(f"removing {INSTALL_PATH}")
        INSTALL_PATH.unlink()
        ok("binary removed")
    else:
        ok(f"{INSTALL_PATH} already absent")

    try:
        pwd.getpwnam(SERVICE_USER)
        info(f"removing service user '{SERVICE_USER}'")
        run(["userdel", SERVICE_USER], check=False)
        ok("service user removed")
    except KeyError:
        ok(f"service user '{SERVICE_USER}' already absent")

    if shutil.which("ufw"):
        for rule in (f"{LISTEN_TCP}/tcp", f"{LISTEN_UDP}/udp"):
            r = run(["ufw", "delete", "allow", rule], check=False, capture=True)
            if r.returncode == 0:
                ok(f"ufw rule {rule} removed")

    if purge_repo is not None:
        repo = purge_repo.resolve()
        if repo.exists():
            info(f"purging repo clone at {repo}")
            shutil.rmtree(repo)
            ok("repo clone removed")
        else:
            ok(f"{repo} already absent")
    else:
        info("repo clone kept (pass --purge-repo <dest> to remove it as well)")

    print()
    ok("uninstall complete")

# ---- commands ---------------------------------------------------------------

def cmd_install(args: argparse.Namespace) -> None:
    require_root()
    check_ubuntu()
    build_user = pick_build_user()
    info(f"build identity: {build_user}")

    ensure_apt_packages()
    cargo = ensure_rust(build_user)
    dest = args.dest.resolve()
    ensure_repo(dest, args.branch, build_user)
    binary = cargo_build(dest, build_user, cargo)

    ensure_service_user()
    binary_changed = install_binary(binary)
    unit_changed = install_service_unit()
    systemd_enable_and_start(unit_changed, binary_changed)
    configure_firewall()
    report_status()

    if args.prune_build:
        print()
        clean_build(dest, build_user)

    print()
    ok("install done")

def cmd_uninstall(args: argparse.Namespace) -> None:
    require_root()
    uninstall(args.purge_repo)

def cmd_clean_build(args: argparse.Namespace) -> None:
    require_root()
    build_user = pick_build_user()
    clean_build(args.dest.resolve(), build_user)

# ---- main -------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Install / update / remove rendezvousd on Ubuntu 24+",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    sub = ap.add_subparsers(dest="command", required=True)

    p_install = sub.add_parser("install", help="install or update rendezvousd")
    p_install.add_argument("dest", type=Path, help="where to clone the repo (e.g. /opt/p2p)")
    p_install.add_argument("--branch", default="develop", help="git branch to deploy (default: develop)")
    p_install.add_argument("--prune-build", action="store_true",
                           help="remove <dest>/target/ after a successful install to save disk")
    p_install.set_defaults(func=cmd_install)

    p_uninstall = sub.add_parser("uninstall", help="stop service and remove binary, unit, user")
    p_uninstall.add_argument("--purge-repo", type=Path, default=None, metavar="<dest>",
                             help="also delete the repo clone at the given path")
    p_uninstall.set_defaults(func=cmd_uninstall)

    p_clean = sub.add_parser("clean-build", help="remove <dest>/target/ to reclaim disk")
    p_clean.add_argument("dest", type=Path, help="repo path whose target/ should be wiped")
    p_clean.set_defaults(func=cmd_clean_build)

    args = ap.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
