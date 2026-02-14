import os
import shutil
import subprocess
import time
from typing import List

import psutil

from .exceptions import ConfigurationError, NordVpnCliError


def find_nordvpn_executable() -> str:
    """
    Finds the NordVPN CLI executable on Linux.

    Returns:
        Absolute path to the `nordvpn` executable.

    Raises:
        ConfigurationError: If the executable cannot be found in PATH.
    """
    resolved = shutil.which("nordvpn")
    if resolved:
        return resolved

    raise ConfigurationError(
        "Could not find NordVPN CLI ('nordvpn'). Please install NordVPN and ensure the command is in PATH."
    )


class LinuxVpnController:
    """
    Controls NordVPN on Linux using the `nordvpn` CLI.
    """

    def __init__(self, exe_path: str):
        """
        Initializes the controller.

        Args:
            exe_path: Optional CLI executable path/name. If empty, defaults to `nordvpn`.
        """
        candidate = exe_path or "nordvpn"
        resolved = shutil.which(candidate)

        if resolved:
            self.exe_path = resolved
        elif os.path.isabs(candidate) and os.path.exists(candidate):
            self.exe_path = candidate
        else:
            raise ConfigurationError(
                "Could not find NordVPN CLI executable. Ensure 'nordvpn' is installed and in PATH, "
                "or provide a valid custom_exe_path."
            )

    def _run_command(self, args: List[str], timeout: int = 60) -> subprocess.CompletedProcess:
        """
        Executes a NordVPN CLI command.

        Args:
            args: List of command arguments.
            timeout: Command timeout in seconds.

        Returns:
            subprocess.CompletedProcess containing command output.

        Raises:
            ConfigurationError: If the executable is not available.
            NordVpnCliError: If the command fails or times out.
        """
        command = [self.exe_path, *args]

        try:
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=timeout,
                check=False,
            )
        except FileNotFoundError as e:
            raise ConfigurationError(
                "NordVPN CLI not found. Ensure 'nordvpn' is installed and available in PATH."
            ) from e
        except subprocess.TimeoutExpired as e:
            raise NordVpnCliError(
                f"NordVPN CLI command '{' '.join(command)}' timed out after {timeout} seconds."
            ) from e
        except Exception as e:
            raise NordVpnCliError(
                f"Unexpected error while running '{' '.join(command)}': {e}"
            ) from e

        if result.returncode != 0:
            error_message = (result.stderr or result.stdout or "Unknown CLI error").strip()
            raise NordVpnCliError(
                f"NordVPN CLI command '{' '.join(command)}' failed.\nError: {error_message}"
            )

        return result

    def _is_connected(self) -> bool:
        """
        Checks current VPN connection status.

        Returns:
            True if connected, False otherwise.

        Raises:
            NordVpnCliError: If `nordvpn status` fails.
        """
        status = self._run_command(["status"], timeout=20).stdout.lower()
        return "status: connected" in status or (
            "connected" in status and "disconnected" not in status
        )

    def _wait_for_status(self, connected: bool, timeout: int = 45, interval: float = 1.0):
        """
        Waits until VPN reaches desired connection status.

        Args:
            connected: Desired connection state.
            timeout: Maximum wait time in seconds.
            interval: Poll interval in seconds.

        Raises:
            NordVpnCliError: If desired state is not reached in time.
        """
        end_time = time.time() + timeout
        while time.time() < end_time:
            try:
                if self._is_connected() == connected:
                    return
            except NordVpnCliError:
                # Keep polling to tolerate transient daemon/route states.
                pass
            time.sleep(interval)

        state = "connected" if connected else "disconnected"
        raise NordVpnCliError(f"NordVPN did not become {state} within {timeout} seconds.")

    def connect(self, target: str, is_group: bool = False):
        """
        Connects to a specific server or group.

        Args:
            target: The server/group name (e.g., 'de123' or 'P2P').
            is_group: Whether the target represents a group.
        """
        label = "group" if is_group else "server"
        print(f"\x1b[34mConnecting to {label} '{target}'...\x1b[0m")

        self._run_command(["connect", target], timeout=120)
        self._wait_for_status(connected=True)

    def disconnect(self):
        """Disconnects from the VPN."""
        print("\n\x1b[34mDisconnecting from NordVPN...\x1b[0m")
        self._run_command(["disconnect"], timeout=90)
        self._wait_for_status(connected=False)

    def flush_dns_cache(self):
        """
        Flushes DNS cache on Linux using supported system resolver commands.

        Tries, in order:
          1) resolvectl flush-caches
          2) systemd-resolve --flush-caches

        Raises:
            NordVpnCliError: If no command is available or all attempts fail.
        """
        attempts = [
            ["resolvectl", "flush-caches"],
            ["systemd-resolve", "--flush-caches"],
        ]

        errors = []
        for command in attempts:
            if shutil.which(command[0]) is None:
                errors.append(f"{command[0]} not found")
                continue

            try:
                subprocess.run(
                    command,
                    capture_output=True,
                    text=True,
                    check=True,
                    timeout=20,
                )
                return
            except subprocess.CalledProcessError as e:
                errors.append((e.stderr or e.stdout or str(e)).strip())
            except subprocess.TimeoutExpired:
                errors.append(f"{' '.join(command)} timed out")
            except Exception as e:
                errors.append(str(e))

        details = "; ".join(err for err in errors if err)
        raise NordVpnCliError(f"DNS flush failed: {details or 'no supported DNS flush command available'}")

    def close(self, force: bool = False):
        """
        Closes NordVPN processes on Linux.

        Args:
            force: If True, kills processes immediately; otherwise terminates gracefully.
        """
        print("\x1b[34mClosing NordVPN processes...\x1b[0m")
        found = False
        target_names = {"nordvpn", "nordvpnd"}

        for proc in psutil.process_iter(["name"]):
            name = (proc.info.get("name") or "").lower()
            if name in target_names:
                found = True
                try:
                    if force:
                        proc.kill()
                    else:
                        proc.terminate()
                    proc.wait(timeout=5)
                    print(f"\x1b[32m{name} closed.\x1b[0m")
                except psutil.TimeoutExpired:
                    if not force:
                        print(f"\x1b[33m{name} did not exit in time, forcing close.\x1b[0m")
                        proc.kill()
                except Exception as e:
                    print(f"\x1b[91mFailed to close {name}: {e}\x1b[0m")

        if not found:
            print("\x1b[33mNo NordVPN process was running.\x1b[0m")
