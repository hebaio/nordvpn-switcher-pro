import os
import subprocess
import time
import psutil
from typing import List

from .exceptions import ConfigurationError, NordVpnCliError

_CLI_IS_READY = False  # Tracks if the NordVPN CLI is ready for commands.


def find_nordvpn_executable() -> str:
    """
    Finds the path to the NordVPN executable on Windows.

    Checks a list of common installation directories.

    Returns:
        The full path to NordVPN.exe.

    Raises:
        ConfigurationError: If the executable cannot be found.
    """
    potential_paths = [
        os.path.join(os.environ["ProgramFiles"], "NordVPN", "NordVPN.exe"),
        os.path.join(os.environ["ProgramFiles(x86)"], "NordVPN", "NordVPN.exe"),
    ]

    for path in potential_paths:
        if os.path.exists(path):
            return path

    raise ConfigurationError(
        "Could not find NordVPN.exe. Please install NordVPN in a standard directory "
        "or provide the correct path in VpnSwitcher(custom_exe_path='C:/Path/To/NordVPN.exe')."
    )


class WindowsVpnController:
    """
    Controls the NordVPN Windows client via its command-line interface.
    """
    def __init__(self, exe_path: str):
        """
        Initializes the controller with the path to NordVPN.exe.

        Args:
            exe_path: The full path to the NordVPN executable.
        """
        if not os.path.exists(exe_path):
            raise ConfigurationError(f"Executable not found at path: {exe_path}")
        self.exe_path = exe_path
        self.cwd_path = os.path.dirname(exe_path)

    def _wait_for_cli_ready(self, threshold_mb: int = 200, stability_window: int = 6, variance_pct: float = 1.0, timeout: int = 60):
        """
        Waits until the NordVPN GUI has fully started and stabilized.
        Stability is determined by both a memory threshold and minimal variance.
        Args:
            threshold_mb: Minimum memory usage in MB to consider the app started.
            stability_window: Number of consecutive samples to check for stability (check every 0.5s -> window of 6, means 3 seconds).
            variance_pct: Maximum allowed percentage variance in memory usage.
            timeout: Maximum time to wait in seconds.
        """
        global _CLI_IS_READY
        if _CLI_IS_READY:
            return

        print("\n\x1b[33mNordVPN launch command issued.\x1b[0m")

        # Launch GUI via Popen so it doesn’t block.
        try:
            subprocess.Popen(
                [self.exe_path],
                shell=True,
                cwd=self.cwd_path,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=subprocess.CREATE_NO_WINDOW,
            )
        except Exception as e:
            print(f"\x1b[31mLaunch failed: {e}\x1b[0m")

        # steady-state detector
        print("\x1b[33mWaiting for NordVPN to become stable...\x1b[0m")
        start_time = time.time()
        samples = []

        while time.time() - start_time < timeout:
            for proc in psutil.process_iter(["name", "memory_info"]):
                if proc.info["name"] == "NordVPN.exe":
                    mem_mb = proc.info["memory_info"].rss / (1024 * 1024)
                    samples.append(mem_mb)
                    if len(samples) > stability_window:
                        samples.pop(0)

                    if mem_mb > threshold_mb and len(samples) == stability_window:
                        avg = sum(samples) / stability_window
                        max_dev = max(abs(s - avg) for s in samples)
                        if (max_dev / avg) * 100 <= variance_pct:
                            print("\x1b[32mNordVPN CLI is ready.\x1b[0m\n")
                            _CLI_IS_READY = True
                            return
            time.sleep(0.5)

        raise NordVpnCliError(
            f"NordVPN did not reach steady state within {timeout} seconds. "
            "Please ensure the application is running and logged in."
        )

    def _run_command(self, args: List[str], timeout: int = 60) -> subprocess.CompletedProcess:
        """
        Executes a NordVPN CLI command after ensuring readiness.
        Uses Popen with controlled waiting to avoid DNS/routing issues
        caused by overlapping service reconfiguration.

        Args:
            args: List of CLI arguments (e.g. ["-c", "-n", "Germany #741"])
            timeout: Max time (seconds) to wait for command stabilization.
        Returns:
            subprocess.CompletedProcess-like object with stdout/stderr.
        Raises:
            ConfigurationError, NordVpnCliError
        """
        self._wait_for_cli_ready()

        # Quote each argument that contains spaces or special characters
        quoted_args = [
            f'"{a}"' if (" " in a or "#" in a or "&" in a) else a
            for a in args
        ]
        command = f'"{self.exe_path}" {" ".join(quoted_args)}'
        # print(f"\n\x1b[34mRunning NordVPN CLI command: {command}\x1b[0m")

        try:
            process = subprocess.Popen(
                command,
                shell=True,                    # run in shell context (ensures env consistency)
                cwd=self.cwd_path,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                creationflags=subprocess.CREATE_NO_WINDOW,
            )

            try:
                stdout, stderr = process.communicate(timeout=timeout)
            except subprocess.TimeoutExpired:
                process.terminate()
                raise NordVpnCliError(f"NordVPN CLI command timed out after {timeout} seconds.")

            if process.returncode != 0:
                error_message = stderr.strip() or stdout.strip()
                raise NordVpnCliError(
                    f"NordVPN CLI command '{command}' failed.\nError: {error_message}"
                )

            # Return consistent result object
            return subprocess.CompletedProcess(
                args=command, returncode=process.returncode, stdout=stdout, stderr=stderr
            )

        except FileNotFoundError:
            raise ConfigurationError(f"Executable not found at path: {self.exe_path}")
        except Exception as e:
            raise NordVpnCliError(f"Unexpected error while running '{command}': {e}")

    def _get_status_output(self) -> str:
        """
        Returns the raw output of the NordVPN status command.

        Raises:
            NordVpnCliError: If the status command fails.
        """
        return self._run_command(["-status"], timeout=20).stdout

    @staticmethod
    def _parse_status_output(output: str) -> dict:
        """
        Parses key-value style output from NordVPN status.

        Args:
            output: Raw CLI output.

        Returns:
            A dictionary of lower-cased keys to string values.
        """
        parsed = {}
        for line in output.splitlines():
            if ":" not in line:
                continue
            key, value = line.split(":", 1)
            parsed[key.strip().lower()] = value.strip()
        return parsed

    def get_status(self) -> str:
        """
        Gets the current VPN status as reported by the NordVPN CLI.

        Returns:
            A human-readable status string (e.g., 'Connected', 'Disconnected').
        """
        parsed = self._parse_status_output(self._get_status_output())
        return parsed.get("status", "Unknown")

    def get_current_ip(self) -> str | None:
        """
        Gets the currently reported VPN/public IP from CLI status output.

        Returns:
            The IP string if available, otherwise None.
        """
        parsed = self._parse_status_output(self._get_status_output())
        for key in ("your new ip", "current ip", "ip"):
            value = parsed.get(key)
            if value and value.lower() not in {"n/a", "none", "-"}:
                return value
        return None

    def get_connected_server(self) -> str | None:
        """
        Gets the currently connected NordVPN server from CLI status output.

        Returns:
            The server name/host if connected, otherwise None.
        """
        parsed = self._parse_status_output(self._get_status_output())
        for key in ("current server", "server"):
            value = parsed.get(key)
            if value and value.lower() not in {"n/a", "none", "-"}:
                return value
        return None

    def connect(self, target: str, is_group: bool = False):
        """
        Connects to a specific server or group.

        Args:
            target: The server name (e.g., 'Germany #123') or a group name.
            is_group: If True, uses the '-g' flag for group connection.
        """
        args = ["-c", "-g", f"{target}"] if is_group else ["-c", "-n", f"{target}"]
        print(f"\x1b[34mConnecting to '{target}'...\x1b[0m")
        self._run_command(args)

    def disconnect(self):
        """Disconnects from the VPN."""
        print("\n\x1b[34mDisconnecting from NordVPN...\x1b[0m")
        self._run_command(["-d"])

    def flush_dns_cache(self):
        """
        Flushes the Windows DNS resolver cache using `ipconfig /flushdns`.

        Raises:
            NordVpnCliError: If the flush command fails.
        """
        try:
            subprocess.run(
                ["ipconfig", "/flushdns"],
                capture_output=True,
                text=True,
                check=True,
                creationflags=subprocess.CREATE_NO_WINDOW,
            )
        except subprocess.CalledProcessError as e:
            raise NordVpnCliError(f"DNS flush failed: {e.stderr or e.stdout}") from e
        except Exception as e:
            raise NordVpnCliError(f"Unexpected error while flushing DNS: {e}") from e

    def close(self, force: bool = False):
        """
        Closes the NordVPN process entirely.

        Args:
            force: If True, kills the process immediately instead of attempting graceful termination.
        """
        global _CLI_IS_READY
        print("\x1b[34mClosing NordVPN.exe...\x1b[0m")
        found = False

        for proc in psutil.process_iter(["name"]):
            if proc.info["name"] == "NordVPN.exe":
                found = True
                try:
                    if force:
                        proc.kill()
                    else:
                        proc.terminate()
                    proc.wait(timeout=5)
                    print("\x1b[32mNordVPN.exe closed.\x1b[0m")
                except psutil.TimeoutExpired:
                    if not force:
                        print("\x1b[33mProcess did not exit in time, forcing close.\x1b[0m")
                        proc.kill()
                except Exception as e:
                    print(f"\x1b[91mFailed to close NordVPN.exe: {e}\x1b[0m")
                _CLI_IS_READY = False

        if not found:
            print("\x1b[33mNordVPN.exe was not running.\x1b[0m")

