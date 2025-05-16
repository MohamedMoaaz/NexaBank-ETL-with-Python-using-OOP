"""
hdfs_handler.py - Debugged Version
"""

import subprocess
import pandas as pd
import io
import pathlib


class HdfsHandler:
    def __init__(
        self,
        hdfs_path: str,
        hdfs_container: str,
        container_temp_dir: str = "/tmp",
    ):
        self.hdfs_path = hdfs_path.rstrip("/")
        self.hdfs_container = hdfs_container
        self.container_temp_dir = container_temp_dir
        self._verify_setup()
        self._i = 0

    def _verify_setup(self):
        """Verify Docker and container are accessible"""
        try:
            # Check Docker is running
            subprocess.run(["docker", "ps"], check=True, capture_output=True)

            # Verify container exists
            result = subprocess.run(
                ["docker", "inspect", self.hdfs_container],
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                raise RuntimeError(
                    f"Container '{self.hdfs_container}' not found"
                )

            # Verify temp directory exists in container
            subprocess.run(
                [
                    "docker",
                    "exec",
                    self.hdfs_container,
                    "mkdir",
                    "-p",
                    self.container_temp_dir,
                ],
                check=True,
            )
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Setup verification failed: {e.stderr}")

    def _write_to_container(self, data: bytes, container_path: str):
        """Safely write binary data to container"""
        try:
            proc = subprocess.Popen(
                [
                    "docker",
                    "exec",
                    "-i",
                    self.hdfs_container,
                    "bash",
                    "-c",
                    f"cat > {container_path}",
                ],
                stdin=subprocess.PIPE,
            )
            proc.communicate(input=data)
            if proc.returncode != 0:
                raise RuntimeError("Failed to write to container")

            # Verify file was created
            result = subprocess.run(
                ["docker", "exec", self.hdfs_container, "stat", container_path],
                capture_output=True,
            )
            if result.returncode != 0:
                raise RuntimeError("File not created in container")
        except Exception as e:
            raise RuntimeError(f"Data transfer error: {str(e)}")

    def export_data(self, df: pd.DataFrame, filename: str) -> tuple[bool, str]:
        """Main export method"""
        if df.empty:
            return (False, "No data to export")

        # Generate unique filename
        filepath = pathlib.PurePath(self.hdfs_path, filename)
        hdfs_file = f"{filepath.parent}/{filepath.stem}.parquet"
        container_path = (
            f"{self.container_temp_dir}/{self._i}{filepath.stem}.parquet"
        )
        self._i += 1
        try:
            # Convert DataFrame to Parquet
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, index=False)
            parquet_data = parquet_buffer.getvalue()

            # Transfer to container
            self._write_to_container(parquet_data, container_path)

            # Execute HDFS commands
            self._run_hdfs_commands(hdfs_file, container_path)

            return (
                True,
                f"Successfully exported {len(df)} rows to {self.hdfs_path}/{hdfs_file}",
            )
        except Exception as e:
            return (False, f"Export failed: {str(e)}")
        finally:
            # Cleanup
            self._cleanup(container_path)

    def _run_hdfs_commands(self, hdfs_file: str, container_path: str):
        """Execute HDFS commands with error handling"""
        filepath = pathlib.PurePath(self.hdfs_path, hdfs_file)

        commands = [
            f"hdfs dfs -mkdir -p {filepath.parent}",
            f"hdfs dfs -put -f {container_path} {filepath}",
        ]

        for cmd in commands:
            result = subprocess.run(
                ["docker", "exec", self.hdfs_container, "bash", "-c", cmd],
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                raise RuntimeError(
                    f"HDFS command failed: {cmd}\n"
                    f"Error: {result.stderr.strip()}"
                )

    def _cleanup(self, container_path: str):
        """Clean up temporary files"""
        subprocess.run(
            ["docker", "exec", self.hdfs_container, "rm", "-f", container_path],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
