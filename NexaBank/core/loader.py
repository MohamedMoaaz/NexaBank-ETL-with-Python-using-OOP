"""
hdfs_handler.py - A utility to export Pandas DataFrames as Parquet files into HDFS
via a Docker container running an HDFS client. Handles verification, data transfer,
HDFS command execution, and cleanup steps.
"""

import subprocess
import pandas as pd
import io
import pathlib
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("logs.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


class HdfsHandler:
    """
    A handler class for exporting pandas DataFrames to HDFS through a Docker container.
    """

    def __init__(
        self,
        hdfs_path: str,
        hdfs_container: str,
        container_temp_dir: str = "/tmp",
    ):
        """
        Initialize the HdfsHandler instance and verify the environment setup.

        Args:
            hdfs_path (str): The HDFS directory where files will be exported.
            hdfs_container (str): The Docker container name/ID running the HDFS client.
            container_temp_dir (str, optional): Temp directory in the container. Default is '/tmp'.
        """
        self.hdfs_path = hdfs_path.rstrip("/")
        self.hdfs_container = hdfs_container
        self.container_temp_dir = container_temp_dir
        self._i = 0
        self._verify_setup()

    def _verify_setup(self):
        """
        Verify that Docker is running, the container is accessible, and the temp directory exists.
        Raises:
            RuntimeError: If Docker is not running or the container setup fails.
        """
        logger.info("Verifying Docker and container setup...")
        try:
            subprocess.run(["docker", "ps"], check=True, capture_output=True)
            result = subprocess.run(
                ["docker", "inspect", self.hdfs_container],
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                raise RuntimeError(
                    f"Container '{self.hdfs_container}' not found"
                )

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
            logger.info(
                f"Container '{self.hdfs_container}' is accessible and ready."
            )
        except subprocess.CalledProcessError as e:
            logger.error(f"Setup verification failed: {e.stderr}")
            raise RuntimeError(f"Setup verification failed: {e.stderr}") from e

    def _write_to_container(self, data: bytes, container_path: str):
        """
        Write binary data to a file inside the Docker container.

        Args:
            data (bytes): The file content in bytes.
            container_path (str): The path inside the container to write to.

        Raises:
            RuntimeError: If writing or verifying the file fails.
        """
        logger.debug(f"Writing data to container at: {container_path}")
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

            result = subprocess.run(
                ["docker", "exec", self.hdfs_container, "stat", container_path],
                capture_output=True,
            )
            if result.returncode != 0:
                raise RuntimeError("File not created in container")
            logger.debug("Data written and file verified in container.")
        except Exception as e:
            logger.error(f"Data transfer error: {str(e)}")
            raise RuntimeError(f"Data transfer error: {str(e)}") from e

    def export_data(self, df: pd.DataFrame, filename: str) -> tuple[bool, str]:
        """
        Export a pandas DataFrame to HDFS as a Parquet file.

        Args:
            df (pd.DataFrame): DataFrame to export.
            filename (str): Name of the output Parquet file.

        Returns:
            tuple[bool, str]: A tuple indicating success status and a message.
        """
        logger.info(f"Initiating export for file: {filename}")
        if df.empty:
            logger.warning("Export aborted: DataFrame is empty.")
            return (False, "No data to export")

        filepath = pathlib.PurePath(self.hdfs_path, filename)
        hdfs_file = f"{filepath.parent}/{filepath.stem}.parquet"
        container_path = (
            f"{self.container_temp_dir}/{self._i}{filepath.stem}.parquet"
        )
        self._i += 1

        try:
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, index=False)
            parquet_data = parquet_buffer.getvalue()
            logger.debug(
                f"Converted DataFrame to Parquet ({len(parquet_data)} bytes)."
            )

            self._write_to_container(parquet_data, container_path)
            self._run_hdfs_commands(hdfs_file, container_path)

            logger.info(
                f"Export successful: {len(df)} rows to {self.hdfs_path}/{hdfs_file}"
            )
            return (
                True,
                f"Successfully exported {len(df)} rows to {self.hdfs_path}/{hdfs_file}",
            )
        except Exception as e:
            logger.exception(f"Export failed: {str(e)}")
            raise
        finally:
            self._cleanup(container_path)

    def _run_hdfs_commands(self, hdfs_file: str, container_path: str):
        """
        Execute HDFS commands to move the file from container to HDFS.

        Args:
            hdfs_file (str): Destination path in HDFS.
            container_path (str): File path in the Docker container.

        Raises:
            RuntimeError: If any HDFS command fails.
        """
        hdfs_file = pathlib.PurePath(hdfs_file)
        logger.debug(
            f"Running HDFS commands to move {container_path} to {hdfs_file}..."
        )

        commands = [
            f"hdfs dfs -mkdir -p {hdfs_file.parent}",
            f"hdfs dfs -put -f {container_path} {hdfs_file}",
        ]
        for cmd in commands:
            result = subprocess.run(
                ["docker", "exec", self.hdfs_container, "bash", "-c", cmd],
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                logger.error(
                    f"HDFS command failed: {cmd}\n{result.stderr.strip()}"
                )
                raise RuntimeError(
                    f"HDFS command failed: {cmd}\n"
                    f"Error: {result.stderr.strip()}"
                )
            logger.debug(f"HDFS command succeeded: {cmd}")

    def _cleanup(self, container_path: str):
        """
        Delete temporary file from container to avoid clutter.

        Args:
            container_path (str): Path to the temporary file inside the container.
        """
        logger.debug(f"Cleaning up temporary file: {container_path}")
        subprocess.run(
            ["docker", "exec", self.hdfs_container, "rm", "-f", container_path],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )