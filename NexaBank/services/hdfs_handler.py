"""
hdfs_handler.py

This module defines the `HdfsHandler` class for exporting pandas DataFrames to HDFS.
It works inside a Docker container that has access to HDFS, and performs the following steps:

- Converts DataFrame to Parquet format in memory.
- Pipes the Parquet file into a Docker container.
- Executes HDFS commands from within the container to place the file into HDFS.
- Cleans up intermediate files inside the container.

Intended for use in containerized data pipelines.
"""

import subprocess
import pandas as pd
from datetime import datetime
from contextlib import contextmanager
import io


class HdfsHandler:
    """
    A utility class for exporting pandas DataFrames to HDFS via a Docker container.

    Attributes:
        hdfs_host (str): Hostname of the HDFS namenode.
        hdfs_port (str): Port of the HDFS namenode.
        hdfs_path (str): HDFS target directory path.
        hdfs_container (str): Docker container name running Hadoop.
        container_temp_dir (str): Temp directory path inside the container.
    """

    def __init__(
        self,
        hdfs_host: str,
        hdfs_port: str,
        hdfs_path: str,
        hdfs_container: str,
        container_temp_dir: str,
    ) -> None:
        """
        Initialize the HdfsHandler with connection and container info.

        Args:
            hdfs_host (str): HDFS host.
            hdfs_port (str): HDFS port.
            hdfs_path (str): Target HDFS directory.
            hdfs_container (str): Docker container name for Hadoop.
            container_temp_dir (str): Temp directory inside the container.
        """
        self.hdfs_host = hdfs_host
        self.hdfs_port = hdfs_port
        self.hdfs_path = hdfs_path.rstrip("/")
        self.hdfs_container = hdfs_container
        self.container_temp_dir = container_temp_dir

    @contextmanager
    def _run_docker_cmd(self, cmd) -> None:
        """
        Run a command inside the Docker container and handle failures.

        Args:
            cmd (str): Shell command to run inside the container.

        Raises:
            RuntimeError: If the subprocess command fails.
        """
        try:
            subprocess.run(
                f"docker exec {self.hdfs_container} {cmd}",
                shell=True,
                check=True,
                capture_output=True,
                text=True,
            )
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Command failed: {cmd}\nError: {e.stderr}")

    def _process_dataframe(self, df: pd.DataFrame) -> str:
        """
        Process and export a DataFrame to HDFS as a Parquet file.

        - Skips rows that match column names (header-like).
        - Converts to in-memory Parquet buffer.
        - Pipes data into container.
        - Uploads to HDFS.
        - Cleans up container temp file.

        Args:
            df (pd.DataFrame): Data to be exported.

        Returns:
            str: Confirmation message including file path.
        """
        if df.empty:
            print("No records found.")
            return "No records"

        hdfs_file = f"data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        container_parquet = f"{self.container_temp_dir}/{hdfs_file}"

        print(f"Found {len(df)} records. Exporting to HDFS...")

        def parquet_converter(row: pd.Series):
            return row.astype(str).str.strip().isin(df.columns).all()

        # Clean header-like rows and convert to Parquet
        df = df[~df.apply(parquet_converter, axis=1)]
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)

        # Pipe into container as a file
        subprocess.run(
            f"docker exec -i {self.hdfs_container} bash -c 'cat > {container_parquet}'",
            input=parquet_buffer.read(),
            check=True,
        )

        # HDFS commands inside container
        self._run_docker_cmd(f"hdfs dfs -mkdir -p {self.hdfs_path}")
        self._run_docker_cmd(f"hdfs dfs -put -f {container_parquet} {self.hdfs_path}/{hdfs_file}")
        self._run_docker_cmd(f"rm -f {container_parquet}")

        print(f"Exported {len(df)} records to {self.hdfs_path}/{hdfs_file}")
        return f"Exported {len(df)} records to {self.hdfs_path}/{hdfs_file}"

    def export_data(self, df):
        """
        Public interface for exporting DataFrame to HDFS.

        Args:
            df (pd.DataFrame): The data to export.

        Returns:
            str: Result of the export operation.
        """
        return self._process_dataframe(df)