import subprocess
import pandas as pd
from datetime import datetime
from contextlib import contextmanager
import io


class HdfsHandler:
    def __init__(
        self,
        hdfs_host: str,
        hdfs_port: str,
        hdfs_path: str,
        hdfs_container: str,
        container_temp_dir: str,
    ) -> None:
        self.hdfs_host = hdfs_host
        self.hdfs_port = hdfs_port
        self.hdfs_path = hdfs_path.rstrip("/")
        self.hdfs_container = hdfs_container
        self.container_temp_dir = container_temp_dir

    @contextmanager
    def _run_docker_cmd(self, cmd) -> None:
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
        if df.empty:
            print("No records found.")
            return "No records"

        # Generate file name for HDFS export
        hdfs_file = f"data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        container_parquet = f"{self.container_temp_dir}/{hdfs_file}"

        print(f"Found {len(df)} records. Exporting to HDFS...")

        def parquet_converter(row: pd.Series):
            return row.astype(str).str.strip().isin(df.columns).all()

        # Export to Parquet (using in-memory buffer)
        parquet_buffer = io.BytesIO()
        df = df[~df.apply(parquet_converter, axis=1)]
        df.to_parquet(parquet_buffer, index=False)

        # Move the data directly to the Docker container (no local file)
        parquet_buffer.seek(0)  # Move to the beginning of the buffer
        subprocess.run(
            f"docker exec -i {self.hdfs_container} bash -c 'cat > {container_parquet}'",
            input=parquet_buffer.read(),
            check=True,
        )

        # Run HDFS commands inside the Docker container to upload to HDFS
        self._run_docker_cmd(f"hdfs dfs -mkdir -p {self.hdfs_path}")
        self._run_docker_cmd(
            f"hdfs dfs -put -f {container_parquet} {self.hdfs_path}/{hdfs_file}"
        )

        # Clean up container file
        self._run_docker_cmd(f"rm -f {container_parquet}")

        print(f"Exported {len(df)} records to {self.hdfs_path}/{hdfs_file}")
        return f"Exported {len(df)} records to {self.hdfs_path}/{hdfs_file}"

    def export_data(self, df):
        return self._process_dataframe(df)
