import os
import subprocess
import sys
import requests
from pathlib import Path

def download_file(url, destination):
    """Download a file from URL to the specified destination"""
    print(f"Downloading {url} to {destination}")
    response = requests.get(url, stream=True)
    response.raise_for_status()
    
    with open(destination, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    print(f"Downloaded successfully: {destination}")

def setup_iceberg_dependencies():
    """Download and setup Iceberg dependencies for Spark"""
    # Create a directory for dependencies
    deps_dir = Path("dependencies")
    deps_dir.mkdir(exist_ok=True)
    
    # Define Iceberg version and dependencies
    iceberg_version = "1.7.2"
    spark_version = "3.5"
    
    # Base Maven repository URL
    maven_base = "https://repo1.maven.org/maven2"
    
    # List of required dependencies (artifact coordinates and output filenames)
    dependencies = [
        # Corrected format for Iceberg Spark runtime JAR
        # (f"org/apache/iceberg/iceberg-spark-{spark_version}_2.12/{iceberg_version}/iceberg-spark-{spark_version}_2.12-{iceberg_version}.jar",
        #  f"iceberg-spark-{spark_version}_2.12-{iceberg_version}.jar"),
        # # Additional core Iceberg JAR
        # (f"org/apache/iceberg/iceberg-core/{iceberg_version}/iceberg-core-{iceberg_version}.jar",
        #  f"iceberg-core-{iceberg_version}.jar"),
        # Additional Iceberg Spark extensions JAR
        (f"org/apache/iceberg/iceberg-spark-extensions-{spark_version}_2.12/{iceberg_version}/iceberg-spark-extensions-{spark_version}_2.12-{iceberg_version}.jar",
         f"iceberg-spark-extensions-{spark_version}_2.12-{iceberg_version}.jar"),
        # Additional Iceberg Spark runtime JAR
        (f"org/apache/iceberg/iceberg-spark-runtime-{spark_version}_2.12/{iceberg_version}/iceberg-spark-runtime-{spark_version}_2.12-{iceberg_version}.jar",
         f"iceberg-spark-runtime-{spark_version}_2.12-{iceberg_version}.jar"),
        # Common Iceberg dependencies
        ("org/apache/parquet/parquet-column/1.12.3/parquet-column-1.12.3.jar",
         "parquet-column-1.12.3.jar"),
        ("org/apache/parquet/parquet-hadoop/1.12.3/parquet-hadoop-1.12.3.jar",
         "parquet-hadoop-1.12.3.jar"),
        # Added common dependency
        ("org/apache/avro/avro/1.11.1/avro-1.11.1.jar",
         "avro-1.11.1.jar")
    ]
    
    # Download each dependency if not already present
    jars = []
    for artifact_path, filename in dependencies:
        jar_path = deps_dir / filename
        jars.append(str(jar_path))
        
        if not jar_path.exists():
            url = f"{maven_base}/{artifact_path}"
            try:
                download_file(url, jar_path)
            except Exception as e:
                print(f"Error downloading {url}: {e}")
                # Continue with other dependencies instead of returning None
                continue
    
    # Only return the list if we have at least some JARs
    if jars:
        return jars
    return None

def update_spark_session_configs():
    """Update Spark session creation code with Iceberg dependencies"""
    jars = setup_iceberg_dependencies()
    if not jars:
        print("Failed to download required dependencies. Cannot update configurations.")
        return
    
    # Generate classpath string for Spark with forward slashes
    jars_classpath = ",".join(jar.replace("\\", "/") for jar in jars)
    
    print("\nAdd the following to your Spark session builder:")
    print(f"""
    .config("spark.jars", "{jars_classpath}")
    """)
    
    # Create a sample .env file with SPARK_JARS for environment variable approach
    with open(".env", "w") as f:
        f.write(f'SPARK_JARS="{jars_classpath}"\n')
    
    print("\nCreated .env file with SPARK_JARS environment variable.")
    print("You can load this in your Python script with python-dotenv.")

if __name__ == "__main__":
    print("Setting up Iceberg dependencies for Spark...")
    update_spark_session_configs()
    print("\nSetup complete!")
