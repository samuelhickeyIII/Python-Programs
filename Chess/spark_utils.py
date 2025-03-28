import os
from pathlib import Path
from pyspark.sql import SparkSession
from dotenv import load_dotenv
import sys
import traceback

# Load environment variables
load_dotenv()

def create_spark_session(app_name="Chess Data Processing", with_iceberg=True):
    """Create and return a Spark session configured for Iceberg"""
    try:
        # Make sure the warehouse directory exists
        warehouse_dir = Path("chess_warehouse").absolute()
        warehouse_dir.mkdir(exist_ok=True)
        
        # Use a simpler path format that works with Hadoop on Windows
        # Just use forward slashes without file:/// prefix
        warehouse_path = str(warehouse_dir).replace("\\", "/")
        
        print(f"Using warehouse path: {warehouse_path}")
        
        jars_classpath = ",".join([
            str(Path(jar).absolute()).replace("\\","/")
            for jar in os.environ.get('SPARK_JARS', '').split(',')
        ])
        jars_classpath = os.environ.get('SPARK_JARS', '')
            
        builder = (
            SparkSession.builder.appName(app_name)
            .config("spark.jars", jars_classpath)
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", "file:///"+warehouse_path)
            .config("spark.sql.defaultCatalog", "local")
            .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
            .master("local[*]")
            .config("spark.driver.memory", "16g")
            # .config("spark.executor.memory", "3g") 
        )
        
        print(f"Creating Spark session with app name: {app_name}")
        return builder.getOrCreate()
    
    except Exception as e:
        print(f"Error creating Spark session: {str(e)}")
        traceback.print_exc()
        raise

# For quick testing
if __name__ == "__main__":
    try:
        # Now test with Iceberg
        print("\nTesting Spark session with Iceberg...")
        spark = create_spark_session(app_name="Spark Utils Test")
        print(f"Iceberg Spark session created successfully: {spark.version}")
        
        # Verify Iceberg functionality
        print("Testing Iceberg catalog...")
        spark.sql("CREATE DATABASE IF NOT EXISTS local.test_db")
        print("TEST_DB created successfully")
        spark.sql("USE local.test_db")
        print("Switched to TEST_DB successfully")
        spark.sql("DROP TABLE IF EXISTS local.test_db")
        print("TEST_DB dropped successfully")

        print("All tests passed!")
    except Exception as e:
        print(f"Test failed: {str(e)}")
        traceback.print_exc()
    finally:
        try:
            spark.stop()
        except:
            pass
