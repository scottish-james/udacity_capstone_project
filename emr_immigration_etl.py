#!/usr/bin/env python3
"""
Immigration ETL Pipeline for EMR 6.9.0
This script processes immigration data using PySpark on EMR.
"""
import sys
import os
import glob
from datetime import datetime, timedelta
import logging
import argparse
import traceback

# PySpark imports
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, BooleanType, DoubleType

# Set up argument parser for EMR execution
parser = argparse.ArgumentParser(description='ETL Pipeline for Immigration Data on EMR 6.9.0')
parser.add_argument('--s3-bucket', type=str, required=True, help='S3 bucket name')
parser.add_argument('--s3-prefix', type=str, default='immigration', help='S3 prefix/folder')
parser.add_argument('--input-path', type=str, required=True, help='S3 path to input data directory')
parser.add_argument('--staging-path', type=str, required=True, help='S3 path to staging directory with lookup files')
parser.add_argument('--output-path', type=str, required=True, help='S3 path to output directory')
parser.add_argument('--rejected-path', type=str, required=True, help='S3 path to rejected records directory')

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class ImmigrationETL:
    """ETL pipeline for US Border & Tourism Analytics using PySpark on EMR 6.9.0."""

    def __init__(self, data_dir, staging_dir, output_dir, rejected_dir):
        """
        Initialize the ETL pipeline.

        Args:
            data_dir (str): S3 path containing the parquet data files
            staging_dir (str): S3 path containing the lookup CSV files
            output_dir (str): S3 path to save the output tables
            rejected_dir (str): S3 path to save rejected records
        """
        self.data_dir = data_dir
        self.staging_dir = staging_dir
        self.output_dir = output_dir
        self.rejected_dir = rejected_dir

        logger.info(f"Initializing ETL with data_dir: {self.data_dir}")
        logger.info(f"Staging directory: {self.staging_dir}")
        logger.info(f"Output directory: {self.output_dir}")
        logger.info(f"Rejected directory: {self.rejected_dir}")

        try:
            # Initialize Spark session optimized for EMR 6.9.0
            self.spark = SparkSession.builder \
                .appName("ImmigrationETL") \
                .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.sql.adaptive.skewJoin.enabled", "true") \
                .config("spark.dynamicAllocation.enabled", "true") \
                .config("spark.shuffle.service.enabled", "true") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .getOrCreate()

            logger.info("Spark session initialized for EMR 6.9.0")
        except Exception as e:
            logger.error(f"Failed to initialize Spark session: {str(e)}")
            logger.error(f"Stack trace: {traceback.format_exc()}")
            raise

        try:
            # Initialize lookup dictionaries
            self.lookup_dicts = self.load_lookup_dictionaries(staging_dir)
        except Exception as e:
            logger.error(f"Failed to load lookup dictionaries: {str(e)}")
            logger.error(f"Stack trace: {traceback.format_exc()}")
            self.lookup_dicts = {}

        # Initialize DataFrames for tables
        self.fact_immigration = None
        self.dim_date = None
        self.dim_visitor = None
        self.dim_visa = None
        self.dim_port = None

        # Initialize DataFrame for rejected records
        self.rejected_records = None

        logger.info("ETL pipeline initialized")

    def load_lookup_dictionaries(self, staging_dir):
        """Load lookup dictionaries from CSV files in S3 staging directory."""
        logger.info(f"Loading lookup dictionaries from {staging_dir}")

        lookup_dicts = {}

        try:
            # List all files in the staging directory to check access
            try:
                file_list = self.spark.sparkContext._jsc.hadoopConfiguration().get("fs.s3a.access.key")
                logger.info(f"S3 access key configuration: {'Set' if file_list else 'Not set'}")
            except Exception as e:
                logger.info(f"Could not check S3 configuration: {e}")

            # Look for all CSV files in the staging directory on S3
            try:
                csv_files = self.spark.sparkContext.wholeTextFiles(f"{staging_dir}/*.csv").collect()
                logger.info(f"Found {len(csv_files)} CSV files in staging directory")

                # Log the file names found
                for file_path, _ in csv_files:
                    logger.info(f"Found CSV file: {file_path}")
            except Exception as e:
                logger.error(f"Error listing CSV files in {staging_dir}: {str(e)}")
                csv_files = []

            # Process each CSV file
            for file_path, content in csv_files:
                # Extract the dictionary name from the filename
                file_name = os.path.basename(file_path)
                dict_name = os.path.splitext(file_name)[0]

                try:
                    # Load the file content as a DataFrame using CSV reader
                    lines = content.strip().split('\n')
                    headers = lines[0].split(',')
                    data = [line.split(',') for line in lines[1:]]

                    # Convert to dictionary
                    code_idx = headers.index('code')
                    desc_idx = headers.index('description')

                    # For i94cntyl, ensure codes are stored as strings
                    if dict_name == "i94cntyl":
                        logger.info(f"Processing i94cntyl dictionary with {len(data)} entries")
                        dict_data = {str(row[code_idx]).strip(): row[desc_idx].strip() for row in data}

                        # Log a sample of the processed dictionary
                        sample_items = list(dict_data.items())[:5]
                        logger.info(f"Sample i94cntyl entries: {sample_items}")
                    else:
                        dict_data = {row[code_idx]: row[desc_idx] for row in data}

                    # Store the dictionary
                    lookup_dicts[dict_name] = dict_data
                    logger.info(f"Loaded {dict_name} lookup with {len(dict_data)} entries")
                except Exception as e:
                    logger.error(f"Error loading lookup dictionary from {file_path}: {e}")
                    logger.error(f"Stack trace: {traceback.format_exc()}")
        except Exception as e:
            logger.error(f"Error accessing staging directory: {e}")
            logger.error(f"Stack trace: {traceback.format_exc()}")

            # Fallback to direct file read
            try:
                # Try using Spark's CSV reader directly for S3 files
                for file_type in ["i94cntyl", "i94prtl", "i94model", "i94addrl"]:
                    try:
                        file_path = f"{staging_dir}/{file_type}.csv"
                        logger.info(f"Trying to load {file_path} with direct CSV reader")
                        df = self.spark.read.csv(file_path, header=True)

                        # For i94cntyl, ensure we have proper string conversion
                        if file_type == "i94cntyl":
                            logger.info(f"Processing i94cntyl from direct CSV with {df.count()} entries")
                            # Convert both code and description to strings
                            df = df.withColumn("code", F.col("code").cast(StringType()))
                            df = df.withColumn("description", F.col("description").cast(StringType()))
                            dict_data = {str(row['code']).strip(): row['description'].strip() for row in df.collect()}

                            # Log sample entries
                            sample_items = list(dict_data.items())[:5]
                            logger.info(f"Sample i94cntyl entries from direct CSV: {sample_items}")
                        else:
                            dict_data = {row['code']: row['description'] for row in df.collect()}

                        lookup_dicts[file_type] = dict_data
                        logger.info(f"Loaded {file_type} lookup with {len(dict_data)} entries")
                    except Exception as file_error:
                        logger.warning(f"Could not load {file_type} lookup: {file_error}")
            except Exception as inner_e:
                logger.error(f"Error in fallback lookup loading: {inner_e}")

        return lookup_dicts

    def create_date_udf(self):
        """
        Create a UDF for SAS date to datetime conversion.
        SAS dates are the number of days since January 1, 1960.
        """

        def sas_date_to_date(sas_date):
            """Convert SAS date to Python date."""
            if sas_date is None:
                return None

            # Base date for SAS (January 1, 1960)
            base_date = datetime(1960, 1, 1)

            # Convert SAS date to datetime
            try:
                return (base_date + timedelta(days=int(sas_date))).date()
            except (ValueError, TypeError):
                return None

        # Register UDF with Spark
        return F.udf(sas_date_to_date, DateType())

    def date_to_id_udf(self):
        """Create a UDF to convert date to date_id (YYYYMMDD)."""

        def date_to_id(date):
            """Convert date to date_id (YYYYMMDD)."""
            if date is None:
                return None
            return int(date.strftime('%Y%m%d'))

        # Register UDF with Spark
        return F.udf(date_to_id, IntegerType())

    def load_immigration_data(self):
        """
        Load immigration data from parquet files.

        Returns:
            DataFrame: Immigration data as a Spark DataFrame
        """
        logger.info(f"Loading immigration data from {self.data_dir}")

        try:
            # Check if the directory exists and list files
            try:
                logger.info(f"Checking directory: {self.data_dir}")
                files = self.spark.sparkContext.wholeTextFiles(f"{self.data_dir}/*").collect()
                logger.info(f"Found {len(files)} files in input directory")
                for file_path, _ in files[:5]:  # Log first 5 files
                    logger.info(f"Found file: {file_path}")
            except Exception as e:
                logger.error(f"Error listing files in {self.data_dir}: {str(e)}")
                logger.error(f"Stack trace: {traceback.format_exc()}")

            # Read parquet files from S3
            logger.info(f"Attempting to read parquet files from {self.data_dir}")
            immigration_df = self.spark.read.parquet(self.data_dir)

            # Get schema details
            schema_fields = immigration_df.schema.fields
            logger.info(f"Schema contains {len(schema_fields)} fields")
            for field in schema_fields[:10]:  # Log first 10 fields
                logger.info(f"Field: {field.name}, Type: {field.dataType}")

            # Log sample data
            logger.info("Sample data (first 5 rows):")
            sample_rows = immigration_df.limit(5).collect()
            for i, row in enumerate(sample_rows):
                logger.info(f"Row {i + 1}: {row}")

            record_count = immigration_df.count()
            logger.info(f"Total records loaded: {record_count}")

            return immigration_df
        except Exception as e:
            logger.error(f"Error loading parquet files: {str(e)}")
            logger.error(f"Stack trace: {traceback.format_exc()}")
            logger.error(f"Make sure parquet files exist in {self.data_dir} and are readable")
            return None

    def clean_immigration_data(self, df):
        """
        Clean and preprocess immigration data.

        Args:
            df (DataFrame): Raw immigration data as Spark DataFrame

        Returns:
            tuple: (cleaned_df, rejected_df)
        """
        if df is None:
            logger.error("Cannot clean immigration data: DataFrame is None")
            return None, None

        logger.info("Cleaning immigration data")
        logger.info(f"Input DataFrame has {df.count()} rows")

        try:
            # Create UDF for date conversion
            sas_to_date = self.create_date_udf()
            date_to_id = self.date_to_id_udf()

            # Register the DataFrame as a temporary view to use SQL
            df.createOrReplaceTempView("immigration_raw")

            # Convert date fields from SAS format to date
            logger.info("Converting SAS dates to date format")
            if "arrdate" in df.columns:
                df = df.withColumn("arrdate_dt", sas_to_date(df["arrdate"]))
            else:
                logger.warning("Column 'arrdate' not found in DataFrame")

            if "depdate" in df.columns:
                df = df.withColumn("depdate_dt", sas_to_date(df["depdate"]))
            else:
                logger.warning("Column 'depdate' not found in DataFrame")

            # Convert dates to date_id format for joining with date dimension
            if "arrdate_dt" in df.columns:
                df = df.withColumn("arrival_date_id", date_to_id(df["arrdate_dt"]))

            if "depdate_dt" in df.columns:
                df = df.withColumn("departure_date_id", date_to_id(df["depdate_dt"]))

            # Add columns to track rejected records (instead of creating separate dataframes)
            df = df.withColumn("is_rejected", F.lit(False))
            df = df.withColumn("rejection_reason", F.lit(None).cast(StringType()))

            # Handle invalid port codes
            if "i94port" in df.columns and "i94prtl" in self.lookup_dicts:
                logger.info("Processing port codes")
                # Get the valid ports as a list
                valid_ports = list(self.lookup_dicts["i94prtl"].keys())
                logger.info(f"Found {len(valid_ports)} valid port codes")

                # Broadcast the list to all workers
                valid_ports_bc = self.spark.sparkContext.broadcast(valid_ports)

                # Create a UDF to check if a port is valid
                def is_valid_port(port):
                    if port is None:
                        return True
                    return port in valid_ports_bc.value

                is_valid_port_udf = F.udf(is_valid_port, BooleanType())

                # Flag records with invalid ports
                df = df.withColumn("valid_port", is_valid_port_udf(df["i94port"]))

                # Mark invalid port records as rejected
                invalid_ports_count = df.filter(~F.col("valid_port")).count()

                if invalid_ports_count > 0:
                    logger.warning(f"Found {invalid_ports_count} records with invalid port codes")

                    # Update rejection flags for invalid ports
                    df = df.withColumn(
                        "is_rejected",
                        F.when(~F.col("valid_port"), F.lit(True)).otherwise(F.col("is_rejected"))
                    )

                    df = df.withColumn(
                        "rejection_reason",
                        F.when(
                            (~F.col("valid_port")) & (F.col("is_rejected")),
                            F.lit("Invalid port code")
                        ).otherwise(F.col("rejection_reason"))
                    )

                    # Set invalid ports to null in the main DataFrame
                    df = df.withColumn(
                        "i94port",
                        F.when(F.col("valid_port"), F.col("i94port")).otherwise(None)
                    )

                # Drop the temporary column
                df = df.drop("valid_port")
            else:
                logger.warning("Skipping port code validation: missing required columns or lookup")

            # Map mode codes to travel modes
            if "i94mode" in df.columns and "i94model" in self.lookup_dicts:
                logger.info("Mapping travel modes")
                # Broadcast the mode dictionary to all workers
                mode_dict_bc = self.spark.sparkContext.broadcast(self.lookup_dicts["i94model"])

                # Create a UDF to map mode codes
                def map_mode(mode):
                    if mode is None:
                        return "Unknown"
                    return mode_dict_bc.value.get(str(mode), "Unknown")

                map_mode_udf = F.udf(map_mode, StringType())

                # Map mode codes to travel modes
                df = df.withColumn("travel_mode", map_mode_udf(df["i94mode"]))
            else:
                logger.warning("Skipping travel mode mapping: missing required columns or lookup")

            # Check for invalid dates (departure before arrival)
            if "arrdate_dt" in df.columns and "depdate_dt" in df.columns:
                logger.info("Checking for invalid date sequences")
                # Identify records with invalid date sequence
                invalid_dates_count = df.filter(
                    (F.col("depdate_dt").isNotNull()) &
                    (F.col("depdate_dt") < F.col("arrdate_dt"))
                ).count()

                if invalid_dates_count > 0:
                    logger.warning(f"Found {invalid_dates_count} records with departure date before arrival date")

                    # Flag records with invalid dates as rejected
                    df = df.withColumn(
                        "is_rejected",
                        F.when(
                            (F.col("depdate_dt").isNotNull()) & (F.col("depdate_dt") < F.col("arrdate_dt")),
                            F.lit(True)
                        ).otherwise(F.col("is_rejected"))
                    )

                    df = df.withColumn(
                        "rejection_reason",
                        F.when(
                            (F.col("depdate_dt").isNotNull()) &
                            (F.col("depdate_dt") < F.col("arrdate_dt")) &
                            (F.col("is_rejected")),
                            F.lit("Departure date before arrival date")
                        ).otherwise(F.col("rejection_reason"))
                    )

                    # For records with invalid date sequence, set departure_date_id to NULL
                    df = df.withColumn(
                        "departure_date_id",
                        F.when(
                            (F.col("depdate_dt").isNotNull()) & (F.col("depdate_dt") < F.col("arrdate_dt")),
                            None
                        ).otherwise(F.col("departure_date_id"))
                    )
            else:
                logger.warning("Skipping date validation: missing required date columns")

            # Set missing visatype to 'Unknown'
            if "visatype" in df.columns:
                logger.info("Processing visa types")
                missing_visa = df.filter(F.col("visatype").isNull()).count()
                if missing_visa > 0:
                    logger.warning(f"Found {missing_visa} records with missing visa type")
                    df = df.withColumn("visatype", F.coalesce(F.col("visatype"), F.lit("Unknown")))
            else:
                logger.warning("Column 'visatype' not found in DataFrame")

            # Add is_overstay flag
            if "arrdate_dt" in df.columns and "depdate_dt" in df.columns:
                logger.info("Adding is_overstay flag")
                # Missing departure date could indicate overstay
                df = df.withColumn("is_overstay", F.col("depdate_dt").isNull())

            # Clean age data
            if "i94bir" in df.columns:
                logger.info("Cleaning age data")
                # Get records with negative ages
                negative_ages_count = df.filter(F.col("i94bir") < 0).count()

                if negative_ages_count > 0:
                    logger.warning(f"Found {negative_ages_count} records with negative ages")

                    # Mark records with negative ages as rejected
                    df = df.withColumn(
                        "is_rejected",
                        F.when(
                            F.col("i94bir") < 0,
                            F.lit(True)
                        ).otherwise(F.col("is_rejected"))
                    )

                    df = df.withColumn(
                        "rejection_reason",
                        F.when(
                            (F.col("i94bir") < 0) & (F.col("is_rejected")),
                            F.lit("Negative age value")
                        ).otherwise(F.col("rejection_reason"))
                    )

                    # Set negative ages to null
                    df = df.withColumn(
                        "i94bir",
                        F.when(F.col("i94bir") >= 0, F.col("i94bir")).otherwise(None)
                    )
            else:
                logger.warning("Column 'i94bir' not found in DataFrame")

            # Handle unknown country codes - maps them temporarily to see which ones resolve to "Unknown(code)"
            if "i94cit" in df.columns and "i94cntyl" in self.lookup_dicts:
                logger.info("Validating country citizenship codes")

                # Create a temporary mapping function that will identify unmapped codes
                country_dict = self.lookup_dicts["i94cntyl"]
                country_dict_bc = self.spark.sparkContext.broadcast(country_dict)

                def map_country_temp(code):
                    """Map country code to name, using 'Unknown(code)' pattern for unmapped codes."""
                    if code is None:
                        return "Unknown"

                    # Try different formats of the code
                    code_variants = [
                        str(code),  # String representation
                        str(int(code)) if isinstance(code, (int, float)) else None,  # Integer string
                        code  # Original value
                    ]

                    # Try each variant
                    for variant in code_variants:
                        if variant is not None and variant in country_dict_bc.value:
                            return country_dict_bc.value[variant]

                    # If we get here, code wasn't found
                    return f"Unknown({code})"

                map_country_temp_udf = F.udf(map_country_temp, StringType())

                # Add temporary mapping column
                df = df.withColumn("temp_country_citizenship", map_country_temp_udf(df["i94cit"]))

                # Check for Unknown pattern to identify unmapped codes
                unknown_pattern = "Unknown\\(.*\\)"
                invalid_citizenship_count = df.filter(F.col("temp_country_citizenship").rlike(unknown_pattern)).count()

                if invalid_citizenship_count > 0:
                    logger.warning(f"Found {invalid_citizenship_count} records with unknown citizenship country codes")

                    # Update rejection flags for invalid country codes
                    df = df.withColumn(
                        "is_rejected",
                        F.when(F.col("temp_country_citizenship").rlike(unknown_pattern), F.lit(True))
                        .otherwise(F.col("is_rejected"))
                    )

                    df = df.withColumn(
                        "rejection_reason",
                        F.when(
                            (F.col("temp_country_citizenship").rlike(unknown_pattern)) & (F.col("is_rejected")),
                            F.lit("Unknown country citizenship code")
                        ).otherwise(F.col("rejection_reason"))
                    )

                # Drop the temporary column
                df = df.drop("temp_country_citizenship")
            else:
                logger.warning("Skipping country citizenship validation: missing required columns or lookup")

            # Also validate residence country codes using the same pattern
            if "i94res" in df.columns and "i94cntyl" in self.lookup_dicts:
                logger.info("Validating country residence codes")

                # Create a temporary mapping function that will identify unmapped codes
                country_dict = self.lookup_dicts["i94cntyl"]
                country_dict_bc = self.spark.sparkContext.broadcast(country_dict)

                def map_country_temp(code):
                    """Map country code to name, using 'Unknown(code)' pattern for unmapped codes."""
                    if code is None:
                        return "Unknown"

                    # Try different formats of the code
                    code_variants = [
                        str(code),  # String representation
                        str(int(code)) if isinstance(code, (int, float)) else None,  # Integer string
                        code  # Original value
                    ]

                    # Try each variant
                    for variant in code_variants:
                        if variant is not None and variant in country_dict_bc.value:
                            return country_dict_bc.value[variant]

                    # If we get here, code wasn't found
                    return f"Unknown({code})"

                map_country_temp_udf = F.udf(map_country_temp, StringType())

                # Add temporary mapping column
                df = df.withColumn("temp_country_residence", map_country_temp_udf(df["i94res"]))

                # Check for Unknown pattern to identify unmapped codes
                unknown_pattern = "Unknown\\(.*\\)"
                invalid_residence_count = df.filter(F.col("temp_country_residence").rlike(unknown_pattern)).count()

                if invalid_residence_count > 0:
                    logger.warning(f"Found {invalid_residence_count} records with unknown residence country codes")

                    # Update rejection flags for invalid country codes
                    df = df.withColumn(
                        "is_rejected",
                        F.when(F.col("temp_country_residence").rlike(unknown_pattern), F.lit(True))
                        .otherwise(F.col("is_rejected"))
                    )

                    df = df.withColumn(
                        "rejection_reason",
                        F.when(
                            (F.col("temp_country_residence").rlike(unknown_pattern)) & (F.col("is_rejected")),
                            F.lit("Unknown country residence code")
                        ).otherwise(F.col("rejection_reason"))
                    )

                # Drop the temporary column
                df = df.drop("temp_country_residence")
            else:
                logger.warning("Skipping country residence validation: missing required columns or lookup")

            # Split DataFrame into cleaned and rejected
            logger.info("Splitting data into cleaned and rejected datasets")
            rejected_df = df.filter(F.col("is_rejected"))
            cleaned_df = df.filter(~F.col("is_rejected"))

            # Drop the temporary rejection columns from cleaned data
            cleaned_df = cleaned_df.drop("is_rejected", "rejection_reason")

            # Store rejected records
            rejected_count = rejected_df.count()
            if rejected_count > 0:
                logger.info(f"Identified {rejected_count} rejected records")
                self.rejected_records = rejected_df
            else:
                logger.info("No rejected records identified")

            logger.info("Cleaning complete")
            logger.info(f"Cleaned DataFrame has {cleaned_df.count()} rows")
            return cleaned_df, rejected_df

        except Exception as e:
            logger.error(f"Error in clean_immigration_data: {str(e)}")
            logger.error(f"Stack trace: {traceback.format_exc()}")
            return None, None

    def create_dim_date(self, start_date='1960-01-01', end_date='2030-01-01'):
        """
        Create date dimension table.

        Args:
            start_date (str): Start date for the date dimension
            end_date (str): End date for the date dimension

        Returns:
            DataFrame: Date dimension table as Spark DataFrame
        """
        logger.info(f"Creating date dimension from {start_date} to {end_date}")

        try:
            # Generate date sequence using Spark SQL
            self.spark.sql(f"""
            SELECT EXPLODE(SEQUENCE(TO_DATE('{start_date}'), TO_DATE('{end_date}'), INTERVAL 1 DAY)) as full_date
            """).createOrReplaceTempView("date_sequence")

            # Create date dimension with additional attributes
            dim_date = self.spark.sql("""
            SELECT 
                full_date,
                CAST(DATE_FORMAT(full_date, 'yyyyMMdd') AS INT) as date_id,
                YEAR(full_date) as year,
                MONTH(full_date) as month,
                DATE_FORMAT(full_date, 'MMMM') as month_name,
                DAY(full_date) as day,
                DAYOFWEEK(full_date) as day_of_week,
                DATE_FORMAT(full_date, 'EEEE') as weekday_name,
                (DAYOFWEEK(full_date) IN (1, 7)) as is_weekend,
                QUARTER(full_date) as quarter
            FROM date_sequence
            ORDER BY full_date
            """)

            logger.info(f"Created date dimension with {dim_date.count()} rows")
            self.dim_date = dim_date
            return dim_date
        except Exception as e:
            logger.error(f"Error creating date dimension: {str(e)}")
            logger.error(f"Stack trace: {traceback.format_exc()}")
            return None

    def create_dim_visitor(self, immigration_df):
        """
        Create visitor dimension table.

        Args:
            immigration_df (DataFrame): Cleaned immigration data as Spark DataFrame

        Returns:
            DataFrame: Visitor dimension table as Spark DataFrame
        """
        if immigration_df is None:
            logger.error("Cannot create visitor dimension: DataFrame is None")
            return None

        logger.info("Creating visitor dimension")

        try:
            # Check available columns
            visitor_cols = ['cicid', 'i94cit', 'i94res', 'i94bir', 'biryear', 'gender']
            available_cols = [col for col in visitor_cols if col in immigration_df.columns]

            if not available_cols:
                logger.error("No visitor columns available in the dataset")
                return None

            logger.info(f"Available visitor columns: {available_cols}")

            # Select relevant columns
            dim_visitor = immigration_df.select(available_cols)

            # Create UDFs for country lookups
            if "i94cntyl" in self.lookup_dicts:
                logger.info("Mapping country codes to names")

                # Log the lookup dictionary structure
                country_dict = self.lookup_dicts["i94cntyl"]
                logger.info(f"Country lookup dictionary has {len(country_dict)} entries")

                # Broadcast the country dictionary
                country_dict_bc = self.spark.sparkContext.broadcast(country_dict)

                def map_country(code):
                    if code is None:
                        return "Unknown"

                    # Try different formats of the code
                    code_variants = [
                        str(code),  # String representation
                        str(int(code)) if isinstance(code, (int, float)) else None,  # Integer string
                        code  # Original value
                    ]

                    # Try each variant
                    for variant in code_variants:
                        if variant is not None and variant in country_dict_bc.value:
                            return country_dict_bc.value[variant]

                    # If we get here, code wasn't found - this shouldn't happen since we've
                    # already rejected these records in the cleaning step
                    # But as a safeguard, just return a standard "Unknown" value
                    return "Unknown"

                map_country_udf = F.udf(map_country, StringType())

                # Apply the mapping to citizenship and residence columns
                if "i94cit" in dim_visitor.columns:
                    dim_visitor = dim_visitor.withColumn("country_of_citizenship",
                                                         map_country_udf(dim_visitor["i94cit"]))

                    # Log mapping statistics for information only
                    mapping_stats = dim_visitor.groupBy("country_of_citizenship").count().orderBy(F.desc("count"))
                    logger.info("Country citizenship mapping distribution (top 10):")
                    top_mappings = mapping_stats.limit(10).collect()
                    for row in top_mappings:
                        logger.info(f"  {row['country_of_citizenship']}: {row['count']} records")

                if "i94res" in dim_visitor.columns:
                    dim_visitor = dim_visitor.withColumn("country_of_residence",
                                                         map_country_udf(dim_visitor["i94res"]))

                    # Log mapping statistics for information only
                    mapping_stats = dim_visitor.groupBy("country_of_residence").count().orderBy(F.desc("count"))
                    logger.info("Country residence mapping distribution (top 10):")
                    top_mappings = mapping_stats.limit(10).collect()
                    for row in top_mappings:
                        logger.info(f"  {row['country_of_residence']}: {row['count']} records")

            else:
                logger.warning("Skipping country mapping: missing country lookup dictionary")

            # Rename columns
            for old_col, new_col in [('cicid', 'visitor_id'), ('i94bir', 'age'),
                                     ('i94cit', 'country_of_citizenship_code'),
                                     ('i94res', 'country_of_residence_code')]:
                if old_col in dim_visitor.columns:
                    dim_visitor = dim_visitor.withColumnRenamed(old_col, new_col)

            # Drop duplicates
            dim_visitor = dim_visitor.dropDuplicates(["visitor_id"])

            logger.info(f"Created visitor dimension with {dim_visitor.count()} rows")
            self.dim_visitor = dim_visitor
            return dim_visitor
        except Exception as e:
            logger.error(f"Error creating visitor dimension: {str(e)}")
            logger.error(f"Stack trace: {traceback.format_exc()}")
            return None

    def create_dim_visa(self, immigration_df):
        """
        Create visa dimension table.

        Args:
            immigration_df (DataFrame): Cleaned immigration data as Spark DataFrame

        Returns:
            DataFrame: Visa dimension table as Spark DataFrame
        """
        if immigration_df is None:
            logger.error("Cannot create visa dimension: DataFrame is None")
            return None

        logger.info("Creating visa dimension")

        try:
            # Check if visatype column exists
            if "visatype" not in immigration_df.columns:
                logger.error("Missing visatype column for visa dimension")
                return None

            # Get unique visatype and visapost combinations
            if "visapost" in immigration_df.columns:
                visa_df = immigration_df.select("visatype", "visapost").distinct()
                # Fill NaN visapost with 'Unknown'
                visa_df = visa_df.withColumn("visapost",
                                             F.coalesce(F.col("visapost"), F.lit("Unknown")))
            else:
                # Get unique visa types
                visa_df = immigration_df.select("visatype").distinct()
                visa_df = visa_df.withColumn("visapost", F.lit("Unknown"))

            # Create basic mapping for visa purposes
            visa_purpose_map = {
                'B1': 'Business',
                'B2': 'Tourism/Pleasure',
                'WT': 'Visa Waiver Tourist',
                'WB': 'Visa Waiver Business',
                'F1': 'Student',
                'M1': 'Vocational Student',
                'J1': 'Exchange Visitor',
                'H1B': 'Work - Specialty Occupation',
                'H2A': 'Work - Agricultural',
                'H2B': 'Work - Non-agricultural',
                'L1': 'Intracompany Transferee',
                'E1': 'Treaty Trader',
                'E2': 'Treaty Investor',
                'CP': 'Crewmember',
                'GMT': 'Global Entry Mexico',
                'I': 'Journalist',
                'Unknown': 'Unknown'
            }

            # Broadcast the visa purpose map
            purpose_map_bc = self.spark.sparkContext.broadcast(visa_purpose_map)

            # Create UDF for mapping visa purpose
            def map_visa_purpose(visa_type):
                if visa_type is None:
                    return "Unknown"
                return purpose_map_bc.value.get(visa_type, "Other")

            map_purpose_udf = F.udf(map_visa_purpose, StringType())

            # Apply the purpose mapping
            visa_df = visa_df.withColumn("visa_purpose", map_purpose_udf(F.col("visatype")))

            # Add visa category based on i94visa if available
            if "i94visa" in immigration_df.columns:
                # Create mapping from i94visa to category
                visa_category_map = {
                    1: 'Business',
                    2: 'Pleasure',
                    3: 'Student'
                }

                # Broadcast the category map
                category_map_bc = self.spark.sparkContext.broadcast(visa_category_map)

                def map_visa_category(visa_code):
                    if visa_code is None:
                        return None
                    return category_map_bc.value.get(visa_code, None)

                map_category_udf = F.udf(map_visa_category, StringType())

                # Create a temp mapping dataframe
                temp_df = immigration_df.select("visatype", "i94visa").distinct()
                temp_df = temp_df.withColumn("visa_category", map_category_udf(F.col("i94visa")))

                # Join with the visa dimension but rename to avoid duplicate
                visa_df = visa_df.join(
                    temp_df.select("visatype", F.col("visa_category").alias("temp_visa_category")),
                    on="visatype",
                    how="left"
                )

                # Use category for 'Other' purposes
                visa_df = visa_df.withColumn(
                    "visa_purpose",
                    F.when(
                        (F.col("visa_purpose") == "Other") & F.col("temp_visa_category").isNotNull(),
                        F.col("temp_visa_category")
                    ).otherwise(F.col("visa_purpose"))
                )

                # Keep the temp_visa_category column and rename it
                visa_df = visa_df.withColumnRenamed("temp_visa_category", "visa_category")
            else:
                # Add empty visa_category column if i94visa is not in the data
                visa_df = visa_df.withColumn("visa_category", F.lit(None).cast(StringType()))

            # Rename columns
            visa_df = visa_df.withColumnRenamed("visatype", "visa_code")
            visa_df = visa_df.withColumnRenamed("visapost", "visa_issuing_post")

            # Fill remaining 'Other' with 'Unknown'
            visa_df = visa_df.withColumn(
                "visa_purpose",
                F.when(F.col("visa_purpose") == "Other", F.lit("Unknown"))
                .otherwise(F.col("visa_purpose"))
            )

            # Create a schema for the "Unknown" row to match visa_df
            if not visa_df.filter(F.col("visa_code") == "Unknown").count() > 0:
                # Create the unknown row with same schema
                unknown_data = [(
                    "Unknown",  # visa_code
                    "Unknown",  # visa_issuing_post
                    "Unknown",  # visa_purpose
                    None  # visa_category
                )]

                # Create a schema for the unknown row
                unknown_schema = StructType([
                    StructField("visa_code", StringType(), True),
                    StructField("visa_issuing_post", StringType(), True),
                    StructField("visa_purpose", StringType(), True),
                    StructField("visa_category", StringType(), True)
                ])

                # Create DataFrame with the unknown row
                unknown_df = self.spark.createDataFrame(unknown_data, unknown_schema)

                # Union the DataFrames by name
                visa_df = visa_df.unionByName(unknown_df)

            logger.info(f"Created visa dimension with {visa_df.count()} rows")
            self.dim_visa = visa_df
            return visa_df
        except Exception as e:
            logger.error(f"Error creating visa dimension: {str(e)}")
            logger.error(f"Stack trace: {traceback.format_exc()}")
            return None

    def create_dim_port(self, immigration_df):
        """
        Create port dimension table.

        Args:
            immigration_df (DataFrame): Cleaned immigration data as Spark DataFrame

        Returns:
            DataFrame: Port dimension table as Spark DataFrame
        """
        if immigration_df is None:
            logger.error("Cannot create port dimension: DataFrame is None")
            return None

        logger.info("Creating port dimension")

        try:
            # Check required columns and lookups
            if "i94port" not in immigration_df.columns:
                logger.error("Missing i94port column for port dimension")
                return None

            if "i94prtl" not in self.lookup_dicts:
                logger.error("Missing i94prtl lookup dictionary")
                return None

            # Get unique port codes from data
            port_codes = [row.i94port for row in
                        immigration_df.select("i94port").distinct().filter(F.col("i94port").isNotNull()).collect()]

            logger.info(f"Found {len(port_codes)} unique port codes in data")

            # Create port dimension from lookup dictionary
            port_dict = self.lookup_dicts["i94prtl"]

            ports_data = []
            for code in port_codes:
                if code in port_dict:
                    port_name = port_dict[code]

                    # Parse city from port name (simplified)
                    city = port_name.split(',')[0].strip() if ',' in port_name else port_name.strip()

                    # Parse state if available
                    state = port_name.split(',')[1].strip() if ',' in port_name and len(
                        port_name.split(',')) > 1 else 'Unknown'

                    # Assume US country code for all ports
                    country_code = 'US'

                    ports_data.append((code, port_name, city, state, country_code))

            # Create Spark DataFrame
            schema = StructType([
                StructField("port_code", StringType(), True),
                StructField("port_of_entry", StringType(), True),
                StructField("city", StringType(), True),
                StructField("state", StringType(), True),
                StructField("country_code", StringType(), True)
            ])

            dim_port = self.spark.createDataFrame(ports_data, schema)

            logger.info(f"Created port dimension with {dim_port.count()} rows")
            self.dim_port = dim_port
            return dim_port
        except Exception as e:
            logger.error(f"Error creating port dimension: {str(e)}")
            logger.error(f"Stack trace: {traceback.format_exc()}")
            return None

    def create_fact_immigration(self, immigration_df):
        """
        Create immigration fact table.

        Args:
            immigration_df (DataFrame): Cleaned immigration data as Spark DataFrame

        Returns:
            DataFrame: Immigration fact table as Spark DataFrame
        """
        if immigration_df is None:
            logger.error("Cannot create fact table: DataFrame is None")
            return None

        logger.info("Creating immigration fact table")

        try:
            # Select relevant columns
            fact_cols = [
                'cicid', 'arrival_date_id', 'departure_date_id',
                'visatype', 'i94port', 'travel_mode', 'is_overstay'
            ]

            # Check which columns are available
            available_cols = [col for col in fact_cols if col in immigration_df.columns]

            if not available_cols:
                logger.error("No fact columns available in the dataset")
                return None

            logger.info(f"Available fact columns: {available_cols}")

            # Create fact table with available columns
            fact_immigration = immigration_df.select(available_cols)

            # Rename columns
            rename_mapping = [
                ('cicid', 'visitor_id'),
                ('i94port', 'port_code')
            ]

            for old_col, new_col in rename_mapping:
                if old_col in fact_immigration.columns:
                    fact_immigration = fact_immigration.withColumnRenamed(old_col, new_col)

            logger.info(f"Created immigration fact table with {fact_immigration.count()} rows")
            self.fact_immigration = fact_immigration
            return fact_immigration
        except Exception as e:
            logger.error(f"Error creating fact table: {str(e)}")
            logger.error(f"Stack trace: {traceback.format_exc()}")
            return None

    def save_table_to_path(self, df, name, format='parquet'):
        """
        Save a Spark DataFrame to specified format.

        Args:
            df (DataFrame): Spark DataFrame to save
            name (str): Name of the file (without extension)
            format (str): Format to save ('parquet' or 'csv')
        """
        if df is None:
            logger.warning(f"Cannot save {name}: DataFrame is None")
            return

        filepath = os.path.join(self.output_dir, f"{name}")

        try:
            # Save as specified format
            if format.lower() == 'csv':
                logger.info(f"Saving {name} to {filepath} (CSV format)")
                df.write.mode("overwrite").option("header", "true").csv(filepath)
            else:
                logger.info(f"Saving {name} to {filepath} (parquet format)")
                df.write.mode("overwrite").parquet(filepath)

            logger.info(f"Saved {name} to {filepath} ({format} format) with {df.count()} rows")
        except Exception as e:
            logger.error(f"Error saving {name}: {e}")
            logger.error(f"Stack trace: {traceback.format_exc()}")

    def save_rejected_records(self):
        """Save rejected records with rejection reasons."""
        if self.rejected_records is None or self.rejected_records.count() == 0:
            logger.info("No rejected records to save")
            return

        filepath = os.path.join(self.rejected_dir, "rejected_immigration_records")

        try:
            # Save rejected records
            logger.info(f"Saving rejected records to {filepath}")
            self.rejected_records.write.mode("overwrite").parquet(filepath)
            logger.info(f"Saved {self.rejected_records.count()} rejected records to {filepath}")

            # Create a summary of rejection reasons
            rejection_summary = self.rejected_records.groupBy("rejection_reason").count()
            rejection_summary = rejection_summary.withColumnRenamed("count", "Count")
            rejection_summary = rejection_summary.withColumnRenamed("rejection_reason", "Rejection Reason")

            # Save summary
            summary_path = os.path.join(self.rejected_dir, "rejection_summary")
            logger.info(f"Saving rejection summary to {summary_path}")
            rejection_summary.write.mode("overwrite").parquet(summary_path)
            logger.info(f"Saved rejection summary to {summary_path}")
        except Exception as e:
            logger.error(f"Error saving rejected records: {e}")
            logger.error(f"Stack trace: {traceback.format_exc()}")

    def run_etl_pipeline(self):
        """Run the full ETL pipeline."""
        logger.info("Starting ETL pipeline")

        try:
            # Load immigration data
            logger.info("=== STEP 1: Loading immigration data ===")
            immigration_df = self.load_immigration_data()
            if immigration_df is None:
                logger.error("Failed to load immigration data. Aborting pipeline.")
                return False

            # Log column names
            logger.info(f"Column names: {immigration_df.columns}")

            # Clean data
            logger.info("=== STEP 2: Cleaning immigration data ===")
            cleaned_df, rejected_df = self.clean_immigration_data(immigration_df)
            if cleaned_df is None:
                logger.error("Failed to clean immigration data. Aborting pipeline.")
                return False

            # Create dimension tables
            logger.info("=== STEP 3: Creating dimension tables ===")

            logger.info("Creating date dimension...")
            self.create_dim_date()

            logger.info("Creating visitor dimension...")
            self.create_dim_visitor(cleaned_df)

            logger.info("Creating visa dimension...")
            self.create_dim_visa(cleaned_df)

            logger.info("Creating port dimension...")
            self.create_dim_port(cleaned_df)

            # Create fact table
            logger.info("=== STEP 4: Creating fact table ===")
            self.create_fact_immigration(cleaned_df)

            # Save tables to S3
            logger.info("=== STEP 5: Saving tables to S3 ===")
            self.save_table_to_path(self.dim_date, "dim_date")
            self.save_table_to_path(self.dim_visitor, "dim_visitor")
            self.save_table_to_path(self.dim_visa, "dim_visa")
            self.save_table_to_path(self.dim_port, "dim_port")
            self.save_table_to_path(self.fact_immigration, "fact_immigration")

            # Save rejected records
            logger.info("=== STEP 6: Saving rejected records ===")
            self.save_rejected_records()

            logger.info("ETL pipeline completed successfully")
            return True

        except Exception as e:
            logger.error(f"Error in ETL pipeline: {str(e)}")
            logger.error(f"Stack trace: {traceback.format_exc()}")
            return False


def main():
    """Main entry point for the ETL pipeline."""
    try:
        args = parser.parse_args()

        # Parse arguments
        s3_bucket = args.s3_bucket
        s3_prefix = args.s3_prefix

        input_path = args.input_path
        staging_path = args.staging_path
        output_path = args.output_path
        rejected_path = args.rejected_path

        # Log configuration
        logger.info(f"S3 Bucket: {s3_bucket}")
        logger.info(f"S3 Prefix: {s3_prefix}")
        logger.info(f"Input Path: {input_path}")
        logger.info(f"Staging Path: {staging_path}")
        logger.info(f"Output Path: {output_path}")
        logger.info(f"Rejected Path: {rejected_path}")

        # Create and run ETL pipeline
        etl = ImmigrationETL(
            data_dir=input_path,
            staging_dir=staging_path,
            output_dir=output_path,
            rejected_dir=rejected_path
        )

        success = etl.run_etl_pipeline()

        # Exit with appropriate status code
        sys.exit(0 if success else 1)

    except Exception as e:
        logger.error(f"Unhandled exception in main: {str(e)}")
        logger.error(f"Stack trace: {traceback.format_exc()}")
        sys.exit(1)


if __name__ == "__main__":
    main()