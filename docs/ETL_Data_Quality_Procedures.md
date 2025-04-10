# Immigration ETL Data Quality Procedures

## Overview

This document outlines the data quality procedures implemented in the Immigration ETL Pipeline to ensure the integrity, validity, and reliability of the processed data. Given the critical nature of immigration data and its potential impact on individuals and families, our quality control measures are designed to be comprehensive and robust.

## Quality Control Approach

Our approach to data quality is built on three pillars:

1. **Preventive Measures**: Validating data early in the pipeline
2. **Detective Measures**: Identifying anomalies and inconsistencies during processing
3. **Corrective Measures**: Isolating non-compliant records for remediation at source

## Data Quality Checks

### 1. Port Code Validation

**Purpose**: Ensure all port codes correspond to valid entry points.

**Implementation**:
```python
# Get the valid ports as a list
valid_ports = list(self.lookup_dicts["i94prtl"].keys())

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
```

**Actions**:
- Records with invalid port codes are marked as rejected
- Rejection reason: "Invalid port code"
- Invalid port values are set to NULL in the cleaned dataset

### 2. Date Sequence Validation

**Purpose**: Identify records where departure date precedes arrival date, which is logically impossible.

**Implementation**:
```python
# Identify records with invalid date sequence
invalid_dates_count = df.filter(
    (F.col("depdate_dt").isNotNull()) &
    (F.col("depdate_dt") < F.col("arrdate_dt"))
).count()

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
```

**Actions**:
- Records with departure date before arrival date are marked as rejected
- Rejection reason: "Departure date before arrival date"
- For rejected records, departure_date_id is set to NULL

### 3. Age Validation

**Purpose**: Identify records with negative age values, which are biologically impossible.

**Implementation**:
```python
# Get records with negative ages
negative_ages_count = df.filter(F.col("i94bir") < 0).count()

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
```

**Actions**:
- Records with negative age values are marked as rejected
- Rejection reason: "Negative age value"
- Negative ages are set to NULL in the cleaned dataset

### 4. Country Code Validation

**Purpose**: Identify records with unknown or invalid country codes.

**Implementation**:
```python
def map_country_temp(code):
    """Map country code to name, using 'Unknown(code)' pattern for unmapped codes."""
    if code is None:
        return "Unknown"

    # Try different formats of the code
    code_variants = [
        str(code),                                              # String representation
        str(int(code)) if isinstance(code, (int, float)) else None,  # Integer string
        code                                                    # Original value
    ]

    # Try each variant
    for variant in code_variants:
        if variant is not None and variant in country_dict_bc.value:
            return country_dict_bc.value[variant]

    # If we get here, code wasn't found
    return f"Unknown({code})"
```

**Actions**:
- Records with unmapped country codes are identified with pattern "Unknown(code)"
- These records are marked as rejected
- Rejection reason: "Unknown country citizenship code" or "Unknown country residence code"

### 5. Missing Value Handling

**Purpose**: Handle missing values appropriately based on field importance.

**Implementation**:
```python
# Set missing visatype to 'Unknown'
if "visatype" in df.columns:
    missing_visa = df.filter(F.col("visatype").isNull()).count()
    if missing_visa > 0:
        df = df.withColumn("visatype", F.coalesce(F.col("visatype"), F.lit("Unknown")))
```

**Actions**:
- Missing visa types are filled with "Unknown" (non-rejecting)
- Missing departure dates are treated as potential overstays (non-rejecting, but flagged)
- Other critical missing fields follow field-specific logic

## Rejection Summary

The following table summarizes the rejection criteria and handling:

| Issue | Detection Method | Rejection Status | Handling |
|-------|------------------|------------------|----------|
| Invalid port codes | Not in lookup dictionary | Rejected | Set to NULL |
| Departure < Arrival | Date comparison | Rejected | Keep arrival, nullify departure |
| Negative age | Value check | Rejected | Set to NULL |
| Unknown country codes | Lookup mapping failure | Rejected | Map to "Unknown(code)" |
| Missing visatype | Null check | Not rejected | Fill with "Unknown" |
| Missing departure date | Null check | Not rejected | Flag as potential overstay |

## Quality Metrics

The ETL pipeline captures the following quality metrics during execution:

- Total records processed
- Number of rejected records
- Rejection reason distribution
- Field-specific validation counts

These metrics are saved as part of the pipeline output and can be monitored over time to identify trends in data quality issues.

## Benefits of this Approach

1. **Data Integrity**: By isolating non-compliant records, we maintain the integrity of the processed dataset
2. **Auditability**: Detailed rejection reasons provide clear audit trails for data issues
3. **Remediation**: Rejected records can be analyzed and fixed at source
4. **Process Improvement**: Quality metrics help identify systemic issues in data collection

This comprehensive quality control process ensures that the immigration data processed through the ETL pipeline is reliable, consistent, and suitable for analysis and decision-making.