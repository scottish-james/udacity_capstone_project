# Implementation Details and Technical Choices

## Technology Stack

| Component | Technology | Justification |
|-----------|------------|---------------|
| Processing Engine | Apache Spark | Distributed processing framework ideal for large-scale data processing with built-in resilience and optimisation |
| Cloud Platform | AWS | Industry-leading cloud platform with mature big data services and integration capabilities |
| Orchestration | EMR | Managed Hadoop/Spark service that simplifies cluster deployment and management |
| Storage | S3 | Scalable, durable object storage with high throughput and low latency |
| Data Format | Parquet | Columnar format optimised for analytics with high compression and query performance |
| Development | Python | Versatile language with strong data processing libraries and Spark integration |

## Key Technical Decisions

### 1. EMR for Distributed Processing

The decision to use Amazon EMR was driven by several factors:

- **Scalability**: EMR allows easy scaling for processing large immigration datasets (potentially billions of records)
- **Cost Efficiency**: On-demand clusters only run when needed, minimising costs
- **Integration**: Seamless integration with S3 for data storage
- **Managed Service**: Reduces operational overhead compared to self-managed Hadoop/Spark clusters

### 2. Star Schema Data Model

The star schema design was chosen for the following reasons:

- **Query Efficiency**: Optimised for analytical queries with denormalised dimension tables
- **Simplicity**: Straightforward structure that is easy to understand and maintain
- **Flexibility**: Supports a wide range of analytical queries without complex joins
- **Performance**: Reduces join complexity in analytical queries

### 3. Preprocessing for Lookup Tables

We made the conscious decision to extract and process lookup data (via `sas_extract.py`) as a preprocessing step:

- **Efficiency**: The SAS lookup data is static and doesn't change between ETL runs
- **Reliability**: Preprocessing reduces points of failure during the main ETL job
- **Maintainability**: Separation of concerns makes the code more maintainable
- **Performance**: Reduces overhead during the main processing job

### 4. Comprehensive Data Validation

Our rigorous data validation approach was implemented because:

- **Data Integrity**: Immigration data directly impacts individuals and must be accurate
- **Auditability**: All rejected records have explicit rejection reasons for traceability
- **Source Remediation**: Isolating problems enables fixing issues at the source
- **Quality Metrics**: Provides insights into data quality trends over time

### 5. Modular Code Architecture

The code is structured into distinct modules for enhanced maintainability:

- **setup.py**: Infrastructure setup
- **sas_extract.py**: Lookup data preparation
- **emr_deployment_script.py**: Deployment orchestration
- **emr_immigration_etl.py**: Core ETL processing

This separation provides:

- **Clear Responsibilities**: Each component has a single, well-defined purpose
- **Reusability**: Components can be reused or replaced independently
- **Testability**: Easier to test individual components
- **Maintainability**: Changes to one component don't affect others

## Implementation Challenges and Solutions

### Challenge 1: Handling Unknown Country Codes

**Problem**: The country code lookup dictionary did not cover all codes in the dataset.

**Solution**: 
- Implemented a multi-stage lookup process that tries different code formats
- Created a pattern for unknown codes (`Unknown(code)`) to track unmapped values
- Added conditional rejection based on the prevalence of unmapped codes

### Challenge 2: Date Format Conversion

**Problem**: SAS dates (days since 1960-01-01) needed conversion to standard dates.

**Solution**:
- Created a custom UDF for SAS date conversion that properly handles edge cases
- Added validation to identify invalid date sequences (departure before arrival)
- Implemented flagging for potential overstays when departure date is missing

### Challenge 3: Optimising for Large Datasets

**Problem**: Immigration datasets can be extremely large and processing can be time-consuming.

**Solution**:
- Leveraged Spark's optimisations for distributed processing
- Added configuration for dynamic allocation and adaptive query execution
- Implemented broadcasting for lookup dictionaries to reduce shuffling
- Used appropriate partitioning strategies for output data

### Challenge 4: Error Handling and Monitoring

**Problem**: Complex ETL jobs can fail in multiple ways and require robust monitoring.

**Solution**:
- Implemented comprehensive logging throughout the pipeline
- Added step output capture in the deployment script
- Created summary statistics for rejected records
- Designed the system to gracefully handle various failure scenarios

## Performance Considerations

1. **Memory Management**: Optimised Spark memory configuration for handling large datasets
2. **Broadcast Variables**: Small lookup tables are broadcast to all workers to avoid shuffling
3. **Partitioning**: Data is appropriately partitioned for parallel processing
4. **Caching**: Strategic dataframe caching to avoid recomputation of frequent operations
5. **I/O Optimisation**: Using efficient file formats (Parquet) for intermediate and final outputs

## Security and Compliance

Given the sensitive nature of immigration data, several security measures were implemented:

1. **IAM Roles**: Least-privilege access principles with specific IAM roles
2. **S3 Bucket Policies**: Restricted access to data buckets
3. **VPC Configuration**: EMR clusters run in private subnets where appropriate
4. **Data Isolation**: Rejected records are stored separately for controlled access
5. **Audit Logging**: Comprehensive logging for all operations