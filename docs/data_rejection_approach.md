# Data Rejection Approach

## Overview

This document explains our approach to data rejection in the Immigration ETL Pipeline and serves as a guide to interpreting the rejected records data dictionary.

## Why We Reject Rather Than Correct

For immigration data, we prioritize rejection of problematic records rather than automatic correction because:

1. **Data Accuracy**: Immigration data directly impacts individuals' legal status and rights
2. **Source System Improvement**: Rejection creates visibility into systematic issues that should be fixed at source
3. **Audit Trail**: Maintaining original problematic data with clear rejection reasons supports compliance requirements

## Rejected Records Structure

As detailed in the data dictionary, rejected records include:

| Column Name | Description |
|-------------|-------------|
| original_record | The complete original record with all source fields |
| is_rejected | Flag indicating rejection (always true) |
| rejection_reason | Specific reason for rejection |
| rejection_timestamp | When the record was rejected |
| source_file | Source file containing the record |
| rejection_id | Unique identifier for the rejection event |

## Common Rejection Reasons

The five main rejection categories we track are:

1. **Unknown country citizenship code (56.2% of rejections)**
   - Country code not found in i94cntyl lookup
   - Example: Code 148.0 appears 157,806 times
   - Impact: Geographic origin analysis inaccuracy

2. **Invalid port code (23.0%)**
   - Port code not found in i94prtl lookup
   - Impact: Geographic analysis inaccuracy

3. **Negative age value (13.7%)**
   - Age (i94bir) value is negative
   - Impact: Demographic analysis errors

4. **Departure date before arrival date (7.1%)**
   - Logical inconsistency in dates
   - Impact: Stay duration analysis errors

5. **Unknown country residence code (<0.1%)**
   - Country code not found in i94cntyl lookup
   - Impact: Residence analysis inaccuracy

## Volume and Impact

In our current dataset, we've identified 687,891 rejected records, representing approximately 12% of the total data volume. The most significant issue—unknown country codes—represents 56.2% of all rejections.

## Using Rejected Data

Rejected data is stored separately but remains available for:
- Root cause analysis
- Data quality improvement initiatives
- Reprocessing once source issues are resolved
- Tracking quality metrics over time

Data stewards should review rejection summaries weekly to identify patterns and prioritize fixes at source systems.

## Key Recommendation

Our analysis of rejected records has led to our primary recommendation: standardize country codes by aligning with ISO 3166 international standards to eliminate the most common rejection reason and significantly improve data quality.