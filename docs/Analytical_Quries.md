# Analytical Queries and Data Model Usage

This document illustrates how the star schema data model designed for the US Immigration ETL Pipeline enables powerful analytical capabilities through simple, efficient queries. Below are examples of common analysis scenarios that would be valuable to government agencies, tourism boards, and border security operations.

## Example Analytical Queries

### 1. Visa Types by Country of Citizenship

**Business Need**: Understand which types of visas are most commonly used by visitors from different countries.

**SQL Query**:
```sql
SELECT
    v.country_of_citizenship,
    visa.visa_purpose,
    COUNT(*) as visitor_count
FROM
    fact_immigration f
JOIN
    dim_visitor v ON f.visitor_id = v.visitor_id
JOIN
    dim_visa visa ON f.visatype = visa.visa_code
GROUP BY
    v.country_of_citizenship,
    visa.visa_purpose
ORDER BY
    v.country_of_citizenship,
    visitor_count DESC;
```

**Business Value**: This analysis helps immigration officials understand travel patterns by country, which can inform policy decisions, resource allocation at embassies, and targeted communication campaigns.

### 2. Average Length of Stay by Port of Entry

**Business Need**: Understand typical visitor stay durations at different ports of entry to help with staffing and resource planning.

**SQL Query**:
```sql
SELECT
    p.city,
    p.state,
    AVG(DATEDIFF(dd.full_date, da.full_date)) as avg_stay_days,
    COUNT(*) as visitor_count
FROM
    fact_immigration f
JOIN
    dim_port p ON f.port_code = p.port_code
JOIN
    dim_date da ON f.arrival_date_id = da.date_id
JOIN
    dim_date dd ON f.departure_date_id = dd.date_id
WHERE
    f.departure_date_id IS NOT NULL
GROUP BY
    p.city,
    p.state
ORDER BY
    avg_stay_days DESC;
```

**Business Value**: This analysis helps tourism boards and local governments understand visitor duration patterns, allowing for better planning of local services, accommodations, and attractions.

### 3. Potential Overstays by Country and Age Group

**Business Need**: Identify demographic patterns in visa overstays to improve compliance efforts.

**SQL Query**:
```sql
SELECT
    v.country_of_citizenship,
    CASE
        WHEN v.age < 25 THEN '18-24'
        WHEN v.age < 35 THEN '25-34'
        WHEN v.age < 45 THEN '35-44'
        WHEN v.age < 55 THEN '45-54'
        ELSE '55+'
    END as age_group,
    f.is_overstay,
    COUNT(*) as visitor_count
FROM
    fact_immigration f
JOIN
    dim_visitor v ON f.visitor_id = v.visitor_id
GROUP BY
    v.country_of_citizenship,
    age_group,
    f.is_overstay
ORDER BY
    v.country_of_citizenship,
    age_group;
```

**Business Value**: This analysis helps identify potential patterns in visa non-compliance, allowing immigration authorities to focus resources on specific demographics or countries for education and enforcement efforts.


## Benefits of the Star Schema Design

The star schema design adopted for this project offers several benefits for analytical queries:

1. **Query Simplicity**: Analytical queries are straightforward to write and understand
2. **Performance**: Denormalized dimension tables reduce the number of joins required
3. **Flexibility**: New analytical dimensions can be added without restructuring the fact table
4. **Intuitiveness**: The model closely matches how business users think about the data
5. **Aggregation Efficiency**: The model is optimized for aggregation and drill-down operations

These characteristics make the data model ideal for immigration analysis, where different stakeholders (policy makers, border security, tourism officials) need to examine the same data from different perspectives.