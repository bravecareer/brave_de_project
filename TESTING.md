# DBT Testing Framework

## Test Structure

The testing framework has been enhanced with the following improvements:

### 1. Test Dependencies

Tests now execute in order according to data flow:
- Staging tests run first
- Dimension table tests run after their respective staging tables
- Fact table tests run after all related dimension tables

Dependencies are implemented using `depends_on` in `schema.yml` files.

### 2. Test Classification

Tests are classified by severity:
- **Error tests** (fail the pipeline): Critical data integrity and business logic
- **Warning tests** (notify but don't fail): Non-critical validations

Configuration is in `dbt_project.yml` under the `tests` section.

### 3. Test Coverage

Added tests for key business metrics:
- `test_conversion_rate_calculation.sql`: Validates conversion rate calculations
- `test_key_metrics_stability.sql`: Checks for unexpected fluctuations in KPIs

## Running Tests

To run all tests:
```bash
dbt test
```

To run only warning-level tests:
```bash
dbt test --severity warn
```

To run only error-level tests:
```bash
dbt test --severity error
```

To run a specific test:
```bash
dbt test --select test_name
```

To generate testing documentation:
```bash
dbt docs generate
dbt docs serve
```
