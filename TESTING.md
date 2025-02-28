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

## Test Types

### 1. Data Integrity Tests
- Ensure data consistency and referential integrity
- Examples:
  - `test_valid_user_ids_new.sql`: Validates that all user_ids exist in the user dimension table
  - `test_valid_product_ids_new.sql`: Validates that all product_ids exist in the product dimension table

### 2. Data Consistency Tests
- Validate business rules and logical relationships
- Examples:
  - `test_date_consistency_new.sql`: Ensures dates follow logical rules
  - `test_stock_level_consistency_new.sql`: Validates inventory status matches stock levels

### 3. Business Metrics Tests
- Validate key business metrics calculations and relationships
- Examples:
  - `test_campaign_funnel_consistency.sql`: Validates campaign funnel metrics follow logical rules
  - `test_search_funnel_consistency.sql`: Validates search funnel metrics follow logical rules

## Test Configuration

Tests are configured in `dbt_project.yml` with appropriate severity levels:
- `error`: Test failures will cause the dbt command to fail
- `warn`: Test failures will show warnings but allow the dbt command to succeed

Generic tests are configured using variables in `dbt_project.yml`:
- `funnel_tests`: Configuration for funnel metrics consistency tests
- `id_tests`: Configuration for ID validity tests

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
