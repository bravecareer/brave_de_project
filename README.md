# DBT Project - User Engagement

## Structure

- `/models/staging/` — cleans raw data
- `/models/dim/` — dimension table(s) (e.g., products)
- `/models/fact/` — fact table built from staging data

## Tests

- Defined in `schema.yml` using `not_null`, `accepted_values`
- Custom SQL tests in `/tests/` for invalid data

## Monitoring

- Tests can be run using `dbt test`
- Failures indicate issues in core data assumptions
