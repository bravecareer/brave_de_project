# Data Mart ERD

```mermaid
erDiagram
    dim_user {
        int user_id PK
        string email
        string account_status
    }

    dim_product {
        int product_id PK
        string product_name
        string product_category
        decimal price
        string product_color
        date manufacturing_date
        date expiration_date
        string warranty_period
        decimal rating
        int weight_grams
        decimal discount_percentage
    }

    dim_inventory {
        int inventory_id PK
        int product_id FK
        int quantity_in_stock
        int warehouse_id
        decimal rating
        string stock_level
        int sales_volume
        date restock_date
        decimal weight
        int supplier_id
        json discounts
        string storage_condition
        int safety_stock
        string inventory_status
        decimal average_monthly_demand
        date last_audit_date
        date last_restock_date
        date next_restock_date
        decimal total_inventory_value
        decimal estimated_days_of_inventory
    }

    dim_search_event {
        string search_event_id PK
        string search_terms
        string search_type
        string search_terms_type
        int search_results_count
    }

    fact_user_engagement {
        int user_id FK
        int product_id FK
        string search_event_id FK
        boolean has_qv
        boolean has_pdp
        boolean has_atc
        boolean has_purchase
        string session_id
        timestamp timestamp
    }

    fact_user_transaction {
        int user_id FK
        int product_id FK
        string search_event_id FK
        string session_id
        string cart_id
        boolean has_purchase
        int quantity_sold
        decimal total_amount
        timestamp timestamp
    }

    fact_search_metrics {
        string search_event_id FK
        date date_key
        string search_terms
        string search_type
        string search_terms_type
        int search_results_count
        int total_searches
        int total_quick_views
        int total_product_detail_views
        int total_add_to_cart
        int total_purchases
        decimal quick_view_rate
        decimal atc_rate
        decimal purchase_rate
    }

    dim_user ||--o{ fact_user_engagement : has
    dim_user ||--o{ fact_user_transaction : makes
    dim_product ||--o{ fact_user_engagement : viewed_in
    dim_product ||--o{ fact_user_transaction : bought_in
    dim_product ||--|| dim_inventory : has
    dim_search_event ||--o{ fact_user_engagement : triggers
    dim_search_event ||--o{ fact_user_transaction : leads_to
    dim_search_event ||--|| fact_search_metrics : analyzed_in
```

## Key Relationships

### Dimension Tables
1. `dim_user`: User dimension table
   - Primary Key: user_id
   - Contains basic user information

2. `dim_product`: Product dimension table
   - Primary Key: product_id
   - Contains basic product information

3. `dim_inventory`: Inventory dimension table
   - Primary Key: inventory_id
   - Foreign Key: product_id -> dim_product
   - Contains inventory and product relationship information

4. `dim_search_event`: Search event dimension table
   - Primary Key: search_event_id
   - Contains basic search event information

### Fact Tables
1. `fact_user_engagement`: User behavior fact table
   - Composite Key: [user_id, search_event_id, timestamp]
   - Foreign Keys:
     - user_id -> dim_user
     - product_id -> dim_product
     - search_event_id -> dim_search_event
   - Records user browsing, add-to-cart, and other engagement behaviors

2. `fact_user_transaction`: User transaction fact table
   - Composite Key: [user_id, search_event_id, product_id, timestamp]
   - Foreign Keys:
     - user_id -> dim_user
     - product_id -> dim_product
     - search_event_id -> dim_search_event
   - Records user purchase behaviors

3. `fact_search_metrics`: Search metrics fact table
   - Composite Key: [search_event_id, date_key]
   - Foreign Key: search_event_id -> dim_search_event
   - Records search conversion rates and other metrics
