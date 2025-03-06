erDiagram
    dim_user_tf ||--o{ fact_search_metrics_tf : "user_id"
    dim_product_tf ||--o{ fact_product_performance_tf : "product_id"
    dim_product_tf ||--o{ fact_search_metrics_tf : "product_id"
    dim_product_tf ||--o{ fact_inventory_metrics_tf : "product_id"
    dim_campaign_tf ||--o{ fact_campaign_metrics_tf : "campaign_id"
    dim_campaign_tf ||--o{ fact_product_performance_tf : "campaign_id"
    dim_search_terms_tf ||--o{ fact_search_metrics_tf : "search_request_id"
    dim_inventory_tf ||--o{ fact_inventory_metrics_tf : "inventory_id"

    dim_user_tf {
        string user_id PK
        string first_name
        string last_name
        string email
        date signup_date
        string preferred_language
        date dob
        boolean marketing_opt_in
        string account_status
        int loyalty_points_balance
    }

    dim_product_tf {
        string product_id PK
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

    dim_inventory_tf {
        string inventory_id PK
        string product_id FK
        string warehouse_id
        string supplier_id
        string storage_condition
        int safety_stock_level
        int restock_point
        int average_monthly_demand
        decimal unit_price
    }

    dim_campaign_tf {
        string campaign_id PK
        string campaign_source
        string campaign_medium
        string campaign_content
        string target_country
        string target_region
        string target_device
        string campaign_language
        string campaign_name
        decimal budget
        date start_date
        date end_date
        string campaign_objective
        string target_audience
        string campaign_type
        string campaign_status
        timestamp last_updated
    }

    dim_search_terms_tf {
        string search_request_id PK
        string search_terms
        string search_terms_type
        string search_type
        string search_feature
        string search_model
        timestamp first_seen_at
        timestamp last_updated_at
    }

    fact_product_performance_tf {
        string product_id PK
        date date_key PK
        string campaign_id FK
        int total_views
        int total_atc
        int total_purchases
        decimal total_revenue
    }

    fact_campaign_metrics_tf {
        string campaign_id PK
        date date_key PK
        int total_events
        int unique_categories_viewed
        int total_product_views
        int total_product_detail_views
        int total_add_to_cart
        int total_purchases
        int impressions
        int clicks
    }

    fact_inventory_metrics_tf {
        string product_id PK,FK
        string warehouse_id PK
        string inventory_id FK
        date date_key PK
        int current_stock_level
        int average_monthly_demand
    }

    fact_search_metrics_tf {
        string search_event_id PK
        string user_id FK
        string product_id FK
        timestamp event_timestamp
        string search_request_id FK
        int search_results_count
        string campaign_id FK
        boolean has_atc
        boolean has_pdp
        boolean has_qv
        boolean has_purchase
    }

## Data Model Description (Updated Version)

### Dimension Tables

1. **dim_user_tf**
   - User dimension table, stores basic user information
   - Primary Key: `user_id`

2. **dim_product_tf**
   - Product dimension table, stores detailed product information
   - Primary Key: `product_id`

3. **dim_inventory_tf**
   - Inventory dimension table, stores inventory-related information
   - Primary Key: `inventory_id`
   - Foreign Key: `product_id` relates to `dim_product_tf`

4. **dim_campaign_tf**
   - Campaign dimension table, stores detailed campaign information
   - Primary Key: `campaign_id`

5. **dim_search_terms_tf**
   - Search terms dimension table, stores search-related information
   - Primary Key: `search_request_id`

### Fact Tables

1. **fact_product_performance_tf**
   - Product performance fact table, records metrics for product views, add-to-cart, and purchases
   - Composite Primary Key: `product_id`, `date_key`
   - Foreign Key: `product_id` relates to `dim_product_tf`
   - Foreign Key: `campaign_id` relates to `dim_campaign_tf`

2. **fact_campaign_metrics_tf**
   - Campaign metrics fact table, records campaign effectiveness metrics
   - Composite Primary Key: `campaign_id`, `date_key`
   - Foreign Key: `campaign_id` relates to `dim_campaign_tf`

3. **fact_inventory_metrics_tf**
   - Inventory metrics fact table, records inventory levels and demand
   - Composite Primary Key: `product_id`, `warehouse_id`, `date_key`
   - Foreign Key: `product_id` relates to `dim_product_tf` (direct relation to product dimension)
   - Foreign Key: `inventory_id` relates to `dim_inventory_tf` (relation to inventory dimension)

4. **fact_search_metrics_tf** (Updated)
   - Search metrics fact table, records search events and results
   - Primary Key: `search_event_id`
   - Foreign Key: `user_id` relates to `dim_user_tf`
   - Foreign Key: `product_id` relates to `dim_product_tf`
   - Foreign Key: `search_request_id` relates to `dim_search_terms_tf`
   - Foreign Key: `campaign_id` relates to `dim_campaign_tf`
   - New fields: `has_atc`, `has_pdp`, `has_qv`, `has_purchase` for tracking user behavior

### Relationship Description

Key changes in this updated model include:

1. The `fact_inventory_metrics_tf` table is now associated with two dimension tables:
   - Through `product_id` it directly relates to `dim_product_tf`, providing access to detailed product information
   - Through `inventory_id` it relates to `dim_inventory_tf`, providing access to detailed inventory information

2. The `fact_search_metrics_tf` table has new user behavior tracking fields:
   - `has_atc`: Whether the item was added to cart
   - `has_pdp`: Whether the product detail page was viewed
   - `has_qv`: Whether a quick view was performed
   - `has_purchase`: Whether a purchase was completed

These updates make the data model more comprehensive, enabling multi-dimensional analysis of user behavior, product performance, and inventory status, providing more complete data support for business decisions.