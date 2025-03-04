erDiagram
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
        int inventory_id PK
        string product_id FK
        int warehouse_id
        int supplier_id
        string storage_condition
        int safety_stock_level
        int restock_point
        decimal average_monthly_demand
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
        int term_length
        int word_count
        timestamp first_seen_at
        timestamp last_updated_at
    }

    fact_campaign_metrics_tf {
        string campaign_id PK, FK
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
        string product_id PK, FK
        int warehouse_id PK
        date date_key PK
        int current_stock_level
        decimal average_monthly_demand
    }

    fact_product_performance_tf {
        string product_id PK, FK
        date date_key PK
        string campaign_id PK, FK
        int total_views
        int total_atc
        int total_purchases
        decimal total_revenue
    }

    fact_search_metrics_tf {
        string search_event_id PK
        string user_id FK
        string product_id FK
        timestamp event_timestamp
        string search_request_id FK
        int search_results_count
        string campaign_id FK
    }

    dim_user_tf ||--o{ fact_search_metrics_tf : has
    dim_product_tf ||--o{ fact_product_performance_tf : has_performance
    dim_product_tf ||--o{ fact_search_metrics_tf : appears_in
    dim_product_tf ||--o{ fact_inventory_metrics_tf : has_inventory
    dim_inventory_tf ||--o{ fact_inventory_metrics_tf : provides_inventory_details
    dim_campaign_tf ||--o{ fact_campaign_metrics_tf : has_metrics
    dim_campaign_tf ||--o{ fact_product_performance_tf : influences_product_performance
    dim_campaign_tf ||--o{ fact_search_metrics_tf : influences_search_behavior
    dim_search_terms_tf ||--o{ fact_search_metrics_tf : has_search_details

## Key Relationships

### Dimension Tables
1. `dim_user_tf`: User dimension table
   - Primary Key: user_id
   - Contains user profile information including signup date, preferences, and loyalty status

2. `dim_product_tf`: Product dimension table
   - Primary Key: product_id
   - Contains comprehensive product information including category, price, and attributes

3. `dim_inventory_tf`: Inventory dimension table
   - Primary Key: inventory_id
   - Foreign Key: product_id -> dim_product_tf
   - Contains inventory configuration information like safety stock levels and storage conditions
   - Relates to fact_inventory_metrics_tf for detailed inventory analysis

4. `dim_campaign_tf`: Campaign dimension table
   - Primary Key: campaign_id
   - Contains campaign details including source, medium, content, and targeting information
   - Enables detailed analysis of campaign attributes and configurations

5. `dim_search_terms_tf`: Search terms dimension table
   - Primary Key: search_request_id
   - Contains standardized search terms and related metadata
   - Reduces storage of duplicate search term data and enhances search analysis capabilities

### Fact Tables
1. `fact_campaign_metrics_tf`: Campaign performance fact table
   - Composite Key: [campaign_id, date_key]
   - Foreign Key: campaign_id -> dim_campaign_tf
   - Contains campaign performance metrics including impressions, clicks, and conversion events
   - Enables marketing specialists to evaluate campaign effectiveness and ROI

2. `fact_inventory_metrics_tf`: Inventory metrics fact table
   - Composite Key: [product_id, warehouse_id, date_key]
   - Foreign Keys: 
     - product_id -> dim_product_tf
     - [product_id, warehouse_id] -> dim_inventory_tf
   - Contains daily inventory levels and demand forecasts
   - Helps supply chain managers forecast demand and manage stock levels

3. `fact_product_performance_tf`: Product performance fact table
   - Composite Key: [product_id, date_key, campaign_id]
   - Foreign Keys: 
     - product_id -> dim_product_tf
     - campaign_id -> dim_campaign_tf
   - Contains key product performance metrics including views, add-to-cart events, purchases, and revenue
   - Enables product managers to analyze product performance over time
   - Now includes campaign_id to evaluate campaign impact on product performance

4. `fact_search_metrics_tf`: Search metrics fact table
   - Primary Key: search_event_id
   - Foreign Keys:
     - user_id -> dim_user_tf
     - product_id -> dim_product_tf
     - search_request_id -> dim_search_terms_tf
     - campaign_id -> dim_campaign_tf
   - Contains search event details and results
   - Now includes campaign_id to analyze which campaigns influence user search behavior
   - Helps marketing analysts evaluate search effectiveness and campaign impact

## Business Requirements Supported

1. **Search Effectiveness (ATC Rate)**
   - `fact_search_metrics_tf` combined with `dim_search_terms_tf` and `fact_product_performance_tf` allows marketing analysts to evaluate the effectiveness of product search by analyzing the "Add to Cart" (ATC) rate.
   - The standardized search terms in `dim_search_terms_tf` enable more efficient storage and better analysis of search patterns.
   - With the addition of campaign_id, analysts can now determine which marketing campaigns drive more effective search behavior.

2. **Product Performance**
   - `fact_product_performance_tf` provides product managers with key metrics (views, ATC events, purchases, revenue) to analyze product performance.
   - The addition of campaign_id enables analysis of how different marketing campaigns impact product performance.

3. **Campaign Effectiveness**
   - `fact_campaign_metrics_tf` combined with `dim_campaign_tf` allows marketing specialists to evaluate campaign success and ROI with metrics like impressions, clicks, and conversion events.
   - The detailed campaign attributes in `dim_campaign_tf` enable more granular analysis of campaign performance by source, medium, and targeting parameters.
   - The new connections to `fact_search_metrics_tf` and `fact_product_performance_tf` provide deeper insights into how campaigns influence user behavior and product performance.

4. **Inventory Management**
   - `fact_inventory_metrics_tf` combined with `dim_inventory_tf` and `dim_product_tf` helps supply chain managers forecast demand accurately and manage stock levels efficiently.
   - The inventory configuration details in `dim_inventory_tf` provide context for the metrics in `fact_inventory_metrics_tf`, enabling more informed inventory decisions.

5. **Cross-Domain Analysis**
   - The addition of campaign_id to multiple fact tables enables cross-domain analysis:
     - Marketing teams can analyze how campaigns affect both search behavior and product performance
     - Product managers can understand which campaigns drive the most engagement with their products
     - Business analysts can trace the full customer journey from campaign exposure to search to purchase
