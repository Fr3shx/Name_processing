
# CSV Product Name Standardizer

This Flask application standardizes product names in CSV files by either uploading raw product data or querying directly from Snowflake. The application processes the data, calculates similarity scores, and provides a downloadable CSV file with standardized product names and detailed similarity statistics.

## Features

- **File Upload:** Upload raw CSV files and an optional abbreviation list.
- **Snowflake Integration:** Query product data directly from Snowflake.
- **Data Standardization:** Standardizes product names based on specified guidelines.
- **Configurable Parameters:** Adjust similarity metrics for tailored results.
- **Detailed Statistics:** View similarity scores and classification counts.
- **Downloadable Output:** Easily download the processed CSV file.

## Prerequisites

- Python 3.x
- pip (Python package installer)
- Snowflake account with access to required data
- Virtual environment setup (optional but recommended)

## Installation

1. **Create a Project Directory:**

   ```bash
   mkdir brand_and_name_audit_processor
   cd brand_and_name_audit_processor
   ```

2. **Set Up a Virtual Environment:**

   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows, use `venv\Scriptsctivate`
   ```

3. **Install Dependencies:**

   ```bash
   pip install --upgrade pip
   pip install -r requirements.txt
   ```

## Configuration

### Modify Snowflake Credentials

Before running the application, you need to update the Snowflake credentials in the `app.py` file:

1. Open `app.py` in a text editor.
2. Locate the Snowflake connection block:

   ```python
   sf_connection = snowflake.connector.connect(
       user='YOUR_SNOWFLAKE_USERNAME',  # Replace with your Snowflake username
       authenticator='externalbrowser',
       account='YOUR_SNOWFLAKE_ACCOUNT',  # Replace with your Snowflake account name
       warehouse='catalog_developer_wh',
       database='catalog',
       schema='tmp'
   )
   ```

3. Replace `'YOUR_SNOWFLAKE_USERNAME'` and `'YOUR_SNOWFLAKE_ACCOUNT'` with your Snowflake credentials.

### Modify the Snowflake Query

The application uses a predefined SQL query to fetch data from Snowflake. You can customize this query to meet your specific data requirements:

1. Open `app.py` in a text editor.
2. Locate the `execute_query` function:

   ```python
   def execute_query(retailer_id):
       query = f'''
       SELECT DISTINCT
           rpv.data :classified_type::text as classified_type,
           pfnd.data:lookup_code::text as lookup_code,
           rpv.product_id,
           rp.id as retailer_product_id,
           pfnd.data :brand_name::text as raw_brand_name,
           rpv.data :normalized_brand_name::text as storefront_brand,
           pfnd.data :name::text as raw_name,
           rpv.data :name::text as storefront_name,
           rpv.data:department_name as department,
           pfnd.data:remote_image_url::text as file_image,
           rpv.data:image_hero_large_url::text as storefront_image,
           rpv.data:prioritized_size as size,
           rpv.data:prioritized_size_value as size_value,
           rpv.data:prioritized_size_uom as size_uom,
           rpv.data:prioritized_unit_count as unit_ct
       FROM
           catalog.catalog.current_retailer_product_view rpv 
           JOIN catalog.catalog.retailer_code_products rcp ON TRUE
           AND rpv.product_id = rcp.product_id
           AND rpv.retailer_id = rcp.retailer_id
       JOIN INSTADATA.RDS_DATA.RETAILER_PRODUCTS rp on TRUE
           AND rpv.product_id = rp.product_id
           AND rpv.retailer_id = rp.retailer_id
       JOIN catalog.catalog.partner_file_normalized_data pfnd on true
           AND rpv.retailer_id = pfnd.retailer_id
           AND pfnd.created_at > CURRENT_DATE - 3
           AND rcp.code = pfnd.lead_code
           and pfnd.data:name is not null
       WHERE
           rpv.retailer_id = {retailer_id}
       QUALIFY ROW_NUMBER() OVER (PARTITION BY pfnd.lead_code ORDER BY pfnd.created_at DESC)=1
       ORDER BY 3
       '''
       logging.info("Query execution started")
       ...
   ```

3. Modify the SQL query within the `execute_query` function to match your data structure and requirements.
4. Save your changes.

### Running the Application

1. **Start the Flask Application:**

   ```bash
   flask run
   ```

2. **Access the Web Interface:**

   Open your web browser and navigate to:

   ```
   http://127.0.0.1:5000/
   ```

3. **Processing CSV Files:**

   - **Option 1: Upload CSV Files**
     - Use the web interface to upload your raw CSV file and optional abbreviation list.
     - Select the appropriate columns and configure similarity parameters.
     - Click "Process" to standardize product names.

   - **Option 2: Query Data from Snowflake**
     - Select "Query from Snowflake" in the web interface.
     - Enter the retailer ID and initiate the query.
     - After the query completes, proceed with selecting columns and configuring parameters.

4. **View Results and Download:**

   - Review the similarity statistics on the results page.
   - Download the processed CSV file using the provided link.

## Snowflake Workflow

### Querying Data

The application supports querying product data directly from Snowflake. This workflow is ideal if your data is stored in Snowflake and you prefer not to download it manually.

1. **Select Snowflake Query Option:**
   - From the main menu, choose the option to query data from Snowflake.

2. **Enter Retailer ID:**
   - Provide the Retailer ID to filter the query results. This ID will be used in the Snowflake query to retrieve relevant data.

3. **Query Execution:**
   - The application runs a pre-configured SQL query on Snowflake and fetches the data. Progress is shown on a dedicated page.
   - Once the query is completed, you will be directed to the column selection page.

### Handling Query Results

After the query completes, the results are processed similarly to uploaded CSV files:

- **Column Selection:** Choose the columns that match your data (e.g., raw brand name, storefront brand).
- **Processing and Standardization:** The data is processed, and similarity scores are calculated based on your configuration.

## Directory Structure

```
brand_and_name_audit_processor/
├── app.py
├── requirements.txt
├── processed_files/         # Directory for saving processed files
├── static/
│   ├── instacart_logo.png   # Static assets like images
│   └── style.css            # Custom stylesheets
└── templates/
    ├── index.html           # Homepage template
    ├── select_columns.html  # Column selection template
    ├── select_retailer.html # Retailer selection template
    ├── progress.html        # Query progress template
    └── results.html         # Results and download template
```

## Troubleshooting

- **ModuleNotFoundError:** Ensure all required Python packages are installed using `pip install -r requirements.txt`.
- **502 Backend Error:** Verify that the Flask application is running correctly and that the virtual environment is activated.

## Notes

- Ensure that the `processed_files` directory exists to save the output CSV files.
- You can customize the application’s logic or parameters by editing `app.py`.
