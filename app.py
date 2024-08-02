from flask import Flask, render_template, request, send_from_directory, redirect, url_for, flash, session
import pandas as pd
import re
import jellyfish
from tqdm import tqdm
import os
import json
import snowflake.connector
from threading import Thread
import logging
import queue
import tempfile

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'
app.secret_key = 'supersecretkey'  # Needed for flashing messages

# Configure logging
logging.basicConfig(level=logging.INFO)

# Global variable to hold Snowflake connection
sf_connection = None
query_in_progress = False
query_result_queue = queue.Queue()

def enhanced_clean_text(text):
    """Remove dollar amounts and extra spaces from text."""
    text = re.sub(r'\$\d+(\.\d{1,2})?', '', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def clean_and_translate_raw_name(raw_name, brand_name, abbreviation_mapping):
    """Clean and translate raw names using abbreviation mapping."""
    brand_name = str(brand_name) if pd.notna(brand_name) else ''
    clean_name = enhanced_clean_text(raw_name)
    for abbr, full_form in abbreviation_mapping.items():
        clean_name = re.sub(r'\b{}\b'.format(re.escape(abbr)), full_form, clean_name, flags=re.IGNORECASE)
    clean_name = re.sub(r'\b{}\b'.format(re.escape(brand_name)), '', clean_name, flags=re.IGNORECASE).strip()
    return clean_name.title()

def recommend_storefront_name(row, raw_name_col, storefront_brand_col, abbreviation_mapping):
    """Generate recommended storefront name."""
    raw_name = row[raw_name_col]
    brand_name = row[storefront_brand_col]
    clean_name = clean_and_translate_raw_name(raw_name, brand_name, abbreviation_mapping)
    recommended_name = f"{brand_name} {clean_name}"
    return recommended_name

def process_data(data, raw_brand_name_col, storefront_brand_col, raw_name_col, storefront_name_col, additional_cols, abbreviation_mapping, tversky_alpha, tversky_beta, jaro_count, use_jaro_winkler):
    """Process data and calculate similarity scores."""
    data[storefront_brand_col] = data[storefront_brand_col].astype(str).fillna('')

    tqdm.pandas(desc="Processing rows")
    data['cleaned_raw_name'] = data.progress_apply(lambda row: clean_and_translate_raw_name(row[raw_name_col], row[storefront_brand_col], abbreviation_mapping), axis=1)
    data['recommended_storefront_name'] = data.progress_apply(lambda row: recommend_storefront_name(row, raw_name_col, storefront_brand_col, abbreviation_mapping), axis=1)

    data['separated_brand_name'] = data[storefront_brand_col]
    data['separated_storefront_name'] = data.progress_apply(lambda row: row['recommended_storefront_name'].replace(row[storefront_brand_col], '').strip(), axis=1)

    def clean_text(text):
        return re.sub(r'[^a-zA-Z0-9\s]', '', text).lower()

    stopwords = set(['and', 'or', 'the', 'a', 'an', 'but', 'is', 'in', 'to', 'for', 'with', 'on', 'that', 'by', 'at', 'from'])

    def capitalize_except_stopwords(text):
        words = text.split()
        capitalized_words = [word.title() if word.lower() not in stopwords else word.lower() for word in words]
        return ' '.join(capitalized_words)

    def combined_text_similarity(a, b, additional_values):
        a = clean_text(str(a))
        b = clean_text(str(b))
        combined_a = a + " " + " ".join([clean_text(str(add)) for add in additional_values if pd.notna(add)])
        combined_b = b + " " + " ".join([clean_text(str(add)) for add in additional_values if pd.notna(add)])
        return tversky_similarity(combined_a, combined_b)

    def tversky_similarity(a, b, alpha=tversky_alpha, beta=tversky_beta):
        set_a = set(a)
        set_b = set(b)
        intersection = len(set_a & set_b)
        differences_a = len(set_a - set_b)
        differences_b = len(set_b - set_a)
        total = intersection + alpha * differences_a + beta * differences_b
        return round(0 if total == 0 else intersection / total, 2)

    def jaro_winkler_reassessment(a, b, count=jaro_count, additional_values=[]):
        a = clean_text(a)[:count]
        b = clean_text(b)[:count]
        combined_a = a + " " + " ".join([clean_text(str(add)) for add in additional_values if pd.notna(add)])[:count]
        combined_b = b + " " + " ".join([clean_text(str(add)) for add in additional_values if pd.notna(add)])[:count]
        return jellyfish.jaro_winkler_similarity(combined_a, combined_b)

    data['brand_score'] = data.progress_apply(lambda x: combined_text_similarity(x['separated_brand_name'], x[storefront_brand_col], [x[col] for col in additional_cols if col]), axis=1)
    data['name_score'] = data.progress_apply(lambda x: combined_text_similarity(x[storefront_name_col], x['separated_storefront_name'], [x[col] for col in additional_cols if col]), axis=1)

    if use_jaro_winkler:
        data['reassessed_score'] = data.progress_apply(lambda x: round(jaro_winkler_reassessment(x[storefront_brand_col], x[raw_name_col], additional_values=[x[col] for col in additional_cols if col]), 2)
                                              if x['brand_score'] < 0.51 else x['brand_score'], axis=1)
        data['brand_score'] = data['reassessed_score'].round(2)
        data.drop(columns=['reassessed_score'], inplace=True)

    data['needs_review_brand'] = data.progress_apply(lambda row: 'Y' if row['brand_score'] <= 0.75 else 'N', axis=1)
    data['needs_review_name'] = data.progress_apply(lambda row: 'Y' if row['name_score'] <= 0.75 else 'N', axis=1)

    if additional_cols[0]:
        data['additional_column_1'] = data[additional_cols[0]]
    if additional_cols[1]:
        data['additional_column_2'] = data[additional_cols[1]]
    if additional_cols[2]:
        data['additional_column_3'] = data[additional_cols[2]]

    cols_to_drop = [col for col in ['additional_column_1', 'additional_column_2', 'additional_column_3'] if col in data.columns]
    if cols_to_drop:
        data.drop(columns=cols_to_drop, inplace=True)

    data.drop(columns=['cleaned_raw_name', 'recommended_storefront_name'], inplace=True)

    data.rename(columns={'separated_brand_name': 'cleaned_brand', 'separated_storefront_name': 'cleaned_name'}, inplace=True)

    # Lowercase all column headers
    data.columns = map(str.lower, data.columns)

    return data

@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        choice = request.form.get("choice")
        if choice == "upload":
            return redirect(url_for("upload_file"))
        elif choice == "query":
            return redirect(url_for("select_retailer"))
    return render_template("index.html")

@app.route("/upload_file", methods=["GET", "POST"])
def upload_file():
    if request.method == "POST":
        try:
            # Upload only the raw file
            raw_file = request.files.get("raw_file")
            if not raw_file:
                flash("No raw file uploaded", 'danger')
                return render_template("upload_file.html")

            raw_temp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
            raw_file.save(raw_temp.name)

            # Store file path in session
            session['raw_file_path'] = raw_temp.name

            # Load DataFrame to extract columns
            raw_df = pd.read_csv(raw_temp.name)
            session['columns'] = raw_df.columns.tolist()

            return redirect(url_for('select_columns'))
        except Exception as e:
            logging.error(f"Error during file upload: {e}", exc_info=True)
            flash(str(e), 'danger')
            return render_template("upload_file.html")
    return render_template("upload_file.html")

@app.route("/select_retailer", methods=["GET", "POST"])
def select_retailer():
    if request.method == "POST":
        try:
            global sf_connection
            sf_connection = snowflake.connector.connect(
                user='', # Replace with your Snowflake username
                authenticator='externalbrowser',
                account='instacart-instacart',
                warehouse='catalog_developer_wh',
                database='catalog',
                schema='tmp'
            )
            if sf_connection:
                retailer_id = request.form["retailer_id"]
                return redirect(url_for('query_data', retailer_id=retailer_id))
            else:
                flash("Failed to connect to Snowflake. Please try again.", "danger")
                return render_template("select_retailer.html")
        except Exception as e:
            flash(str(e), 'danger')
            return render_template("select_retailer.html")
    return render_template("select_retailer.html")

@app.route("/query_data/<retailer_id>")
def query_data(retailer_id):
    if not sf_connection:
        flash("Not connected to Snowflake. Please try again.", "danger")
        return redirect(url_for('index'))

    global query_in_progress
    query_in_progress = True
    thread = Thread(target=execute_query, args=(retailer_id,))
    thread.start()
    return render_template("progress.html")

def execute_query(retailer_id):
    global query_in_progress
    logging.info(f"Executing query for retailer_id: {retailer_id}")
    try:
        query = f"""
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
        """
        logging.info("Query execution started")
        df = pd.read_sql(query, sf_connection)
        with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as temp_file:
            df.to_json(temp_file.name)
            temp_file_path = temp_file.name
        logging.info("Query execution completed and file saved")
        query_in_progress = False
        query_result_queue.put({'status': 'success', 'columns': df.columns.tolist(), 'temp_file_path': temp_file_path, 'abbreviation_mapping': json.dumps({})})
    except Exception as e:
        logging.error(f"Query execution failed: {e}")
        query_in_progress = False
        query_result_queue.put({'status': 'error', 'message': str(e)})

@app.route("/progress")
def progress():
    if query_in_progress:
        return "Query is still in progress..."
    else:
        if not query_result_queue.empty():
            result = query_result_queue.get()
            if result['status'] == 'success':
                session['columns'] = result['columns']
                session['temp_file_path'] = result['temp_file_path']
                session['abbreviation_mapping'] = result['abbreviation_mapping']
                return "Query completed."
            else:
                flash(result['message'], 'danger')
                return "Query failed."
    return "Query status unknown."

@app.route("/select_columns", methods=["GET", "POST"])
def select_columns():
    if request.method == "POST":
        try:
            # Retrieve the file paths from the session or Snowflake query
            temp_file_path = session.get('temp_file_path')
            raw_file_path = session.get('raw_file_path')

            # Load the data from the stored file paths
            if temp_file_path:
                raw_df = pd.read_json(temp_file_path)
            elif raw_file_path:
                raw_df = pd.read_csv(raw_file_path)
            else:
                flash("An error occurred: No data source found. Please start the process again.", 'danger')
                return redirect(url_for('index'))

            # Convert column names to lowercase for consistency
            raw_df.columns = raw_df.columns.str.lower()

            # Log the available columns
            logging.info(f"Available columns: {raw_df.columns.tolist()}")

            abbreviation_file_path = session.get('abbreviation_file_path')
            if abbreviation_file_path:
                abbreviation_df = pd.read_csv(abbreviation_file_path)
                abbreviation_mapping = dict(zip(abbreviation_df['abbrev'], abbreviation_df['abbreviation']))
            else:
                abbreviation_mapping = {}

            # Collect form data
            raw_brand_name_col = request.form.get("raw_brand_name_col").lower()
            storefront_brand_col = request.form.get("storefront_brand_col").lower()
            raw_name_col = request.form.get("raw_name_col").lower()
            storefront_name_col = request.form.get("storefront_name_col").lower()
            additional_col1 = request.form.get("additional_col1").lower() if request.form.get("additional_col1") else None
            additional_col2 = request.form.get("additional_col2").lower() if request.form.get("additional_col2") else None
            additional_col3 = request.form.get("additional_col3").lower() if request.form.get("additional_col3") else None
            tversky_alpha = float(request.form.get("tversky_alpha", 0.5))
            tversky_beta = float(request.form.get("tversky_beta", 0.5))
            jaro_count = int(request.form.get("jaro_count", 4))
            use_jaro_winkler = 'use_jaro_winkler' in request.form

            logging.info("Form data collected successfully")

            additional_cols = [col for col in [additional_col1, additional_col2, additional_col3] if col]

            # Ensure the selected columns exist in the DataFrame
            for col in [raw_brand_name_col, storefront_brand_col, raw_name_col, storefront_name_col]:
                if col not in raw_df.columns:
                    raise KeyError(f"Column '{col}' not found in uploaded file.")

            # Process the data
            processed_data = process_data(
                raw_df, raw_brand_name_col, storefront_brand_col, raw_name_col, 
                storefront_name_col, additional_cols, abbreviation_mapping, 
                tversky_alpha, tversky_beta, jaro_count, use_jaro_winkler
            )

            # Save the processed data to a file
            if not os.path.exists(app.config['UPLOAD_FOLDER']):
                os.makedirs(app.config['UPLOAD_FOLDER'])

            output_file_path = os.path.join(app.config['UPLOAD_FOLDER'], "processed_output.csv")
            processed_data.to_csv(output_file_path, index=False)

            logging.info("Data processed and saved successfully")

            # Calculate statistics for the results page
            stats = {
                'brand_above_50': round((processed_data['brand_score'] > 0.50).mean() * 100, 2),
                'brand_below_50': round((processed_data['brand_score'] <= 0.50).mean() * 100, 2),
                'name_above_50': round((processed_data['name_score'] > 0.50).mean() * 100, 2),
                'name_below_50': round((processed_data['name_score'] <= 0.50).mean() * 100, 2),
                'needs_review_brand_percentage': round((processed_data['needs_review_brand'] == 'Y').mean() * 100, 2),
                'needs_review_name_percentage': round((processed_data['needs_review_name'] == 'Y').mean() * 100, 2),
                'classification_counts': processed_data['classified_type'].value_counts(normalize=True).mul(100).round(2).to_dict() if 'classified_type' in processed_data.columns else {}
            }

            logging.info("Statistics calculated successfully")

            # Render the results page
            return render_template("results.html", stats=stats, download_link="/download/processed_output.csv")
        except KeyError as e:
            logging.error(f"Missing column error: {e}")
            flash(f"An error occurred: {str(e)}. Please ensure that the correct columns are selected.", 'danger')
            return redirect(url_for('select_columns'))
        except Exception as e:
            logging.error(f"Error during processing: {e}", exc_info=True)
            flash("An error occurred during processing. Please check the logs.", 'danger')
            return redirect(url_for('upload_file'))
    else:
        try:
            # Check if the session data is available
            columns = session.get('columns')

            if not columns:
                logging.error("Session data is missing: columns not found")
                flash("An error occurred: Session data is missing. Please start the process again.", 'danger')
                return redirect(url_for('index'))

            # Render the column selection page
            return render_template("select_columns.html", columns=columns)
        except Exception as e:
            logging.error(f"Error during GET request processing: {e}", exc_info=True)
            flash("An error occurred while loading the page. Please try again.", 'danger')
            return redirect(url_for('upload_file'))

@app.route("/download/<filename>")
def download_file(filename):
    response = send_from_directory(app.config['UPLOAD_FOLDER'], filename, as_attachment=True)
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response

@app.route("/reset", methods=["GET"])
def reset():
    session.clear()
    return redirect(url_for('index'))

if __name__ == "__main__":
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    app.run(debug=True, host="0.0.0.0")
