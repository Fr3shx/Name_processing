from flask import Flask, render_template, request, send_from_directory, redirect, url_for, flash
import pandas as pd
import re
import jellyfish
from tqdm import tqdm
import os
import json

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'
app.secret_key = 'supersecretkey'  # Needed for flashing messages

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

def process_data(data, raw_brand_name_col, storefront_brand_col, raw_name_col, storefront_name_col, additional_cols, abbreviation_mapping, tversky_alpha, tversky_beta, jaro_count):
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

    return data

@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        try:
            raw_file = request.files["raw_file"]
            abbreviation_file = request.files["abbreviation_file"]
            
            raw_df = pd.read_csv(raw_file)
            abbreviation_df = pd.read_csv(abbreviation_file)
        except Exception as e:
            flash(str(e), 'danger')
            return render_template("index.html", columns=None)

        abbreviation_mapping = dict(zip(abbreviation_df['abbrev'], abbreviation_df['abbreviation']))

        columns = raw_df.columns.tolist()
        return render_template("index.html", columns=columns, raw_df=raw_df.to_json(), abbreviation_mapping=json.dumps(abbreviation_mapping))

    return render_template("index.html", columns=None)

@app.route("/process", methods=["POST"])
def process():
    try:
        raw_df = pd.read_json(request.form["raw_df"])
        abbreviation_mapping = json.loads(request.form["abbreviation_mapping"])
        raw_brand_name_col = request.form["raw_brand_name_col"]
        storefront_brand_col = request.form["storefront_brand_col"]
        raw_name_col = request.form["raw_name_col"]
        storefront_name_col = request.form["storefront_name_col"]
        additional_col1 = request.form["additional_col1"]
        additional_col2 = request.form["additional_col2"]
        additional_col3 = request.form["additional_col3"]
        tversky_alpha = float(request.form["tversky_alpha"])
        tversky_beta = float(request.form["tversky_beta"])
        jaro_count = int(request.form["jaro_count"])

        additional_cols = [additional_col1, additional_col2, additional_col3]

        processed_data = process_data(raw_df, raw_brand_name_col, storefront_brand_col, raw_name_col, storefront_name_col, additional_cols, abbreviation_mapping, tversky_alpha, tversky_beta, jaro_count)
        
        if not os.path.exists(app.config['UPLOAD_FOLDER']):
            os.makedirs(app.config['UPLOAD_FOLDER'])
            
        output_file_path = os.path.join(app.config['UPLOAD_FOLDER'], "processed_output.csv")
        processed_data.to_csv(output_file_path, index=False)

        stats = {
            'brand_above_50': round((processed_data['brand_score'] > 0.50).mean() * 100, 2),
            'brand_below_50': round((processed_data['brand_score'] <= 0.50).mean() * 100, 2),
            'name_above_50': round((processed_data['name_score'] > 0.50).mean() * 100, 2),
            'name_below_50': round((processed_data['name_score'] <= 0.50).mean() * 100, 2),
            'needs_review_brand_percentage': round((processed_data['needs_review_brand'] == 'Y').mean() * 100, 2),
            'needs_review_name_percentage': round((processed_data['needs_review_name'] == 'Y').mean() * 100, 2),
            'classification_counts': processed_data['classified_type'].value_counts(normalize=True).mul(100).round(2).to_dict() if 'classified_type' in processed_data.columns else {}
        }

        return render_template("results.html", stats=stats, download_link="/download/processed_output.csv")
    except Exception as e:
        flash(str(e), 'danger')
        return redirect(url_for('index'))

@app.route("/download/<filename>")
def download_file(filename):
    response = send_from_directory(app.config['UPLOAD_FOLDER'], filename, as_attachment=True)
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response

@app.route("/reset", methods=["GET"])
def reset():
    return redirect(url_for('index'))

if __name__ == "__main__":
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    app.run(debug=True)
