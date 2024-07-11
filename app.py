from flask import Flask, render_template, request, send_file
import pandas as pd
import re
import jellyfish
from polyleven import levenshtein
from tqdm import tqdm
import os
import json

app = Flask(__name__)

def enhanced_clean_text(text):
    text = re.sub(r'\$\d+(\.\d{1,2})?', '', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def clean_and_translate_raw_name(raw_name, brand_name, abbreviation_mapping):
    brand_name = str(brand_name) if pd.notna(brand_name) else ''
    clean_name = enhanced_clean_text(raw_name)
    for abbr, full_form in abbreviation_mapping.items():
        clean_name = re.sub(r'\b{}\b'.format(re.escape(abbr)), full_form, clean_name, flags=re.IGNORECASE)
    clean_name = re.sub(r'\b{}\b'.format(re.escape(brand_name)), '', clean_name, flags=re.IGNORECASE).strip()
    return clean_name.title()

def recommend_storefront_name(row, raw_name_col, storefront_brand_col, abbreviation_mapping):
    raw_name = row[raw_name_col]
    brand_name = row[storefront_brand_col]
    clean_name = clean_and_translate_raw_name(raw_name, brand_name, abbreviation_mapping)
    recommended_name = f"{brand_name} {clean_name}"
    return recommended_name

def process_data(data, raw_brand_name_col, storefront_brand_col, raw_name_col, storefront_name_col, additional_cols, abbreviation_mapping, tversky_alpha, tversky_beta, jaro_count):
    data[storefront_brand_col] = data[storefront_brand_col].astype(str).fillna('')

    tqdm.pandas(desc="Processing rows")
    data['cleaned_raw_name'] = data.progress_apply(lambda row: clean_and_translate_raw_name(row[raw_name_col], row[storefront_brand_col], abbreviation_mapping), axis=1)
    data['recommended_storefront_name'] = data.progress_apply(lambda row: recommend_storefront_name(row, raw_name_col, storefront_brand_col, abbreviation_mapping), axis=1)

    data['separated_brand_name'] = data[storefront_brand_col]
    data['separated_storefront_name'] = data.progress_apply(lambda row: row['recommended_storefront_name'].replace(row[storefront_brand_col], '').strip(), axis=1)

    data['changes_needed'] = data.progress_apply(lambda row: 'Y' if row['separated_storefront_name'] != row[storefront_name_col] else 'N', axis=1)

    def clean_text(text):
        return re.sub(r'[^a-zA-Z0-9\s]', '', text).lower()

    stopwords = set(['and', 'or', 'the', 'a', 'an', 'but', 'is', 'in', 'to', 'for', 'with', 'on', 'that', 'by', 'at', 'from'])

    def capitalize_except_stopwords(text):
        words = text.split()
        capitalized_words = [word.title() if word.lower() not in stopwords else word.lower() for word in words]
        return ' '.join(capitalized_words)

    def tversky_similarity(a, b, alpha=tversky_alpha, beta=tversky_beta):
        if pd.isna(a) and pd.isna(b):
            return 1.0
        a = clean_text(str(a)) if not pd.isna(a) else ''
        b = clean_text(str(b)) if not pd.isna(b) else ''
        set_a = set(a)
        set_b = set(b)
        intersection = len(set_a & set_b)
        differences_a = len(set_a - set_b)
        differences_b = len(set_b - set_a)
        total = intersection + alpha * differences_a + beta * differences_b
        return round(0 if total == 0 else intersection / total, 2)

    def jaro_winkler_reassessment(a, b, count=jaro_count):
        if pd.isna(a) or pd.isna(b):
            return 0.0
        a, b = clean_text(a)[:count], clean_text(b)[:count]
        return jellyfish.jaro_winkler_similarity(a, b)

    data['brand_score'] = data.progress_apply(lambda x: tversky_similarity(x[storefront_brand_col], x[storefront_brand_col]), axis=1)
    data['name_score'] = data.progress_apply(lambda x: tversky_similarity(x[raw_name_col], x['separated_storefront_name']), axis=1)

    data['reassessed_score'] = data.progress_apply(lambda x: round(jaro_winkler_reassessment(x[storefront_brand_col], x[raw_name_col]), 2) 
                                          if x['brand_score'] < 0.51 else x['brand_score'], axis=1)

    data['brand_score'] = data['reassessed_score'].round(2)
    data.drop(columns=['reassessed_score'], inplace=True)
    
    if additional_cols[0]:
        data['additional_column_1'] = data[additional_cols[0]]
    if additional_cols[1]:
        data['additional_column_2'] = data[additional_cols[1]]
    
    return data

@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        raw_file = request.files["raw_file"]
        abbreviation_file = request.files["abbreviation_file"]

        raw_df = pd.read_csv(raw_file)
        abbreviation_df = pd.read_csv(abbreviation_file)
        abbreviation_mapping = dict(zip(abbreviation_df['abbrev'], abbreviation_df['abbreviation']))

        columns = raw_df.columns.tolist()
        return render_template("index.html", columns=columns, raw_df=raw_df.to_json(), abbreviation_mapping=json.dumps(abbreviation_mapping))

    return render_template("index.html", columns=None)

@app.route("/process", methods=["POST"])
def process():
    raw_df = pd.read_json(request.form["raw_df"])
    abbreviation_mapping = json.loads(request.form["abbreviation_mapping"])
    raw_brand_name_col = request.form["raw_brand_name_col"]
    storefront_brand_col = request.form["storefront_brand_col"]
    raw_name_col = request.form["raw_name_col"]
    storefront_name_col = request.form["storefront_name_col"]
    additional_col1 = request.form["additional_col1"]
    additional_col2 = request.form["additional_col2"]
    tversky_alpha = float(request.form["tversky_alpha"])
    tversky_beta = float(request.form["tversky_beta"])
    jaro_count = int(request.form["jaro_count"])

    additional_cols = [additional_col1, additional_col2]

    processed_data = process_data(raw_df, raw_brand_name_col, storefront_brand_col, raw_name_col, storefront_name_col, additional_cols, abbreviation_mapping, tversky_alpha, tversky_beta, jaro_count)
    output_file_path = "processed_output.csv"
    processed_data.to_csv(output_file_path, index=False)

    return send_file(output_file_path, as_attachment=True, attachment_filename="processed_output.csv")

if __name__ == "__main__":
    app.run(debug=True)
