# CSV Product Name Standardizer

This Flask application processes raw CSV files to standardize product names according to specific guidelines. The application allows users to upload a raw CSV file and an abbreviation list, then processes the files, provides similarity statistics, and a downloadable CSV output with the standardized product names.

## Features

- Upload raw CSV file and abbreviation list
- Standardize product names based on provided guidelines
- Configure similarity parameters for better accuracy
- View detailed similarity statistics
- Download processed CSV file

## Prerequisites

- Python 3.x
- pip (Python package installer)
- Virtual environment setup (optional but recommended)

## Installation (executed in your terminal)

1. **Create and change directory:**

   ```bash
   mkdir brand_and_name_audit_processor
   cd brand_and_name_audit_processor
   ```
2. **Create and activate a virtual environment:**

   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
   ```
3. **Install the dependencies:**

   ```bash
   pip install -r requirements.txt
   ```

## Running the Application

1. **Start the Flask application:**

   ```bash
   flask run
   ```
2. **Open your web browser and navigate to:**

   ```
   http://127.0.0.1:5000/
   ```
3. **Upload the raw CSV file and abbreviation list:**

   - Use the provided form to upload your raw CSV file and abbreviation list CSV file.
   - Configure the column selections and similarity parameters as needed.
   - Click the "Process" button to standardize the product names.
4. **View Results and Download Processed File:**

   - After processing, you will be redirected to a page showing detailed statistics.
   - Download the processed CSV file using the provided download link.

## Directory Structure

```
csv_processor/
├── app.py
├── requirements.txt
├── processed_files/
│   └── (processed files will be saved here)
├── static/
│   ├── instacart_logo.png
│   └── style.css
└── templates/
    ├── index.html
    └── results.html
```

## Dependencies

- Flask
- pandas
- re
- jellyfish
- tqdm

## Notes

- Ensure the `processed_files` directory exists in the project root to save the processed CSV files.
- Modify `app.py` to change the parameters or processing logic as needed.

## Troubleshooting

- **ModuleNotFoundError:** Ensure all dependencies are installed correctly by running `pip install -r requirements.txt` in your virtual environment.
- **502 Backend Error:** Check the Flask application logs for errors and ensure the virtual environment is activated correctly.
