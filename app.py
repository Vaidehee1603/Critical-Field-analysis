from flask import Flask, request, jsonify
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import to_timestamp
import os

# Initialize Flask app
app = Flask(__name__)

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Twitter Data Processing") \
    .config("spark.ui.port", "4060") \
    .getOrCreate()


# Create a directory to store uploaded files
UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

@app.route('/')
def home():
    return "Upload your CSV file with Twitter data!"

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return "No file part", 400

    file = request.files['file']
    
    if file.filename == '':
        return "No selected file", 400

    # Save the file
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
    file.save(filepath)

    # Process the CSV file using PySpark
    df = process_csv(filepath)
    df.show()
    
    # Count mentions of "Mike loves big data" using SQL
    result = count_mentions_using_sql(df)

    
    # Return result as JSON
    return jsonify(result)

def process_csv(filepath):
    # Load CSV into a DataFrame using PySpark
    schema = "datetime STRING, time STRING, handle STRING, tweet STRING"
    
    # Load CSV into DataFrame with defined schema
    df = spark.read.csv(filepath, header=False, schema=schema)

    # Drop the datetime and time columns since they are not needed
    df = df.drop("datetime", "time")
    
    # Register DataFrame as a temporary SQL table
    df.createOrReplaceTempView("tweets")
    
    return df

def count_mentions_using_sql(df):
    # SQL query to count the number of mentions of "Mike loves big data" per handle
    query = """
    SELECT handle, COUNT(*) AS mention_count
    FROM tweets
    WHERE tweet LIKE '%MikeDoesBigData%'
    GROUP BY handle
    """
    
    # Execute the query
    result_df = spark.sql(query)

    result_df.show()
    
    # Collect the results into a list of dictionaries
    result = result_df.collect()
    
    # Convert the results to a dictionary for easy display
    result_dict = {row['handle']: row['mention_count'] for row in result}
    
    return result_dict

if __name__ == '__main__':
    app.run(debug=True, port=6001)
