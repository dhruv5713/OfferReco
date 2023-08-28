# Google BigQuery Data Processing

This script demonstrates how to process and analyze data using Google BigQuery. It involves data extraction, transformation, and loading (ETL) operations to generate valuable insights. The code is structured to run in a time zone-specific manner, particularly for the "Asia/Singapore" time zone.

## Basic Data Extraction and Processing

The script starts by extracting data from Google Analytics using SQL queries in BigQuery. It retrieves online user activity data, including information like date, external ID, client ID, coupon details, time spent on the page, and coupon redemption status.

### Online Data Extraction Query

The `basic_data_query` query is constructed dynamically with parameters for `start_date` and `end_date`. It retrieves relevant online data, excluding certain coupon types, and compiles it into a structured format.

The script uses the Google Cloud `bigquery` library to connect to the BigQuery service and fetches the extracted data into a Pandas DataFrame. The extracted data is then uploaded to BigQuery into a table named `coupon_reco_v2.basic_history_data_v1`. The table schema is defined, and if data already exists in the table, it is replaced with the new data.

## Rating Calculation and Data Export

After extracting basic history data, the script calculates a rating for each user based on their interaction with coupons. The rating is determined using a combination of time spent on the page and coupon redemption activity. Users who spend more time and redeem coupons receive higher ratings.

### Rating Calculation Query

The `rating_data_query` is constructed to calculate ratings for users. It considers user behavior over the past seven days and assigns a rating value based on the time spent on pages and coupon redemption activity. The ratings are calculated using a weighted average approach.

The calculated rating data is fetched using the `bigquery` library and converted into a Pandas DataFrame. It's then exported as a CSV file named "rating_coupon_data.csv."

Additionally, the calculated rating data is uploaded to BigQuery into a table named `coupon_reco_v2.`. Similar to the basic history data, the table schema is defined, and existing data is replaced if applicable.

## Usage and Customization

1. Ensure you have the necessary Google Cloud credentials and the `google-cloud-bigquery` Python library installed.

2. Adjust the `start_date` and `end_date` to specify the desired time range for data extraction.

3. Customize the table schema and locations when uploading data to BigQuery as needed.

4. Run the script to extract, process, and load data into BigQuery.

## License

This code is provided under the [MIT License](LICENSE). Feel free to modify and use it for your purposes.
