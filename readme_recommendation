# Collaborative Filtering Recommendation System

This script demonstrates the creation of a collaborative filtering recommendation system using the LightFM library. Collaborative filtering analyzes user-item interactions to generate personalized recommendations for users. The script utilizes Google BigQuery for data extraction and storage and LightFM for model training and prediction.

## Data Extraction

The script starts by reading the `rating_coupon_data.csv` file, which contains user-coupon interactions with ratings. The script then prepares the data for model training.

## Data Preprocessing

The data is preprocessed by converting ratings to integers, splitting it into training and testing sets, and creating user-item interaction matrices. The `LightFM` model is then trained on the training set.

## Model Evaluation

The trained model's performance is evaluated using the `recall_at_k` metric on the test set. The `recall_at_k` metric measures the fraction of relevant items that were successfully recommended to users.

## Recommendation Generation

A function called `collabrative_recommendation` is defined to generate recommendations for a given user. The function calculates the recommendations based on the trained model's predictions. The recommendations are returned as a list of coupon titles.

## Generating Recommendations

The script iterates through user IDs to generate recommendations for each user. The recommendations are stored in a DataFrame named `df_rec`. The generated recommendations are then exported to a CSV file named "predicted_rec_1_aug_v2.csv."

## Adding Category Information

Category information for recommended coupons is added using the "with_cat.csv" file. The script reads the category information and matches it with the recommended coupons. The final recommendations, including category information, are stored in a new CSV file named "final_recommendations_with_cat.csv."

## Data Export to BigQuery

The final recommendations, including category information, are uploaded to Google BigQuery. The DataFrame is exported to the table `coupon_reco_v2.prod_prediction_reco_v2_2`.

## Additional Steps

The script further refines the recommendations by randomly selecting one recommendation from the top ones for each user. The script then adds category information to these refined recommendations. The final refined recommendations are stored in a CSV file named "Final_reco_random.csv."

## Usage and Customization

1. Ensure you have the necessary Google Cloud credentials and the required libraries (`pandas`, `numpy`, `matplotlib`, `lightfm`, `scipy`, `sklearn`, `seaborn`, etc.) installed.

2. Adjust file paths and table names as per your data storage and naming conventions.

3. Run the script to execute the collaborative filtering recommendation process.

## License

This code is provided under the [MIT License](LICENSE). You are free to modify and use it for your purposes.
