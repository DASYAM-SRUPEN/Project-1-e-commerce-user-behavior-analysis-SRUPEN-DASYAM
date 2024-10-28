import pandas as pd
from collections import defaultdict

# File paths
user_activity_file = "input/user_activity.csv"
transactions_file = "input/transactions.csv"
output_file = "output/conversion_rates.csv"

# Load user activity and transactions data
user_activity_df = pd.read_csv(user_activity_file)
transactions_df = pd.read_csv(transactions_file)

# Step 1: Create a mapping of ProductID to ProductCategory from transactions data
product_category_map = transactions_df.set_index('ProductID')['ProductCategory'].to_dict()

# Step 2: Aggregate interactions from user activity data
category_interactions = defaultdict(lambda: {'interactions': 0, 'purchases': 0})

for _, row in user_activity_df.iterrows():
    product_id = row['ProductID']
    activity_type = row['ActivityType']
    
    # Get the category of the product
    category = product_category_map.get(product_id)
    if category:
        # Count as interaction if it's 'browse' or 'add_to_cart'
        if activity_type in ['browse', 'add_to_cart']:
            category_interactions[category]['interactions'] += 1
        elif activity_type == 'purchase':
            category_interactions[category]['purchases'] += 1

# Step 3: Calculate purchases from the transactions data
for _, row in transactions_df.iterrows():
    product_id = row['ProductID']
    category = row['ProductCategory']
    quantity_sold = row['QuantitySold']
    
    # Increment purchase count for the category
    category_interactions[category]['purchases'] += quantity_sold

# Step 4: Calculate conversion rate per category
conversion_rates = []
for category, counts in category_interactions.items():
    interactions = counts['interactions']
    purchases = counts['purchases']
    conversion_rate = purchases / interactions if interactions > 0 else 0
    conversion_rates.append([category, interactions, purchases, conversion_rate])

# Step 5: Save results to CSV
output_df = pd.DataFrame(conversion_rates, columns=['ProductCategory', 'Interactions', 'Purchases', 'ConversionRate'])
output_df.to_csv(output_file, index=False)

print(f"Conversion rate per category has been saved to {output_file}")
