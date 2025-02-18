import csv

# Initialize an empty list to store the concatenated values
vector = []

# Open the CSV file
with open('unscaled_preds.csv', mode='r') as csv_file:
    csv_reader = csv.DictReader(csv_file)
    
    # Iterate over each row in the CSV
    for row in csv_reader:
        # Get the values from the 'borough_id' and 'community district' columns
        borough_id = row['borough_id']
        community_district = row['community_district']
        
        # Concatenate the values and convert to integer
        concatenated_value = int(f"{borough_id}{community_district}")
        
        # Append the concatenated value to the vector
        vector.append(concatenated_value)

# Print the resulting vector
print(vector)

