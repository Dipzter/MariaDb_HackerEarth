# generate_embeddings.py
from sentence_transformers import SentenceTransformer
import pandas as pd

# 1. Load the pre-trained model
# This will download the model 'all-MiniLM-L6-v2' the first time you run it
model = SentenceTransformer('all-MiniLM-L6-v2')

# 2. Load the OpenFlights airports data
# Download airports.dat from: https://github.com/mariaDB/openflights
# Note: This file is tab-separated and has no header
column_names = ['id', 'name', 'city', 'country', 'iata', 'icao', 'lat', 'lon', 'alt', 'timezone', 'dst', 'tz', 'type', 'source']
df_airports = pd.read_csv('airports.dat', names=column_names, header=None, sep=',')

# 3. Create a combined text field for each airport
# We combine name, city, and country for the model to understand the context
df_airports['combined_text'] = df_airports['name'] + ", " + df_airports['city'] + ", " + df_airports['country']

# 4. Generate embeddings for all airports
print("Generating embeddings for all airports... (This may take a minute)")
# This line does all the work: it passes the list of combined texts to the model
embeddings = model.encode(df_airports['combined_text'].tolist())

# 5. Add the embeddings back to the DataFrame
# We convert the numpy arrays to lists for easier storage
df_airports['embedding'] = embeddings.tolist()

# 6. Print the first airport and its embedding to verify
print("\nFirst airport in the dataset:")
print(df_airports[['name', 'city', 'country', 'combined_text']].iloc[0])
print(f"\nIts embedding vector (first 10 of {len(embeddings[0])} dimensions):")
print(embeddings[0][:10])

# 7. Save the DataFrame with embeddings to a CSV for the next step
# This is optional but helpful for debugging
df_airports.to_csv('airports_with_embeddings.csv', index=False)
print("\nSaved airports with embeddings to 'airports_with_embeddings.csv'")