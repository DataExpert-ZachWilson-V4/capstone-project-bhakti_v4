import pandas as pd
import ast

# Path to the CSV file
csv_file = '/Users/bhakti/Downloads/Git/capstone-project-bhakti_v4/sample-data/Childcarecentres.csv'

# Load the CSV file into a DataFrame
df = pd.read_csv(csv_file)
print(df)

# Drop the columns 'ward', 'phone', and 'run_date'
df = df.drop(columns=['ward', 'PHONE', 'run_date'])

# Print the DataFrame
print(df)

df['PCODE'] = df['PCODE'].str[:3]
print(df)
def get_coordinates(x, index):
    try:
        x = ast.literal_eval(x)
        return x['coordinates'][0][index] if 'coordinates' in x else None
    except (ValueError, SyntaxError):
        return None

df['Longitude'] = df['geometry'].apply(lambda x: get_coordinates(x, 0))
df['Latitude'] = df['geometry'].apply(lambda x: get_coordinates(x, 1))
print(df)
df = df.drop(columns=['geometry'])
df = df.rename(columns={'PCODE': 'PostalCode'})
df.to_csv('/Users/bhakti/Downloads/Git/capstone-project-bhakti_v4/sample-data/Childcarecentresnew.csv', index=False)




