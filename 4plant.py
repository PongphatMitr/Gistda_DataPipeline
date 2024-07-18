import geopandas as gpd
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from shapely.validation import make_valid

# List of file paths to the GeoJSON files
files = [
    r"C:\Users\pongp\Downloads\Gistda\Gistda_Intern\Test 6 Plant\Palm_65.geojson",
    r"C:\Users\pongp\Downloads\Gistda\Gistda_Intern\Test 6 Plant\Rubber_65.geojson",
    r"C:\Users\pongp\Downloads\Gistda\Gistda_Intern\Test 6 Plant\Sugarcane_65.geojson",
    r"C:\Users\pongp\Downloads\Gistda\Gistda_Intern\Test 6 Plant\Maize_65.geojson"
]

def read_and_process_file(file):
    print(f"Reading file {file}...")
    gdf = gpd.read_file(file)
    print(f"File {file} read successfully!")
    # Ensure the GeoDataFrame is in a projected CRS for buffering
    if gdf.crs.is_geographic:
        gdf = gdf.to_crs(epsg=3857)  # Use a common projected CRS like EPSG:3857
    gdf['geometry'] = gdf['geometry'].simplify(tolerance=0.001)
    gdf['geometry'] = gdf['geometry'].buffer(10)  # Buffer with a larger value in projected CRS
    # Clean and repair geometries
    gdf['geometry'] = gdf['geometry'].apply(make_valid)
    return gdf

# Read and process GeoJSON files concurrently
with ThreadPoolExecutor() as executor:
    futures = {executor.submit(read_and_process_file, file): file for file in files}
    gdfs = []
    for future in as_completed(futures):
        gdfs.append(future.result())

# Ensure CRS consistency, reproject back to geographic CRS if needed
common_crs = gdfs[0].crs
for gdf in gdfs:
    gdf.to_crs(common_crs, inplace=True)

# Union the geometries
print("Starting union operation...")
gdf_union = gdfs[0].copy()

for i, gdf in enumerate(tqdm(gdfs[1:], desc="Unioning geometries"), start=2):
    # Rename columns to avoid duplicates during overlay
    rename_dict = {col: f"{col}_{i}" for col in gdf.columns if col != 'geometry'}
    gdf = gdf.rename(columns=rename_dict)
    gdf_union = gpd.overlay(gdf_union, gdf, how='union')

print("Union operation complete!")

# Define the function to apply the conditions
def check_plants(row):
    name_columns = [col for col in row.index if 'Name' in col]
    names = [row[col] for col in name_columns if pd.notnull(row[col])]
    
    if not names:
        return "-"
    elif len(names) == 1:
        return names[0]
    else:
        return " // ".join([f"Attribute ({name})" for name in names])

# Apply the function to create the new column
gdf_union['Plant_Status'] = gdf_union.apply(check_plants, axis=1)

# Save the GeoDataFrame to a CSV file without the geometry column
output_csv_file = 'union_data_without_geometry_4_plant.csv'
gdf_union.drop(columns=['geometry']).to_csv(output_csv_file, index=False)
print(f"Union data without geometry saved to {output_csv_file}")

# Save to geojson
geojson_data = gdf_union.to_json()
output_geojson_file = 'union_data_4_plant.geojson'
with open(output_geojson_file, 'w') as f:
    f.write(geojson_data)
print(f"Union data saved to {output_geojson_file}")
