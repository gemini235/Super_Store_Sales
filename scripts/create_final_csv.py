# Script: create_final_csv.py
# Purpose: Read the transformed and engineered dataset
#          and create separate CSV files for dimension and fact tables
#          to be ready for loading into a SQL Server database using BULK INSERT.

import pandas as pd
import numpy as np  # Needed if we recalculate anything
import os

# --- Setting up paths (Important: Modify these paths to suit your environment)---

# The primary path where the intermediate files are located
base_input_path = "E:\\Super_Store_Sales\\csv_files"
transformed_file_name = "Superstore_Sales_Dataset_Transformed.csv"
transformed_file_path = os.path.join(base_input_path, transformed_file_name)

output_path = "E:\Super_Store_Sales\csv_files\data_model_csv_files"

# Ensure that the output folder exists, and create it if it does not exist.
if not os.path.exists(output_path):
    try:
        os.makedirs(output_path)
        print(f"The output folder has been created: {output_path}")
    except OSError as e:
        print(f"Error creating output folder {output_path}: {e}")
        print("Please check the path and permissions.")
        exit()

# --- 2. Load the transformed data ---
print(f"[*] Loading the transformed data from: {transformed_file_path}")
try:
    df = pd.read_csv(transformed_file_path)
    print(f"    > Successfully loaded {len(df)} rows.")

    # Reconvert date columns to datetime if they were read as text
    if 'Order Date' in df.columns:
        df['Order Date'] = pd.to_datetime(df['Order Date'], errors='coerce')
    if 'Ship Date' in df.columns:
        df['Ship Date'] = pd.to_datetime(df['Ship Date'], errors='coerce')

    # Check for errors in converting the essential date columns
    if df['Order Date'].isnull().any() or df['Ship Date'].isnull().any():
        print("    > Warning: There are null values (NaT) in the date columns after loading/conversion.")

except FileNotFoundError:
    print(f"[!] Error: The file {transformed_file_path} was not found.")
    print("    Make sure to run the data transformation and feature engineering script first.")
    exit()
except Exception as e:
    print(f"[!] Unexpected error while loading the file: {e}")
    exit()

# --- 3. Create and save dimension files ---

# 3.1 Customer dimension file (DimCustomer)
print("\n[*] Creating customers.csv...")
customer_cols = ['Customer ID', 'Customer Name', 'Segment', 'Country', 'City', 'State', 'Postal Code', 'Region']
# Ensure the required columns exist before proceeding
if all(col in df.columns for col in customer_cols):
    # Use drop_duplicates to ensure a unique record for each customer based on Customer ID
    customers_df = df[customer_cols].drop_duplicates(subset=['Customer ID']).reset_index(drop=True)
    customers_output_path = os.path.join(output_path, "customers.csv")
    try:
        customers_df.to_csv(customers_output_path, index=False, encoding='utf-8-sig')
        print(f"    > Saved {len(customers_df)} records to: {customers_output_path}")
    except Exception as e:
        print(f"[!] Error while saving customers.csv: {e}")
else:
    print("[!] Error: Cannot create customers.csv - some required columns are missing in the source file.")

# 3.2 Product dimension file (DimProduct)
print("\n[*] Creating products.csv...")
product_cols = ['Product ID', 'Product Name', 'Category', 'Sub-Category']
if all(col in df.columns for col in product_cols):
    # Use drop_duplicates to ensure a unique record for each product based on Product ID
    products_df = df[product_cols].drop_duplicates(subset=['Product ID']).reset_index(drop=True)
    products_output_path = os.path.join(output_path, "products.csv")
    try:
        products_df.to_csv(products_output_path, index=False, encoding='utf-8-sig')
        print(f"    > Saved {len(products_df)} records to: {products_output_path}")
    except Exception as e:
        print(f"[!] Error while saving products.csv: {e}")
else:
    print("[!] Error: Cannot create products.csv - some required columns are missing in the source file.")

# 3.3 Shipping mode dimension file (DimShipMode)
print("\n[*] Creating DimShipMode.csv...")
ship_mode_col = 'Ship Mode'
if ship_mode_col in df.columns:
    ship_modes_df = pd.DataFrame(df[ship_mode_col].unique(), columns=[ship_mode_col])
    ship_modes_df = ship_modes_df.dropna().reset_index(drop=True)  # Remove any potential null values
    ship_modes_output_path = os.path.join(output_path, "DimShipMode.csv")
    try:
        ship_modes_df.to_csv(ship_modes_output_path, index=False, encoding='utf-8-sig')
        print(f"    > Saved {len(ship_modes_df)} records to: {ship_modes_output_path}")
    except Exception as e:
        print(f"[!] Error while saving DimShipMode.csv: {e}")
else:
    print("[!] Warning: Cannot create DimShipMode.csv - 'Ship Mode' column is missing.")

# --- 4. Create and save the combined fact table file (FactSales) ---
print("\n[*] Creating the combined FactSales.csv file...")

# List of required columns for the FactSales table (must match the columns in the BULK INSERT command)
fact_sales_sql_columns = [
    'OrderID', 'CustomerID', 'ProductID', 'OrderDate', 'ShipDate', 'ShipMode',
    'Sales', 'Order_Year', 'Order_Month', 'Order_Day', 'Order_Weekday',
    'Shipping_Delay', 'Log_Sales', 'Scaled_Sales'
]

# Corresponding column names in the dataframe (adjust names here if they differ)
fact_sales_df_columns = [
    'Order ID', 'Customer ID', 'Product ID', 'Order Date', 'Ship Date', 'Ship Mode',
    'Sales', 'Order_Year', 'Order_Month', 'Order_Day', 'Order_Weekday',
    'Shipping_Delay', 'Log_Sales', 'Scaled_Sales'
]

# Ensure all required columns exist in the dataframe
missing_cols = [col for col in fact_sales_df_columns if col not in df.columns]
if missing_cols:
    print(f"[!] Error: Cannot create FactSales.csv - the following columns are missing in the source file: {missing_cols}")
    print(f"    Available columns are: {df.columns.tolist()}")
else:
    # Select the required columns
    fact_sales_df = df[fact_sales_df_columns].copy() 

    # Rename the columns in the dataframe to match the SQL table column names exactly
    rename_map = dict(zip(fact_sales_df_columns, fact_sales_sql_columns))
    fact_sales_df.rename(columns=rename_map, inplace=True)
    print(f"    > Selected {len(fact_sales_sql_columns)} columns for FactSales.")

    # Format date columns to 'YYYY-MM-DD' to ensure compatibility with BULK INSERT
    print("    > Formatting date columns to 'YYYY-MM-DD'...")
    try:
        fact_sales_df['OrderDate'] = fact_sales_df['OrderDate'].dt.strftime('%Y-%m-%d')
        fact_sales_df['ShipDate'] = fact_sales_df['ShipDate'].dt.strftime('%Y-%m-%d')
    except AttributeError:
         print("[!] Warning: Failed to convert date format. Ensure they are of datetime type.")
    except Exception as e:
         print(f"[!] Unexpected error while formatting dates: {e}")

    # Save the final file
    # Note: Typically, we do not use drop_duplicates on fact tables unless there is a very specific reason
    fact_sales_output_path = os.path.join(output_path, "FactSales.csv")
    try:
        fact_sales_df.to_csv(fact_sales_output_path, index=False, encoding='utf-8-sig')
        print(f"    > Saved {len(fact_sales_df)} records to: {fact_sales_output_path}")
    except Exception as e:
        print(f"[!] Error while saving FactSales.csv: {e}")

# --- 5. Completion ---
print("\n[*] Final CSV files for loading have been successfully created.")