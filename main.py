# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

import pandas as pd
import openpyxl
from numpy import dtype

# Read the parquet file
df = pd.read_parquet("veridion_product_deduplication_challenge.snappy.parquet", engine="pyarrow")

# Save it to CSV (headers are included by default)
df.to_csv("veridion_product_deduplication_challenge.csv", index=False)

# Read he CSV file
new_df = pd.read_csv("veridion_product_deduplication_challenge.csv")

null_columns = new_df.columns

# Group the dataset by "unspsc" column to remove the duplicates
deduped = new_df.groupby("unspsc").agg({
    "root_domain": lambda x: max(x, key=lambda item: len(str(item)) if isinstance(item, str) else 0),
    "page_url": lambda x: max(x, key=lambda item: len(str(item)) if isinstance(item, str) else 0),
    "product_title": lambda x: max(x, key=lambda item: len(str(item)) if isinstance(item, str) else 0),
    "product_summary": lambda x: max(x, key=lambda item: len(str(item)) if isinstance(item, str) else 0),
    "product_name": lambda x: max(x, key=lambda item: len(str(item)) if isinstance(item, str) else 0),
    "product_identifier": lambda x: max(x, key=lambda item: len(str(item)) if isinstance(item, str) else 0),
    "brand": lambda x: max(x, key=lambda item: len(str(item)) if isinstance(item, str) else 0),
    "intended_industries": lambda x: max(x, key=lambda item: len(str(item)) if isinstance(item, str) else 0),
    "applicability": lambda x: max(x, key=lambda item: len(str(item)) if isinstance(item, str) else 0),
    "eco_friendly": lambda x: max(x, key=lambda item: len(str(item)) if isinstance(item, str) else 0),
    "ethical_and_sustainability_practices": lambda x: max(x, key=len),
    "production_capacity": "max",
    "price": "min",
    "materials": lambda x: max(x, key=len),
    "ingredients": lambda x: max(x, key=len),
    "manufacturing_countries": lambda x: max(x, key=len),
    "manufacturing_year": "max",
    "manufacturing_type": lambda x: max(x, key=len),
    "customization": lambda x: max(x, key=len),
    "packaging_type": lambda x: max(x, key=len),
    "form": lambda x: max(x, key=len),
    "size": lambda x: max(x, key=len),
    "color": lambda x: max(x, key=len),
    "purity": lambda x: max(x, key=len),
    "energy_efficiency": lambda x: max(x, key=lambda item: len(str(item)) if isinstance(item, str) else 0),
    "pressure_rating": lambda x: max(x, key=len),
    "power_rating": lambda x: max(x, key=len),
    "quality_standards_and_certifications": lambda x: max(x, key=len),
    "miscellaneous_features": lambda x: max(x, key=len),
    "description": lambda x: max(x, key=lambda item: len(str(item)) if isinstance(item, str) else 0)
})

import ast

# Create the default values in case there are some null cells in database
default_pressure_rating_value = {'qualitative': False, 'type': None, 'unit': None, 'value': None}
default_product_identifier = {'Name': 'None'}
default_production_capacity = {'quantity': 0, 'time_frame': None, 'type': None, 'unit': None}
default_price = {'amount': 0, 'currency': None, 'type': None}
default_size = {'dimension': None, 'qualitative': False, 'type': None, 'unit': None, 'value': None}
default_color = {'original': None, 'simple': None}
default_purity = {'qualitative': False, 'type': None, 'unit': None, 'value': None}
default_power_rating_value = {'qualitative': False, 'type': None, 'unit': None, 'value': None}

# Return the non-null value for product identifiers
def clean_product_identifier(value):
    if value == "[]":
        return [default_product_identifier]
    if isinstance(value, str):
        try:
            value = ast.literal_eval(value)
        except:
            return value
    if value == []:
        return default_product_identifier
    return value

# Return the non-null value for pressure rating
def clean_pressure_rating(value):
    if value == "[]":
        return [default_pressure_rating_value]
    if isinstance(value, str):
        try:
            value = ast.literal_eval(value)
        except:
            return value
    if value == []:
        return default_pressure_rating_value
    return value

# Return the non-null value for power rating
def clean_power_rating(value):
    if value == "[]":
        return [default_power_rating_value]
    if isinstance(value, str):
        try:
            value = ast.literal_eval(value)
        except:
            return value
    if value == []:
        return default_power_rating_value
    return value

# Return the non-null value for applicability
def clean_applicability(value):
    if value == "[]":
        return [None]
    if isinstance(value, str):
        try:
            value = ast.literal_eval(value)
        except:
            return value
    if value == []:
        return None
    return value

# Return the non-null value for intended industries
def clean_intended_industries(value):
    if value == "[]":
        return [None]
    if isinstance(value, str):
        try:
            value = ast.literal_eval(value)
        except:
            return value
    if value == []:
        return None
    return value

# Return ethical and sustainabilityu practices
def clean_ethical_and_sustainability_practices(value):
    if value == "[]":
        return [None]
    if isinstance(value, str):
        try:
            value = ast.literal_eval(value)
        except:
            return value
    if value == []:
        return None
    return value

# Return the non-null value for production capacity
def clean_production_capacity(value):
    if value == "[]":
        return [default_production_capacity]
    if isinstance(value, str):
        try:
            value = ast.literal_eval(value)
        except:
            return value
    if value == []:
        return default_production_capacity
    return value

# Return the non-null value for price
def clean_price(value):
    if value == "[]":
        return [default_price]
    if isinstance(value, str):
        try:
            value = ast.literal_eval(value)
        except:
            return value
    if value == []:
        return default_price
    return value

# Return the non-null value for materials
def clean_materials(value):
    if value == "[]":
        return [None]
    if isinstance(value, str):
        try:
            value = ast.literal_eval(value)
        except:
            return value
    if value == []:
        return None
    return value

# Return the non-null value for clean ingredients
def clean_ingredients(value):
    if value == "[]":
        return [None]
    if isinstance(value, str):
        try:
            value = ast.literal_eval(value)
        except:
            return value
    if value == []:
        return None
    return value

# Return the non-null value for manufacturing countries
def clean_manufacturing_countries(value):
    if value == "[]":
        return [None]
    if isinstance(value, str):
        try:
            value = ast.literal_eval(value)
        except:
            return value
    if value == []:
        return None
    return value

# Return the non-null value for manufacturing types
def clean_manufacturing_type(value):
    if value == "[]":
        return [None]
    if isinstance(value, str):
        try:
            value = ast.literal_eval(value)
        except:
            return value
    if value == []:
        return None
    return value

# Return the non-null value for customizations
def clean_customization(value):
    if value == "[]":
        return [None]
    if isinstance(value, str):
        try:
            value = ast.literal_eval(value)
        except:
            return value
    if value == []:
        return None
    return value

# Return the non-null value for packaging types
def clean_packaging_type(value):
    if value == "[]":
        return [None]
    if isinstance(value, str):
        try:
            value = ast.literal_eval(value)
        except:
            return value
    if value == []:
        return None
    return value

# Return the non-null value for forms
def clean_form(value):
    if value == "[]":
        return [None]
    if isinstance(value, str):
        try:
            value = ast.literal_eval(value)
        except:
            return value
    if value == []:
        return None
    return value

# Return the non-null value for sizes
def clean_size(value):
    if value == "[]":
        return [default_size]
    if isinstance(value, str):
        try:
            value = ast.literal_eval(value)
        except:
            return value
    if value == []:
        return default_size
    return value

# Return the non-null value for colors
def clean_color(value):
    if value == "[]":
        return [default_color]
    if isinstance(value, str):
        try:
            value = ast.literal_eval(value)
        except:
            return value
    if value == []:
        return default_color
    return value

# Return the non-null value for purity
def clean_purity(value):
    if value == "[]":
        return [default_purity]
    if isinstance(value, str):
        try:
            value = ast.literal_eval(value)
        except:
            return value
    if value == []:
        return default_purity
    return value

# Return the non-null value for quality standards and certifications
def clean_quality_standards_and_certifications(value):
    if value == "[]":
        return [None]
    if isinstance(value, str):
        try:
            value = ast.literal_eval(value)
        except:
            return value
    if value == []:
        return None
    return value

# Return the non-null value for miscellaneous features
def clean_miscellaneous_features(value):
    if value == "[]":
        return [None]
    if isinstance(value, str):
        try:
            value = ast.literal_eval(value)
        except:
            return value
    if value == []:
        return None
    return value

# Apply the methods over the columns of the dataset
deduped["root_domain"] = deduped["root_domain"].fillna('None')
deduped["page_url"] = deduped["page_url"].fillna('None')
deduped["product_title"] = deduped["page_url"].fillna('None')
deduped["product_summary"] = deduped["product_summary"].fillna('None')
deduped["product_name"] = deduped["product_name"].fillna('None')
deduped["product_identifier"] = deduped["product_identifier"].apply(clean_product_identifier)
deduped["brand"] = deduped["brand"].fillna('None')
deduped["intended_industries"] = deduped["intended_industries"].apply(clean_intended_industries)
deduped["applicability"] = deduped["applicability"].apply(clean_applicability)
deduped["ethical_and_sustainability_practices"] = deduped["ethical_and_sustainability_practices"]\
                                                        .apply(clean_ethical_and_sustainability_practices)
deduped["production_capacity"] = deduped["production_capacity"].apply(clean_production_capacity)
deduped["price"] = deduped["price"].apply(clean_price)
deduped["materials"] = deduped["materials"].apply(clean_materials)
deduped["ingredients"] = deduped["ingredients"].apply(clean_ingredients)
deduped["manufacturing_countries"] = deduped["manufacturing_countries"].apply(clean_manufacturing_countries)
deduped["manufacturing_year"] = deduped["manufacturing_year"].fillna(-1)
deduped["manufacturing_type"] = deduped["manufacturing_type"].apply(clean_manufacturing_type)
deduped["customization"] = deduped["customization"].apply(clean_customization)
deduped["packaging_type"] = deduped["packaging_type"].apply(clean_packaging_type)
deduped["form"] = deduped["form"].apply(clean_form)
deduped["size"] = deduped["size"].apply(clean_size)
deduped["color"] = deduped["color"].apply(clean_color)
deduped["purity"] = deduped["purity"].apply(clean_purity)
deduped["eco_friendly"] = deduped["eco_friendly"].fillna(False)
deduped["energy_efficiency"] = deduped["energy_efficiency"].fillna(False)
deduped["pressure_rating"] = deduped["pressure_rating"].apply(clean_pressure_rating)
deduped["power_rating"] = deduped["power_rating"].apply(clean_power_rating)
deduped["quality_standards_and_certifications"] = deduped["quality_standards_and_certifications"].apply(clean_quality_standards_and_certifications)
deduped["miscellaneous_features"] = deduped["miscellaneous_features"].apply(clean_miscellaneous_features)
deduped["description"] = deduped["description"].fillna('None')

# Save the new dataset on .CSV format
deduped.to_csv("file_without_duplicates.csv")

# Save the new dataset on Excel format
deduped.to_excel('file_with_no_duplicates.xlsx')