import re
import pandas as pd


def remove_special_characters(df):

    # Function to remove special characters
    df['Stripped_SPN'] = df["supplier_part_no"].apply(lambda x: re.sub(r'[^A-Za-z0-9]+', '', str(x)))
   
    return df


df = pd.read_excel("full_items_data_formatted.xlsx")

df = remove_special_characters(df)

df.to_excel("full_items_data_formatted.xlsx")