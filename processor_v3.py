import pandas as pd
import bcs_connector_orange_st
import bcs_connector_fullitems
import json
import re
import numpy as np

def read_full_items():

    """df = bcs_connector_fullitems.reader_df()
    
    df.to_excel("full_items_data.xlsx", index=False)"""

    df = pd.read_excel("full_items_data.xlsx")

    print("Read the full suppliers data")

    return df


def read_supplier_data(suppliers):

    """full_extracted_df = pd.DataFrame()

    for sup in suppliers:
        df = bcs_connector_orange_st.reader_df(sup)
        full_extracted_df = pd.concat([full_extracted_df, df], axis=0)
        

    full_extracted_df.to_excel("full_suppliers_data.xlsx", index=False)"""

    full_extracted_df = pd.read_excel("full_suppliers_data.xlsx")

    print("Read the suppliers data")
    
    return full_extracted_df


def remove_special_characters(df):

    # Function to remove special characters
    df['Stripped_SPN'] = df["Supplier_part_no"].apply(lambda x: re.sub(r'[^A-Za-z0-9 ]+', '', str(x)))
   
    return df


def column_adder(df, suffix):
    # Add 's_id' column with sequential unique numbers as string ending with "P"
    df['s_id'] = [str(i) + str(suffix) for i in range(1, len(df) + 1)]

    return df


def matching_items(fulldf, supdf):
    # Create copies to avoid SettingWithCopyWarning
    print("Proceeding with matching!!")
    fulldf = fulldf.copy()
    supdf = supdf.copy()

    # Initialize columns for 'fulldf'
    matching_columns = [
        "Matching_check", "Matching_spn", "Matching_stripped_SPN", 
        "Matching_itemid", "Matching_count", "Matching_spn_ids", 
        "Matching_stripped_SPN_ids", "Matching_itemid_ids"
    ]
    for col in matching_columns:
        fulldf[col] = "Not matching"
    fulldf["Matching_count"] = 0

    # Initialize columns for 'supdf'
    sup_matching_columns = [
        "Matching_check", "Matching_count", "Matching_criteria", 
        "Matching_spn", "Matching_sspn", "Matching_itemid", 
        "Matching_spn_ids", "Matching_stripped_SPN_ids", "Matching_itemid_ids"
    ]
    for col in sup_matching_columns:
        supdf[col] = "Not matching"
    supdf["Matching_count"] = 0

    # Extremely fast matching function using NumPy
    def ultra_fast_matching(fulldf, supdf, match_column):
        # Convert columns to NumPy arrays for fastest comparison
        full_values = fulldf[match_column].to_numpy()
        sup_values = supdf[match_column].to_numpy()
        
        # Create boolean mask for matching
        match_mask = np.isin(sup_values, full_values)
        
        # Get indices of matching rows
        if match_mask.any():
            # Matching supplier rows
            matching_sup_indices = np.where(match_mask)[0]
            
            # Update supdf for matching rows
            for idx in matching_sup_indices:
                # Find matching rows in fulldf
                full_match_mask = fulldf[match_column] == supdf.loc[supdf.index[idx], match_column]
                matching_full_rows = fulldf[full_match_mask]
                
                # Update supdf
                supdf.loc[supdf.index[idx], 'Matching_check'] = 'Matching'
                supdf.loc[supdf.index[idx], 'Matching_count'] = len(matching_full_rows)
                
                # Determine matching criteria and column names
                if match_column == 'Supplier_part_no':
                    criteria = 'SPN matching'
                    sup_match_col = 'Matching_spn'
                    sup_ids_col = 'Matching_spn_ids'
                    full_match_col = 'Matching_spn'
                elif match_column == 'Stripped_SPN':
                    criteria = 'Stripped SPN matching'
                    sup_match_col = 'Matching_sspn'
                    sup_ids_col = 'Matching_stripped_SPN_ids'
                    full_match_col = 'Matching_stripped_SPN'
                else:  # Item_id
                    criteria = 'Item id matching'
                    sup_match_col = 'Matching_itemid'
                    sup_ids_col = 'Matching_itemid_ids'
                    full_match_col = 'Matching_itemid'
                
                # Update supdf specific columns
                supdf.loc[supdf.index[idx], 'Matching_criteria'] = criteria
                supdf.loc[supdf.index[idx], sup_match_col] = supdf.loc[supdf.index[idx], match_column]
                supdf.loc[supdf.index[idx], sup_ids_col] = ', '.join(map(str, matching_full_rows['s_id'].tolist()))
                
                # Update fulldf for matching rows
                fulldf.loc[full_match_mask, 'Matching_check'] = 'Matching'
                fulldf.loc[full_match_mask, full_match_col] = supdf.loc[supdf.index[idx], match_column]
                fulldf.loc[full_match_mask, full_match_col + '_ids'] = ', '.join(map(str, matching_full_rows['s_id'].tolist()))
        
        return fulldf, supdf

    # Perform matching for different columns
    fulldf, supdf = ultra_fast_matching(fulldf, supdf, 'Supplier_part_no')
    fulldf, supdf = ultra_fast_matching(fulldf, supdf, 'Stripped_SPN')
    fulldf, supdf = ultra_fast_matching(fulldf, supdf, 'Item_id')

    # Recalculate total match count for fulldf
    fulldf['Matching_count'] = (
        (fulldf['Matching_spn'] != 'Not matching').astype(int) + 
        (fulldf['Matching_stripped_SPN'] != 'Not matching').astype(int) + 
        (fulldf['Matching_itemid'] != 'Not matching').astype(int)
    )

    return fulldf, supdf


def main():

    with open("D:\\Price_mapping_automation\\suppliers.json", 'r+') as f:
        suppliers_data = json.load(f)

    # Extract all the 'supplier_id' values into a list
    supplier_ids = [supplier["supplier_id"] for supplier in suppliers_data]
    print(supplier_ids)

    fulldf = read_full_items()
    supdf = read_supplier_data(supplier_ids)

    print("Datasets that has been read is now modified and columns are added")
    fulldf = column_adder(fulldf, "F")
    supdf = column_adder(supdf, "S")

    
    supdf = supdf.rename(columns={"clean_sup_part_no" : "Stripped_SPN", "supplier_part_no" : "Supplier_part_no", "item_id" : "Item_id"})
    fulldf = fulldf.rename(columns={"supplier_part_no" : "Supplier_part_no", "item_id" : "Item_id"})
    fulldf = remove_special_characters(fulldf)

    print("Datasets have been processed, and is not sent to matching")
    fulldf, supdf = matching_items(fulldf, supdf)   
    fulldf.to_excel("V3_fullitems.xlsx", index=False)
    supdf.to_excel("V3_suppliers_data.xlsx", index=False)


if __name__ == "__main__":
    main()


## changes on the orange_st or the Sup_df
## need to change the clean_sup_part_no to stripped_SPN
## supplier_part_no
## item_id


## fulldf
## needs to have stripped SPN 
## supplier_part_no and item_id