import pandas as pd
import bcs_connector_orange_st
import bcs_connector_fullitems
import json
import re


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

    # Vectorized matching without merge
    def vectorized_matching(fulldf, supdf, match_column):
        # Create a dictionary for efficient lookup
        full_dict = {}
        for _, row in fulldf.iterrows():
            key = row[match_column]
            if key not in full_dict:
                full_dict[key] = []
            full_dict[key].append(row['s_id'])

        # Matching process
        for idx, sup_row in supdf.iterrows():
            match_value = sup_row[match_column]
            
            if match_value in full_dict:
                # Matching found
                matching_ids = full_dict[match_value]
                
                # Update supdf
                supdf.loc[idx, 'Matching_check'] = 'Matching'
                supdf.loc[idx, 'Matching_count'] = len(matching_ids)
                
                # Update specific columns based on match type
                if match_column == 'Supplier_part_no':
                    supdf.loc[idx, 'Matching_spn'] = match_value
                    supdf.loc[idx, 'Matching_spn_ids'] = ', '.join(map(str, matching_ids))
                    supdf.loc[idx, 'Matching_criteria'] = 'SPN matching'
                
                elif match_column == 'Stripped_SPN':
                    supdf.loc[idx, 'Matching_sspn'] = match_value
                    supdf.loc[idx, 'Matching_stripped_SPN_ids'] = ', '.join(map(str, matching_ids))
                    supdf.loc[idx, 'Matching_criteria'] = 'Stripped SPN matching'
                
                elif match_column == 'Item_id':
                    supdf.loc[idx, 'Matching_itemid'] = match_value
                    supdf.loc[idx, 'Matching_itemid_ids'] = ', '.join(map(str, matching_ids))
                    supdf.loc[idx, 'Matching_criteria'] = 'Item id matching'
                
                # Update corresponding fulldf rows
                for match_id in matching_ids:
                    full_mask = (fulldf['s_id'] == match_id)
                    
                    if match_column == 'Supplier_part_no':
                        fulldf.loc[full_mask, 'Matching_check'] = 'Matching'
                        fulldf.loc[full_mask, 'Matching_spn'] = match_value
                        fulldf.loc[full_mask, 'Matching_spn_ids'] = str(match_id)
                    
                    elif match_column == 'Stripped_SPN':
                        fulldf.loc[full_mask, 'Matching_check'] = 'Matching'
                        fulldf.loc[full_mask, 'Matching_stripped_SPN'] = match_value
                        fulldf.loc[full_mask, 'Matching_stripped_SPN_ids'] = str(match_id)
                    
                    elif match_column == 'Item_id':
                        fulldf.loc[full_mask, 'Matching_check'] = 'Matching'
                        fulldf.loc[full_mask, 'Matching_itemid'] = match_value
                        fulldf.loc[full_mask, 'Matching_itemid_ids'] = str(match_id)

        return fulldf, supdf

    # Perform matching for different columns
    fulldf, supdf = vectorized_matching(fulldf, supdf, 'Supplier_part_no')
    fulldf, supdf = vectorized_matching(fulldf, supdf, 'Stripped_SPN')
    fulldf, supdf = vectorized_matching(fulldf, supdf, 'Item_id')

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

    print(fulldf.head(2))
    print(supdf.head(2))
    
    supdf = supdf.rename(columns={"clean_sup_part_no" : "Stripped_SPN", "supplier_part_no" : "Supplier_part_no", "item_id" : "Item_id"})
    fulldf = fulldf.rename(columns={"supplier_part_no" : "Supplier_part_no", "item_id" : "Item_id"})
    fulldf = remove_special_characters(fulldf)

    print("Datasets have been processed, and is not sent to matching")
    fulldf, supdf = matching_items(fulldf, supdf)   
    fulldf.to_excel("V0_fullitems.xlsx", index=False)
    supdf.to_excel("V0_suppliers_data.xlsx", index=False)


if __name__ == "__main__":
    main()


## changes on the orange_st or the Sup_df
## need to change the clean_sup_part_no to stripped_SPN
## supplier_part_no
## item_id


## fulldf
## needs to have stripped SPN 
## supplier_part_no and item_id