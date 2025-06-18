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

    print("Procedding with matching!!")
    fulldf = fulldf.copy()
    supdf = supdf.copy()

    # Initialize columns for 'fulldf'
    fulldf["Matching_check"] = "Not matching"
    fulldf["Matching_spn"] = "Not matching"
    fulldf["Matching_stripped_SPN"] = "Not matching"
    fulldf["Matching_itemid"] = "Not matching"
    fulldf["Matching_count"] = 0
    fulldf["Matching_spn_ids"] = "Not Matching"
    fulldf["Matching_stripped_SPN_ids"] = "Not Matching"
    fulldf["Matching_itemid_ids"] = "Not Matching"

    # Initialize columns for 'supdf'
    supdf["Matching_check"] = "Not matching"
    supdf["Matching_count"] = 0
    supdf["Matching_criteria"] = "Not matching"
    supdf["Matching_spn"] = "Not matching"
    supdf["Matching_sspn"] = "Not matching"
    supdf["Matching_itemid"] = "Not matching"
    supdf["Matching_spn_ids"] = "Not Matching"
    supdf["Matching_stripped_SPN_ids"] = "Not Matching"
    supdf["Matching_itemid_ids"] = "Not Matching"

    # Iterate through each row in supdf
    for i, r in supdf.iterrows():
        supspn = r["Supplier_part_no"]
        supsspn = r["Stripped_SPN"]
        supitemid = r["Item_id"]
        supid = r["s_id"]

        # Reset tracking variables for each supplier row
        matching_count = 0
        matching_criteria = []
        matching_spn = []
        matching_sspn = []
        matching_itemid = []

        # ID tracking lists
        sid_spn = []
        sid_sspn = []
        sid_id = []

        matching_spn_ids = []
        matching_sspn_ids = []
        matching_itemid_ids = []

        # Mask approach for efficient row-wise matching
        spn_mask = fulldf["Supplier_part_no"] == supspn
        sspn_mask = fulldf["Stripped_SPN"] == supsspn
        itemid_mask = fulldf["Item_id"] == supitemid

        # Process SPN matching
        if spn_mask.any():
            matching_criteria.append("SPN matching")
            matching_spn = fulldf.loc[spn_mask, "Supplier_part_no"].tolist()
            fulldf.loc[spn_mask, "Matching_spn"] = supspn
            matching_count += spn_mask.sum()
            fulldf.loc[spn_mask, "Matching_check"] = "Matching"
            
            # Collect matching IDs for SPN
            sid_spn = fulldf.loc[spn_mask, "s_id"].tolist()
            matching_spn_ids = fulldf.loc[spn_mask, "s_id"].tolist()
            fulldf.loc[spn_mask, "Matching_spn_ids"] = ", ".join(map(str, set(sid_spn)))

        # Process Stripped SPN matching
        if sspn_mask.any():
            matching_criteria.append("Stripped SPN matching")
            matching_sspn = fulldf.loc[sspn_mask, "Stripped_SPN"].tolist()
            fulldf.loc[sspn_mask, "Matching_stripped_SPN"] = supsspn
            matching_count += sspn_mask.sum()
            fulldf.loc[sspn_mask, "Matching_check"] = "Matching"
            
            
            # Collect matching IDs for Stripped SPN
            sid_sspn = fulldf.loc[sspn_mask, "s_id"].tolist()
            matching_sspn_ids = fulldf.loc[sspn_mask, "s_id"].tolist()
            fulldf.loc[sspn_mask, "Matching_stripped_SPN_ids"] = ", ".join(map(str, set(sid_sspn)))

        # Process Item ID matching
        if itemid_mask.any():
            matching_criteria.append("Item id matching")
            matching_itemid = fulldf.loc[itemid_mask, "Item_id"].tolist()
            fulldf.loc[itemid_mask, "Matching_itemid"] = supitemid
            matching_count += itemid_mask.sum()
            fulldf.loc[itemid_mask, "Matching_check"] = "Matching"
            
            # Collect matching IDs for Item ID
            sid_id = fulldf.loc[itemid_mask, "s_id"].tolist()
            matching_itemid_ids = fulldf.loc[itemid_mask, "s_id"].tolist()
            fulldf.loc[itemid_mask, "Matching_itemid_ids"] = ", ".join(map(str, set(sid_id)))

        # Update supdf based on matches
        if matching_criteria:
            supdf.loc[i, "Matching_check"] = "Matching"
            supdf.loc[i, "Matching_count"] = matching_count
            supdf.loc[i, "Matching_criteria"] = ", ".join(set(matching_criteria))
            supdf.loc[i, "Matching_spn"] = ", ".join(map(str, set(matching_spn)))
            supdf.loc[i, "Matching_sspn"] = ", ".join(map(str, set(matching_sspn)))
            supdf.loc[i, "Matching_itemid"] = ", ".join(map(str, set(matching_itemid)))
            
            # Add ID matching columns for supdf
            supdf.loc[i, "Matching_spn_ids"] = ", ".join(map(str, set(matching_spn_ids)))
            supdf.loc[i, "Matching_stripped_SPN_ids"] = ", ".join(map(str, set(matching_sspn_ids)))
            supdf.loc[i, "Matching_itemid_ids"] = ", ".join(map(str, set(matching_itemid_ids)))
        else:
            # Reset columns if no matching criteria found
            supdf.loc[i, "Matching_check"] = "Not Matching"
            supdf.loc[i, "Matching_count"] = 0
            supdf.loc[i, "Matching_criteria"] = "Not Matching"
            supdf.loc[i, "Matching_spn"] = "Not Matching"
            supdf.loc[i, "Matching_sspn"] = "Not Matching"
            supdf.loc[i, "Matching_itemid"] = "Not Matching"
            supdf.loc[i, "Matching_spn_ids"] = "Not Matching"
            supdf.loc[i, "Matching_stripped_SPN_ids"] = "Not Matching"
            supdf.loc[i, "Matching_itemid_ids"] = "Not Matching"

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