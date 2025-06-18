import pandas as pd
import bcs_connector_orange_st
import bcs_connector_fullitems
import json



def read_full_items():

    df = bcs_connector_fullitems.reader_df()

    return df


def read_supplier_data(suppliers):

    full_extracted_df = pd.DataFrame()

    for sup in suppliers:
        df = bcs_connector_orange_st.reader_df(sup)
        pd.concat([full_extracted_df, df], axis=0)
    
    return full_extracted_df



def matching_items(fulldf, supdf):
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

    for i, r in supdf.iterrows():
        supspn = r["Supplier_part_no"]
        supsspn = r["Stripped_SPN"]
        supitemid = r["Item_id"]
        supid = r["s_id"]

        matching_count = 0
        matching_criteria = []
        matching_spn = []
        matching_sspn = []
        matching_itemid = []

        matching_spn_ids = []
        matching_sspn_ids = []
        matching_itemid_ids = []

        sid_spn = []
        sid_sspn = []
        sid_id = []

        # Iterate through fulldf to compare
        for j, t in fulldf.iterrows():
            spn = t["Supplier_part_no"]
            sspn = t["Stripped_SPN"]
            itemid = t["Item_id"]
            sid = t["s_id"]
            

            # Check for matches and store criteria
            if supspn == spn:
                matching_criteria.append("SPN matching")
                matching_spn.append(spn)
                fulldf.loc[j, "Matching_spn"] = supspn
                sid_spn.append(supid)
                matching_spn_ids.append(sid)

            if supsspn == sspn:
                matching_criteria.append("Stripped SPN matching")
                matching_sspn.append(sspn)
                fulldf.loc[j, "Matching_stripped_SPN"] = supsspn
                sid_sspn.append(supid)
                matching_sspn_ids.append(sid)

            if supitemid == itemid:
                matching_criteria.append("Item id matching")
                matching_itemid.append(itemid)
                fulldf.loc[j, "Matching_itemid"] = supitemid
                sid_id.append(supid)
                matching_itemid_ids.append(sid)

            # Increment the matching count if any match happens
            if matching_criteria:
                matching_count += 1
                fulldf.loc[j, "Matching_check"] = "Matching"
                fulldf.loc[j, "Matching_spn_ids"] = ", ".join(map(str, set(sid_spn)))
                fulldf.loc[j, "Matching_stripped_SPN_ids"] = ", ".join(map(str, set(sid_sspn)))
                fulldf.loc[j, "Matching_itemid_ids"] = ", ".join(map(str, set(sid_id)))


        # Check if any matching occurred for the supplier
        if matching_criteria:
            supdf.loc[i, "Matching_check"] = "Matching"
            supdf.loc[i, "Matching_count"] = matching_count
            supdf.loc[i, "Matching_criteria"] = ", ".join(set(matching_criteria))
            supdf.loc[i, "Matching_spn"] = ", ".join(map(str,set(matching_spn)))
            supdf.loc[i, "Matching_sspn"] = ", ".join(map(str,set(matching_sspn)))
            supdf.loc[i, "Matching_itemid"] = ", ".join(map(str,set(matching_itemid)))
            supdf.loc[i, "Matching_spn_ids"] = ", ".join(map(str, set(matching_spn_ids)))
            supdf.loc[i, "Matching_stripped_SPN_ids"] = ", ".join(map(str, set(matching_sspn_ids)))
            supdf.loc[i, "Matching_itemid_ids"] = ", ".join(map(str, set(matching_itemid_ids)))
        

        else:
            supdf.loc[i, "Matching_check"] = "Not Matching"
            supdf.loc[i, "Matching_count"] = "Not Matching"
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

    fulldf = read_full_items()
    supdf = read_supplier_data(supplier_ids)

    fulldf, supdf = matching_items(fulldf, supdf)
    fulldf.to_excel("V0_fullitems.xlsx", index=False)
    supdf.to_excel("V0_suppliers_data.xlsx", index=False)


if __name__ == "__main__":
    main()