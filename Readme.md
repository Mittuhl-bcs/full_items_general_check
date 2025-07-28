## Objective
This aims at pulling in all the managed items data from our database, and items data from each of the suppliers from our database, and check matching on the basis of supplier part number, stripped SPN, item id

## files
Saves two files:
- full items
- suppliers data
these two have mapped info of each other in each of the files, items that are mapped on suppliers data on the full items file would have that info, while the suppliers data mapped to have data from full items file would have that info for each of the (row) items in the suppliers data.  