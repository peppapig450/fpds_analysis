import pyarrow.parquet as pq
import pyarrow as pa

file_path = "data/contract_data.parquet"

try:
    table = pq.read_table(file_path)

    new_names = {}
    for old_name in table.column_names:
        if isinstance(old_name, tuple): # Handle tuple column names
            new_name = tuple(str(x).replace("@", "").replace("#", "").replace(" ", "_").replace("-", "_").replace(".", "_") for x in old_name)
        else: # Handle string column names
            new_name = str(old_name).replace("@", "").replace("#", "").replace(" ", "_").replace("-", "_").replace(".", "_") # More replacements as needed
        new_name = "".join(c for c in new_name if c.isalnum() or c=="_") #Remove non-alphanumeric
        if new_name[0].isdigit():
            new_name = "_"+new_name # Add _ if starts with number
        new_names[old_name] = new_name #Handles both string and tuple names

    renamed_table = table.rename_columns(list(new_names.values()))
    
    pq.write_table(renamed_table, "data/sanitized_contract_data.parquet")
    fixed_table = pq.read_table("data/sanitized_contract_data.parquet")
    df = fixed_table.to_pandas()
    
    print(df)
except FileNotFoundError:
    print(f"Error: File not found at {file_path}")
except Exception as e:
    print(f"An error occurred: {e}")