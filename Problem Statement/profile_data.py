import os
import glob
import pandas as pd

def profile_tables(data_dir="data"):
    """
    Profiles all CSV files found within the specified directory, 
    calculating null tracking metrics and unique value cardinalities.
    """
    csv_files = glob.glob(os.path.join(data_dir, "*.csv"))
    if not csv_files:
        print(f"No source CSV datasets detected inside directory: '{data_dir}'")
        return

    print("=" * 80)
    print("DATA PROFILING SUMMARY REPORT")
    print("=" * 80)

    for file_path in csv_files:
        table_name = os.path.basename(file_path).replace(".csv", "")
        try:
            df = pd.read_csv(file_path)
        except Exception as e:
            print(f"Failed to read {file_path}: {e}")
            continue

        total_rows = len(df)
        print(f"\nTable: {table_name} (Total Rows: {total_rows})")
        print("-" * 80)
        
        profiling_rows = []
        for col in df.columns:
            null_count = int(df[col].isna().sum())
            pct_populated = ((total_rows - null_count) / total_rows * 100) if total_rows > 0 else 0.0
            distinct_count = int(df[col].nunique(dropna=True))
            
            # Extract boundary examples
            non_null_vals = df[col].dropna()
            min_val = non_null_vals.min() if not non_null_vals.empty else "N/A"
            max_val = non_null_vals.max() if not non_null_vals.empty else "N/A"
            
            profiling_rows.append({
                "Column Name": col,
                "Data Type": str(df[col].dtype),
                "Null Count": null_count,
                "Percentage Populated": f"{pct_populated:.2f}%",
                "Distinct Value Count": distinct_count,
                "Minimum Value": min_val,
                "Maximum Value": max_val
            })
            
        profile_df = pd.DataFrame(profiling_rows)
        print(profile_df.to_string(index=False))
        print("-" * 80)

if __name__ == "__main__":
    # Assumes local directory execution context
    profile_tables()