import os
import pandas as pd
from tabulate import tabulate

# Path to the data folder
data_folder = "dataset_exploration/data"
output_file = "dataset_exploration/data_descriptions.md"

def analyze_csv(file_path):
    """Analyze the structure and description of a CSV file."""
    try:
        df = pd.read_csv(file_path)
        description = df.describe(include='all').transpose()
        structure = {
            "columns": list(df.columns),
            "dtypes": df.dtypes.to_dict(),
            "shape": df.shape
        }
        return structure, description
    except Exception as e:
        return None, str(e)

def write_descriptions_to_md(data_folder, output_file):
    """Write the structure and descriptions of CSV files to a markdown file."""
    with open(output_file, "w") as md_file:
        md_file.write("# Data Descriptions\n\n")
        for file_name in os.listdir(data_folder):
            if file_name.endswith(".csv"):
                file_path = os.path.join(data_folder, file_name)
                md_file.write(f"## {file_name}\n\n")
                structure, description = analyze_csv(file_path)
                if structure:
                    md_file.write("### Structure\n")
                    md_file.write(f"- Columns: {structure['columns']}\n")
                    md_file.write(f"- Data Types: {structure['dtypes']}\n")
                    md_file.write(f"- Shape: {structure['shape']}\n\n")
                    md_file.write("### Description\n")
                    md_file.write(tabulate(description, headers="keys", tablefmt="github") + "\n\n")
                else:
                    md_file.write(f"Error processing file: {description}\n\n")

if __name__ == "__main__":
    write_descriptions_to_md(data_folder, output_file)