import pandas as pd
import glob
import os

BASE_DIR = os.getcwd()
# specify the input file path pattern, output file path, and the columns you want to extract
input_file_pattern = os.path.join(BASE_DIR, "data", "dbNSFP4.3a_variant.chr*")
output_file_path = os.path.join(BASE_DIR, "data")
columns = [4, 5, 11, 15, 37, 40, 43, 46, 49, 53, 58, 61, 64, 67, 69, 72, 76, 79, 82, 84, 89, 91, 93, 96, 99, 102, 105,
           108, 117, 120, 123, 125, 129, 132, 138, 152]
# example columns to extract

# find all input files that match the specified pattern
input_files = sorted(glob.glob(input_file_pattern))

# loop through each input file and extract the desired columns
for input_file in input_files:
    first_chunk = True
    for chunk in pd.read_csv(input_file, delimiter="\t", usecols=columns, chunksize=10000000):
        # append the chunk to the output file
        chunk.to_csv(output_file_path, mode='a', index=False, sep='\t', header=first_chunk)
        if first_chunk:
            first_chunk = False
