import os
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed


def process_file(file_path):
    data = {}
    with open(file_path, 'r') as f:
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            protein_id = row['Ensembl_proteinid']
            aa_pos = row['aapos']
            aa_alt = row['aaalt']
            file = file_path
            tool_Score = row[file.split('/')[-1].split('.')[0]]

            if protein_id not in data:
                data[protein_id] = {
                    'scores': {}
                }

            if aa_pos not in data[protein_id]['scores']:
                data[protein_id]['scores'][aa_pos] = {}

            if tool_Score != '.':
                try:
                    data[protein_id]['scores'][aa_pos][aa_alt] = float(tool_Score)
                except ValueError:
                    data[protein_id]['scores'][aa_pos][aa_alt] = tool_Score

                data[protein_id]['scores'][aa_pos]['ref'] = row['aaref']
            else:
                continue

    with open(os.path.join(output_dir, file.split('/')[-1].split('.')[0] + '.json'), 'w') as f:
        for key in data.keys():
            f.write(f"{key}\t{data[key]}\n")


if __name__ == '__main__':
    input_dir = os.path.join(os.getcwd(), 'new_datas')
    output_dir = os.path.join(os.getcwd(), 'json_files')

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    file_paths = [os.path.join(input_dir, f) for f in os.listdir(input_dir) if f.endswith('.tsv')]

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(process_file, f) for f in file_paths]

        for future in as_completed(futures):
            try:
                _ = future.result()
            except Exception as e:
                print(f'Error processing file: {e}')
