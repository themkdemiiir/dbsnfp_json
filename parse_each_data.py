import csv
import os
from concurrent.futures import ThreadPoolExecutor, as_completed


def process_file(file_path):
    new_file_path = os.path.join(output_dir, os.path.basename(file_path))
    with open(file_path, 'r', newline='') as input_file, open(new_file_path, 'w', newline='') as output_file:
        tsv_reader = csv.reader(input_file, delimiter='\t')
        tsv_writer = csv.writer(output_file, delimiter='\t')
        for line in tsv_reader:
            positions = line[2].split(";")
            proteins = line[3].split(";")
            scores = line[4].split(';')
            for i in range(len(positions)):
                score = scores[i] if len(scores) > 1 else scores[0]
                tsv_writer.writerow([line[0], line[1], positions[i], proteins[i], score.strip('\n')])


if __name__ == '__main__':
    input_dir = os.path.join(os.getcwd(), 'datas')
    output_dir = os.path.join(os.getcwd(), 'new_datas')

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    file_paths = [os.path.join(input_dir, f) for f in os.listdir(input_dir)]

    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_file, f) for f in file_paths]

        for future in as_completed(futures):
            try:
                _ = future.result()
            except Exception as e:
                print(f'Error processing file: {e}')


