import csv
import concurrent.futures

columns = ['aaref', 'aaalt', 'aapos', 'Ensembl_proteinid', 'SIFT_score', 'SIFT4G_score', 'Polyphen2_HDIV_score',
           'Polyphen2_HVAR_score', 'LRT_score', 'MutationTaster_score', 'MutationAssessor_score', 'FATHMM_score',
           'PROVEAN_score', 'VEST4_score', 'MetaSVM_score', 'MetaLR_score', 'MetaRNN_score', 'M-CAP_score',
           'REVEL_score', 'MutPred_score', 'MVP_score', 'MPC_score', 'PrimateAI_score', 'DEOGEN2_score',
           'BayesDel_addAF_score', 'BayesDel_noAF_score', 'ClinPred_score', 'LIST-S2_score', 'CADD_raw',
           'CADD_raw_hg19', 'DANN_score', 'fathmm-MKL_coding_score', 'fathmm-XF_coding_score', 'Eigen-raw_coding',
           'GenoCanyon_score', 'LINSIGHT']

def process_file(index, title):
    with open('raw_dbsnfp.tsv', 'r') as dbsnfp:
        tsvreader = csv.reader(dbsnfp, delimiter='\t')
        with open(f'datas/{title}.tsv', 'w') as new_file:
            for line in tsvreader:
                if line[0] == '.' or line[1] == '.':
                    continue
                else:
                    new_file.write(f"{line[0]}\t{line[1]}\t{line[2]}\t{line[3]}\t{line[index+4]}\n")
        dbsnfp.seek(0)

if __name__ == '__main__':
    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = []
        for index, title in enumerate(columns[4:]):
            future = executor.submit(process_file, index, title)
            futures.append(future)

        # Wait for all tasks to complete
        concurrent.futures.wait(futures)
