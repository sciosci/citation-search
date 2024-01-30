import os
import pandas as pd
import time
import logging
from article_has_citation import get_etree_with_path, Article
from multiprocessing import Pool, freeze_support
from string import Template

logging.basicConfig(filename='logs/mpoa_xml_extract.log', filemode='a',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


def extract_citation_context(path: str):
    print(f"Started processing {path}")
    result = get_etree_with_path(path=path)
    article = Article(root=result)
    meta = article.build_meta_list_all_level()
    context = meta[2]
    for entry in context:
        if entry['citations']:
            entry['citations'] = list(entry['citations'])
            entry['citations'] = ','.join(entry['citations'])
        else:
            entry['citations'] = ''
            entry['has_citations'] = 0
    return context


def get_all_pmoa_file_names():
    # Reading all the PMOA CITE Dataset file names
    pmoa_dataset_file_names = '/home/ubuntu/mypetalibrary/pmoa-cite-dataset/oa_comm_files.csv'
    df = pd.read_csv(pmoa_dataset_file_names)
    pm_ids = df['0'].values
    logging.info(f"Returning {len(pm_ids)} pubmed XML file names")
    return pm_ids


def ingest_chunk_to_parquet(files_list: list, target_path: str):
    path = '/home/ubuntu/mypetalibrary/pmc-oa-opendata/oa_comm'
    start_time = time.perf_counter()
    df_main = pd.DataFrame()
    for curr in files_list:
        result = extract_citation_context(path=f"{path}/{curr}")
        df = pd.DataFrame(result)
        df_main = pd.concat([df_main, df])
    df_main.to_parquet(target_path, engine='pyarrow')
    end_time = time.perf_counter()
    logging.info(f"Ingest file : {target_path} in {end_time - start_time} seconds.")


if __name__ == '__main__':
    pm_ids = get_all_pmoa_file_names()
    cores = 10
    folder_step = 500000
    file_step = 1000
    total_size = len(pm_ids)
    target_path = Template('/home/ubuntu/mypetalibrary/pmoa-cite-dataset/extracted_pmoa_files/$folder/$file_name')
    folder_idx = 0
    file_idx = 0
    for folder_start in range(0, total_size, folder_step):
        folder_idx += 1
        file_idx = 0
        # todo: Create Folder Here
        folder_path = f"/home/ubuntu/mypetalibrary/pmoa-cite-dataset/extracted_pmoa_files/{folder_idx}"
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        folder_end = min(folder_start + folder_step, len(pm_ids))
        args = []
        for pm_idx in range(folder_start, folder_end, file_step):
            file_idx += 1
            target_file = target_path.substitute({'folder': folder_idx, 'file_name': f"{file_idx}.parquet"})
            curr_pm_ids = pm_ids[pm_idx:pm_idx + file_step]
            args.append((curr_pm_ids, target_file))
        with Pool(processes=cores) as pool:
            run = pool.starmap(ingest_chunk_to_parquet, args)
