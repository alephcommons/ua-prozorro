import os
import sys
import json
import logging
from dotenv import load_dotenv
from tqdm import tqdm
import prozorro_api as pr
from alephclient.api import AlephAPI

project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, project_root)

from utils.logger_utils import configure_logging

load_dotenv()

configure_logging(add_stream_handler=True,
                  log_folder_path=os.environ.get('LOGS_FOLDER_PATH'))
logger = logging.getLogger(__name__)

from state_registries.ukraine import transform


aleph = AlephAPI(os.environ['ALEPH_URL'], os.environ['ALEPH_API_KEY'])
collection = aleph.load_collection_by_foreign_id(os.environ['ALEPH_FOREIGN_ID'])
collection_id = collection.get('id')


def save_tender(tender):
    DIR = os.path.dirname(os.path.realpath(__file__))
    save_to_path = f"{DIR}/tenders/{tender['tenderID']}.json"
    with open(save_to_path, 'w', encoding='utf-8') as outfile:
        json.dump(tender, outfile, ensure_ascii=False)


def transform_many(tenders_gen):
    tenders_counter = 0
    for tender_js in tenders_gen:
        try:
            tenders_counter += 1
            logger.info(f'Now processing tender with id {tender_js["tenderID"]}')
            tenders_gen.set_description(f'tender number - {tenders_counter}')
            if 'contracts' in tender_js:
                """Process only tenders which has contacts inside, 
                    Otherwise it should be a lot of missed fields (at least I think so)"""
                # save_tender(tender_js)  # for debugging
                yield from transform.transform(tender_js)
            else:
                logger.info(f'"contracts" field was not found in tender json file with id - {tender_js["tenderID"]}, '
                            f'skipping processing it')
        except Exception as e:
            logger.exception(f'Failed to process tender with id - {tender_js["tenderID"]}. '
                             f'Skipping it. Original exception - {e}')
            save_tender(tender_js)


def extract_transform_upload(start_date, end_date):
    try:
        logger.info(f'Starting processing Prozorro tenders in diapason [{start_date} : {end_date}]')
        tenders_gen = tqdm(pr.get_objects_stream("tenders", start_date, end_date, 1))
        transformed_tenders = transform_many(tenders_gen)
        aleph.write_entities(collection_id, transformed_tenders, chunk_size=1000)
    except Exception as e:
        logger.exception(f'Unexpected exception has occured. Exiting with non zero status.'
                         f'Original exception - {e}')


if __name__ == '__main__':
    start_date = "2022-01-01"
    end_date = "2022-10-31T00:00:00+00:00"
    extract_transform_upload(start_date, end_date)
