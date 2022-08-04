import logging
import os
import json
from followthemoney import model


logger = logging.getLogger(__name__)


def _get_prozorro_tender_url(tender_id):
    return f'https://prozorro.gov.ua/tender/{tender_id}'


def get_address(tender_supplier_js):
    address_js = tender_supplier_js['address']
    address = model.make_entity(model.get('Address'))
    address.make_id(address_js['countryName'], address_js['region'], address_js['postalCode'], )
    address.add('country', address_js['countryName'])
    address.add('region', address_js['region'])
    address.add('city', address_js['locality'])
    address.add('street', address_js['streetAddress'])
    address.add('postalCode', address_js['postalCode'])
    full_address = [address_js['countryName'], address_js['region'],
                    address_js['locality'], address_js['streetAddress'], address_js['postalCode']]
    full_address = ', '.join([x for x in full_address if x])
    address.add('full', full_address)
    return address


def get_legal_entity(tender_supplier_js):
    le = model.make_entity(model.get('LegalEntity'))
    le.make_id(tender_supplier_js['identifier']['id'], tender_supplier_js['identifier'].get('scheme', ''))
    le.add('name', tender_supplier_js['name'])

    le.add('phone', tender_supplier_js['contactPoint'].get('telephone'))
    le.add('email', tender_supplier_js['contactPoint'].get('email'))
    le.add('website', tender_supplier_js['contactPoint'].get('url'))

    le.add('registrationNumber', tender_supplier_js['identifier']['id'])
    le.add('classification', tender_supplier_js['identifier']['scheme'])
    address = get_address(tender_supplier_js)
    le.add('addressEntity', address)
    le.add('country', tender_supplier_js['address']['countryName'])  # fuzzy=True
    return le, address


def get_item_contract(item_js, tender_js, contract_sub_js, buyer):
    item_contract = model.make_entity(model.get('Contract'))
    item_contract.make_id(item_js['id'])
    item_contract.add('title', item_js['description'])
    item_contract.add('authority', buyer)
    item_contract.add('amount', contract_sub_js['value']['amount'])
    item_contract.add('currency', contract_sub_js['value']['currency'])
    if 'dateSigned' in contract_sub_js:
        item_contract.add('contractDate', tender_js['date'])
    # item_contract.add('status', contract_sub_js['status'])
    item_contract.add('status', contract_sub_js['status'])
    item_contract.add('method', tender_js['procurementMethod'])
    if 'awardCriteria' in tender_js:
        item_contract.add('criteria', tender_js['awardCriteria'])
    classification = ' | '.join([item_js['classification']['id'],
                                 item_js['classification']['scheme'],
                                 item_js['classification']['description']])
    item_contract.add('classification', classification)
    item_contract.add('sourceUrl', _get_prozorro_tender_url(tender_js['tenderID']))
    return item_contract


def get_contract_award(contract_js, tender_js, supplier, contract):
    contract_award = model.make_entity(model.get('ContractAward'))
    contract_award.make_id(supplier.id, contract.id)
    contract_award.add('recordId', tender_js['id'])
    contract_award.add('lotNumber', tender_js['tenderID'])
    contract_award.add('supplier', supplier)
    contract_award.add('contract', contract)
    contract_award.add('sourceUrl', _get_prozorro_tender_url(tender_js['tenderID']))
    contract_award.add('amount', contract_js['value']['amount'])
    contract_award.add('currency', contract_js['value']['currency'])
    contract_award.add('status', tender_js['status'])

    contract_award.add('role', 'supplier')  # check if it needed
    if 'startDate' in contract_js:
        contract_award.add('startDate', contract_js['period']['startDate'])
    if 'endDate' in contract_js:
        contract_award.add('endDate', contract_js['period']['endDate'])

    if 'dateSigned' in contract_js:
        contract_award.add('date', contract_js['dateSigned'])

    contract_award.add('publisher', tender_js['owner'])
    contract_award.add('modifiedAt', tender_js['dateModified'])

    contract_award.add('summary', tender_js['title'])
    if 'description' in tender_js:
        # yes, description could be absent too. ex - UA-2018-11-21-001239-a
        contract_award.add('description', tender_js['description'])

    return contract_award


def transform(tender_js):
    """
    Main function for transforming Prozorro tenders json into FTM format
    Very important the way how it cross the entities inside Prozorro json
    """
    contract_entities = []
    buyer, buyer_address = get_legal_entity(tender_js['procuringEntity'])

    for contract_js in tender_js['contracts']:
        suppliers = []
        suppliers_addresses = []
        for supplier_js in contract_js['suppliers']:
            supplier, supplier_address = get_legal_entity(supplier_js)
            suppliers.append(supplier)
            suppliers_addresses.append(supplier_address)

        # get items from contract. It could be absent in contracts section
        # when it totally items from tender section
        items_js_list = contract_js.get('items') or tender_js['items']
        contracts_awards = []
        items_contracts = []
        for item_js in items_js_list:
            item_contract = get_item_contract(item_js, tender_js, contract_js, buyer)
            items_contracts.append(item_contract)

            for supplier in suppliers:
                contract_award = get_contract_award(contract_js, tender_js, supplier, item_contract)
                contracts_awards.append(contract_award)

        contract_entities.append({'suppliers': suppliers,
                                  'contracts': items_contracts,
                                  'contract_awards': contracts_awards,
                                  'suppliers_address': suppliers_addresses})
    frtm_obj_flattenized = [buyer, buyer_address] + sum(sum((list(x.values()) for x in contract_entities), []), [])
    for entity in frtm_obj_flattenized:
        if not entity.id:
            # not fail when id is absent, but log it
            logger.error(f'Id was not found for entity - {str(entity)}')
            continue
        yield entity


def read_and_process_filepath(js_filepath):
    with open(js_filepath, encoding='utf-8') as fin:
        tender_js = json.load(fin)
    transformed = transform(tender_js)
    return transformed


def transform_directory(folderpath):
    jsons_list = os.listdir(folderpath)
    jsons_list = [x for x in jsons_list if x.endswith('.json')]
    for js_filename in jsons_list:
        js_filepath = os.path.join(folderpath, js_filename)
        yield read_and_process_filepath(js_filepath)


def write_entities(entities):
    # for debugging, isn't supposed to be used in this module
    from dotenv import load_dotenv
    from alephclient.api import AlephAPI
    load_dotenv()
    aleph = AlephAPI(os.environ['ALEPH_URL'], os.environ['ALEPH_API_KEY'])
    collection = aleph.load_collection_by_foreign_id(os.environ['ALEPH_FOREIGN_ID'])
    collection_id = collection.get('id')
    aleph.write_entities(collection_id, entities, chunk_size=1)


if __name__ == '__main__':
    ukraine_folder_path = os.path.dirname(__file__)

    # UA-2018-11-27-000114-c
    tender_filepath = os.path.join(ukraine_folder_path, 'tenders', 'UA-2018-12-03-001103-c.json')
    # tender_filepath = os.path.join(ukraine_folder_path, 'test_tenders', 'simple.json')
    entities = read_and_process_filepath(tender_filepath)
    entities = list(entities)
    write_entities(entities)
