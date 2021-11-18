from clearml import Task, StorageManager, Dataset
import json
import jsonlines
import os

def load_jsonl(load_path:str):
    data = []
    with open(load_path, 'r') as file:
        for doc in file:
            data.append(json.loads(doc))
    return data

def to_jsonl(filename:str, file_obj):
    resultfile = open(filename, 'wb')
    writer = jsonlines.Writer(resultfile)
    writer.write_all(file_obj) 

def create_clearml_dataset(dataset_project, dataset_version, local_dataset_path:str, tags=[], wildcard = "*.json"):
    dataset = Dataset.create(dataset_name=dataset_version, dataset_project=dataset_project, dataset_tags=tags, parent_datasets=None, use_current_task=False)
    logger = dataset.get_logger() # for logging of media to debug samples
    dataset.add_files(local_dataset_path, wildcard=wildcard, local_base_folder=".", dataset_path="data", recursive=True, verbose=False)
    dataset.upload(output_url="s3://experiment-logging/datasets")
    dataset.finalize()
    dataset.publish()
    print("Files added", dataset.list_files())
    return dataset

def delete_dataset(dataset_project, dataset_name):
    Dataset.delete(dataset_name=dataset_name, dataset_project=dataset_project, force=True)

class bucket_ops:
    StorageManager.set_cache_file_limit(5, cache_context=None)

    def list(remote_path:str):
        return StorageManager.list(remote_path, return_full_path=False)

    def upload_folder(local_path:str, remote_path:str):
        StorageManager.upload_folder(local_path, remote_path, match_wildcard=None)
        print("Uploaded {}".format(local_path))

    def download_folder(local_path:str, remote_path:str):
        StorageManager.download_folder(remote_path, local_path, match_wildcard=None, overwrite=True)
        print("Downloaded {}".format(remote_path))
    
    def get_file(remote_path:str):        
        object = StorageManager.get_local_copy(remote_path)
        return object

    def upload_file(local_path:str, remote_path:str):
        StorageManager.upload_file(local_path, remote_path, wait_for_upload=True, retries=3)

def combine_wikievents_coref(main_data_path,  coref_data_path):
    for dataset in ["train", "dev", "test"]:
        main_data_file = os.path.join(main_data_path, dataset+".jsonl")
        coref_data_file = os.path.join(coref_data_path, dataset+".jsonlines")
        new_data = [{**main_data, **coref_data} for (main_data, coref_data) in zip(load_jsonl(main_data_file), load_jsonl(coref_data_file))]
        to_jsonl(os.path.join("upload", dataset+".jsonl"), new_data)


def wikievents2dygie(wikievent:list, maxlength=1024, filter_events=['conflict.attack', 'life.die','contact.contact','life.injure','movement.transportation','genericcrime.genericcrime','conflict.demonstrate','contact.threatencoerce','disaster.crash','artifactexistence.damagedestroydisabledismantle']):
    entity_list = [{entity['text']:[entity['start'], entity['end']] for entity in doc['entity_mentions']} for doc in wikievent]

    if len(filter_events)>0:
        wikievent = [{'dataset': "wikievent", 'doc_key': doc['doc_id'], 'doc_len': len(doc['tokens']+['.']), 'sentences': [doc['tokens']+['.']], '_sentence_start': [0],
            'events': [[
                [[event['trigger']['start'], '.'.join(event['event_type'].split('.')[:-1])]]
                +[entities[arg['text']]+[arg['role']] if arg['text'] in entities.keys() else ['0','0', arg['role']] for arg in event['arguments']] for event in doc['event_mentions'] if '.'.join(event['event_type'].split('.')[:-1]).lower() in filter_events
            ]], 
            'ner':[[[entity['start'], entity['end'], entity['entity_type']] for entity in doc['entity_mentions']]]} for entities, doc in zip(entity_list, wikievent)]        
    else:
        wikievent = [{'dataset': "wikievent", 'doc_key': doc['doc_id'], 'doc_len': len(doc['tokens']+['.']), 'sentences': [doc['tokens']+['.']], '_sentence_start': [0],
            'events': [[
                [[event['trigger']['start'], '.'.join(event['event_type'].split('.')[:-1])]]
                +[entities[arg['text']]+[arg['role']] if arg['text'] in entities.keys() else ['0','0', arg['role']] for arg in event['arguments']] for event in doc['event_mentions']
            ]], 
            'ner':[[[entity['start'], entity['end'], entity['entity_type']] for entity in doc['entity_mentions']]]} for entities, doc in zip(entity_list, wikievent)]
    
    wikievent = [{**event, 'sentences': [[token.encode('utf-8').decode('ascii', errors='ignore') for token in event['sentences'][0]]]} for event in wikievent]        
    wikievent = [event for event in wikievent if event.pop('doc_len')<=maxlength]    
    return wikievent


#delete_dataset(dataset_name="processed-data", dataset_project="ace05-event")
# dataset = create_clearml_dataset(dataset_project = "datasets/wikievents", dataset_version = "gen-arg", local_dataset_path = "wikievents", tags = ["original"], wildcard = None)
#dataset = create_clearml_dataset(dataset_project = "datasets/muc4", dataset_version = "muc4-processed", local_dataset_path = "./muc/processed", tags = ["processed"], wildcard = "*.json")

original_dataset = Dataset.get(dataset_name="gen-arg", dataset_project="datasets/wikievents", dataset_tags=["original"], only_published=True)
folder_path = original_dataset.get_local_copy()

for dataset in ["train.jsonl", "dev.jsonl", "test.jsonl"]:
    filepath = os.path.join(folder_path, "data/wikievents", dataset)
    wikievent = wikievents2dygie(load_jsonl(filepath))
    to_jsonl(os.path.join("upload", dataset), wikievent)    

#combine_wikievents_coref(os.path.join(folder_path, "data/wikievents"), os.path.join(folder_path, "data/wikievents/coref"))    

new_dataset = Dataset.create(dataset_name="wikievents-10events", dataset_project="datasets/wikievents", dataset_tags=["10events"], parent_datasets=None, use_current_task=False)
new_dataset.add_files("./upload", wildcard=None, local_base_folder=".", dataset_path="data", recursive=True, verbose=False)
new_dataset.upload(output_url="s3://experiment-logging/datasets")
new_dataset.finalize()
new_dataset.publish()




