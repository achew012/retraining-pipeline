from clearml import Task, StorageManager, Dataset
from clearml import PipelineController
import os, json, jsonlines

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

def get_local_data(dataset_name, dataset_project, dataset_tags):
    original_dataset = Dataset.get(dataset_name=dataset_name, dataset_project=dataset_project, dataset_tags=dataset_tags, only_published=True)
    folder_path = original_dataset.get_local_copy()
    return [folder_path, os.path.join(folder_path, "coref"), original_dataset]

def combine_wikievents_coref(main_data_path,  coref_data_path):
    new_dataset = {}
    for dataset in ["train", "dev", "test"]:
        main_data_file = os.path.join(main_data_path, dataset+".jsonl")
        coref_data_file = os.path.join(coref_data_path, dataset+".jsonlines")
        new_data = [{**main_data, **coref_data} for (main_data, coref_data) in zip(load_jsonl(main_data_file), load_jsonl(coref_data_file))]
        new_dataset[dataset] = new_data
    return new_dataset

def wikievents2dygie(datasets:dict):
    if os.path.exists==False:
        os.makedirs("uploads")
    new_datasets = {}
    for key in datasets.keys():
        wikievent = datasets[key]
        entity_list = [{entity['text']:[entity['start'], entity['end']] for entity in doc['entity_mentions']} for doc in wikievent]
    #     wikievent = [{'dataset': "wikievent", 'doc_key': doc['doc_id'], 'doc_len': len(doc['tokens']+['.']), 'sentences': [doc['tokens']+['.']], '_sentence_start': [0], 'events': [[
    #         [[event['trigger']['start'], event['event_type']]]
    #         +[entities[arg['text']]+[arg['role']] if arg['text'] in entities.keys() else ['0','0', arg['role']] for arg in event['arguments']] for event in doc['event_mentions']
    #         ]], 
    #         'ner':[[[entity['start'], entity['end'], entity['entity_type']] for entity in doc['entity_mentions']]]} for entities, doc in zip(entity_list, wikievent)]
        wikievent = [{
            'dataset': "wikievent", 
            'doc_key': doc['doc_id'], 
            'doc_len': len(doc['tokens']+['.']), 
            'sentences': [doc['tokens']+['.']], 
            '_sentence_start': [0], 
            'events': [[
            [[event['trigger']['start'], '.'.join(event['event_type'].split('.')[:-1])]]
            +[entities[arg['text']]+[arg['role']] if arg['text'] in entities.keys() else ['0','0', arg['role']] for arg in event['arguments']] for event in doc['event_mentions']
            ]], 
            'ner':[[[entity['start'], entity['end'], entity['entity_type']] for entity in doc['entity_mentions']]],
            'clusters': [],
            } for entities, doc in zip(entity_list, wikievent)]
        
        wikievent = [{**event, 'sentences': [[token.encode('utf-8').decode('ascii', errors='ignore') for token in event['sentences'][0]]]} for event in wikievent]        
        wikievent = [event for event in wikievent if event.pop('doc_len')<=1024]    

        to_jsonl("uploads/{}.jsonl".format(key), wikievent)

    return "uploads"

def create_clearml_dataset(dataset_project, dataset_version, local_dataset_path:str, tags=[], wildcard = "*.json", parent_datasets=None):
    dataset = Dataset.create(dataset_name=dataset_version, dataset_project=dataset_project, dataset_tags=tags, parent_datasets=parent_datasets, use_current_task=False)
    dataset.add_files(local_dataset_path, wildcard=wildcard, local_base_folder=".", dataset_path="data", recursive=True, verbose=False)
    dataset.upload(output_url="s3://experiment-logging/datasets")
    dataset.finalize()
    dataset.publish()
    print("Files added", dataset.list_files())
    return dataset

if __name__ == '__main__':

    # Creating the pipeline
    pipe = PipelineController(project='pipeline', name='combine-coref-dygiepp-fmt', version='2')

    # set the default execution queue to be used (per step we can override the execution)
    pipe.set_default_execution_queue('default')

    pipe.add_parameter(name="dataset_name", default="gen-arg")
    pipe.add_parameter(name="dataset_project", default="datasets/wikievents")
    pipe.add_parameter(name="dataset_tags", default="original")

    pipe.add_function_step(
        name='get_local_data',
        function=get_local_data,
        function_kwargs=dict(dataset_name='${pipeline.dataset_name}', dataset_project='${pipeline.dataset_project}', dataset_tags='${pipeline.dataset_tags}'),
        function_return=['list'],
        cache_executed_step=True,
    )

    pipe.add_function_step(
        name='combine_wikievents_coref',
        function=combine_wikievents_coref,
        function_kwargs=dict(main_data_path='${get_local_data.list}'[0],  coref_data_path='${get_local_data.list}'[1]),
        function_return=['dict'],
        cache_executed_step=True,
    )

    pipe.add_function_step(
        name='wikievents2dygie',
        function=wikievents2dygie,
        function_kwargs=dict(datasets='${combine_wikievents_coref.dict}'),
        function_return=['path'],
        cache_executed_step=True,
        execution_queue="128RAMv100"
    )

    pipe.add_function_step(
        name='create_clearml_dataset',
        function=create_clearml_dataset,
        function_kwargs=dict(dataset_project="datasets/wikievents", dataset_version="combined-coref-dygiepp-fmt", local_dataset_path='${wikievents2dygie.path}', tags=["coref-combined", "dygiepp"], wildcard = "*.jsonl", parent_datasets='${get_local_data.list}'[-1]),
        function_return=['dict'],
        cache_executed_step=True,
        execution_queue="128RAMv100"
    )

    pipe.start()
    print('pipeline completed')




