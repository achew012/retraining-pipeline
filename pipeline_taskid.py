from clearml import Task, StorageManager, Dataset, PipelineController
import os, json, jsonlines

# def load_jsonl(load_path:str):
#     data = []
#     with open(load_path, 'r') as file:
#         for doc in file:
#             data.append(json.loads(doc))
#     return data

# def to_jsonl(filename:str, file_obj):
#     resultfile = open(filename, 'wb')
#     writer = jsonlines.Writer(resultfile)
#     writer.write_all(file_obj) 

# def get_local_data(dataset_name, dataset_project, dataset_tags):
#     original_dataset = Dataset.get(dataset_name=dataset_name, dataset_project=dataset_project, dataset_tags=dataset_tags, only_published=True)
#     folder_path = original_dataset.get_local_copy()
#     return [folder_path, os.path.join(folder_path, "coref"), original_dataset]

from clearml import Task, StorageManager, Dataset, PipelineController

# Creating the pipeline
pipe = PipelineController(project = "pipeline", name="combination-dygiepp-gtt", version='2')
pipe.set_default_execution_queue("services")

pipe.add_step(name='predict', base_task_id='17cc79f0dae0426d9354fe08d979980a', execution_queue="128RAMv100")

pipe.start()
# Wait until pipeline terminates
pipe.wait()
# cleanup everything
pipe.stop()  
print('pipeline completed')


# pipe.add_step(name='stage_data', base_task_project='examples', base_task_name='pipeline step 1 dataset artifact') # add dygiepp data and gtt data to a location
# , parents=['stage_data', ],
# , parameter_override={'General/dataset_url': '${stage_data.artifacts.dataset.url}', 'General/test_size': 0.25})

#pipe.add_step(name='dygiepp-predict', base_task_id='17cc79f0dae0426d9354fe08d979980a')
# pipe.add_step(name='gtt-predict', base_task_project='GTT', base_task_id='21b08e6bf66945db894918014679695e')
# pipe.add_step(
#     name='combine-predictions', 
#     parents=['dygiepp-predict', 'gtt-predict'], 
#     base_task_id = "9a3cbb74b4804f45bf2896a6362ba4db",
#     parameter_override={'General/dygiepp-preds-path': "${dygiepp-predict.artifacts.predictions.url}", 'General/gtt-preds-path': "${gtt-predict.artifacts.preds.url}"}
#     )





