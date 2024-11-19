from backend.procs.sqlite3 import properties
from backend.procs.sqlite3 import stage
from backend.procs.sqlite3 import satellite
from backend.procs.sqlite3 import hub
from backend.procs.sqlite3 import link
from backend.procs.sqlite3 import pit
from backend.procs.sqlite3 import nh_satellite
from backend.procs.sqlite3 import ma_satellite
from backend.procs.sqlite3 import rt_satellite
from backend.procs.sqlite3 import nh_link
from backend.procs.sqlite3 import ref

task_proc_mapping = {
    'Properties': properties.gen_properties,
    'Stage': stage.generate_stage,
    'Standard Hub': hub.generate_hub,
    'Standard Link': link.generate_link,
    'Standard Satellite': satellite.generate_satellite,
    'Point-in-Time': pit.generate_pit,
    'Non-Historized Satellite': nh_satellite.generate_nh_satellite,
    'Multi-Active Satellite': ma_satellite.generate_ma_satellite,
    'Record Tracking Satellite': rt_satellite.generate_rt_satellite,
    'Non-Historized Link': nh_link.generate_nh_link,
    'Reference Table': ref.generate_ref
}

def handle_task(task, data_structure, task_proc_mapping):
    try:
        task_proc_mapping[task](data_structure)
    except Exception as e:
        data_structure['print2FeedbackConsole'](message= f'Failed to generate the {task} {data_structure["source"]}: {str(e)}')

def generate_selected_entities(todo, data_structure):
    for task in todo:
        if task in task_proc_mapping:
            handle_task(task, data_structure, task_proc_mapping)
