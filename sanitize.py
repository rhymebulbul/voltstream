import os
import json
import re

def clean_text(text):
    text = re.sub(r'FIT5202 Assignment 2A', 'VoltStream Model Training', text, flags=re.IGNORECASE)
    text = re.sub(r'FIT5202', 'voltstream', text, flags=re.IGNORECASE)
    text = re.sub(r'Assignment 1', 'Phase 1', text, flags=re.IGNORECASE)
    text = re.sub(r'Assignment 2A', 'Phase 2A', text, flags=re.IGNORECASE)
    text = re.sub(r'Assignment 2B', 'Phase 2B', text, flags=re.IGNORECASE)
    text = re.sub(r'Assignment', 'Task', text, flags=re.IGNORECASE)
    text = re.sub(r'Monash', 'Open Source', text, flags=re.IGNORECASE)
    text = re.sub(r'from the labs', 'from the community', text, flags=re.IGNORECASE)
    text = re.sub(r'university', 'open source', text, flags=re.IGNORECASE)
    text = re.sub(r'student', 'developer', text, flags=re.IGNORECASE)
    text = re.sub(r'(?i)Write code to split the data.*?', '', text)
    text = re.sub(r'(?i)Apply the techniques you have learnt.*?optimisation\.\s*', 'Perform hyperparameter tuning.', text)
    text = re.sub(r'(?i)Based on the data exploration.*?how you plan to create/transform them\.\s*', 'Feature selection discussion.', text)
    text = re.sub(r'(?i)Assuming we are querying the dataset based on.*?hardware resources\?', 'Data partitioning strategy.', text)
    return text

for root, dirs, files in os.walk('.'):
    for f in files:
        if f.endswith('.ipynb'):
            path = os.path.join(root, f)
            with open(path, 'r', encoding='utf-8') as file:
                try:
                    data = json.load(file)
                except:
                    continue
            
            for cell in data.get('cells', []):
                if 'source' in cell:
                    if isinstance(cell['source'], list):
                        cell['source'] = [clean_text(line) for line in cell['source']]
                    else:
                        cell['source'] = clean_text(cell['source'])
            
            with open(path, 'w', encoding='utf-8') as file:
                json.dump(data, file, indent=1)
