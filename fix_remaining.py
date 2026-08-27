import json

path1 = "src/cpu_workload_optimization.ipynb"
with open(path1, "r", encoding="utf-8") as f:
    data = json.load(f)
for cell in data.get("cells", []):
    if "outputs" in cell:
        for output in cell["outputs"]:
            if "text" in output:
                if isinstance(output["text"], list):
                    output["text"] = [t.replace("FIT5202", "voltstream") for t in output["text"]]
                else:
                    output["text"] = output["text"].replace("FIT5202", "voltstream")
with open(path1, "w", encoding="utf-8") as f:
    json.dump(data, f, indent=1)

path2 = "src/gbt_model_training.ipynb"
with open(path2, "r", encoding="utf-8") as f:
    data = json.load(f)
for cell in data.get("cells", []):
    if "source" in cell:
        if isinstance(cell["source"], list):
            cell["source"] = [t.replace("mentioned in Lab 5", "from standard data analysis practices") for t in cell["source"]]
with open(path2, "w", encoding="utf-8") as f:
    json.dump(data, f, indent=1)
