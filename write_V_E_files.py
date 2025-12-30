import json
import pandas as pd
import os

# 定义节点和边的名称列表
node_name_list = ["Alias", "Part", "Age", "Infection", "Insurance", "Department", "Checklist", "Symptom", "Complication", "Treatment", "Drug", "Period", "Rate", "Money"]
edge_name_list = ["HAS_ALIAS", "IS_OF_PART", "IS_OF_AGE", "IS_INFECTIOUS", "In_Insurance", "IS_OF_Department", "HAS_Checklist", "HAS_SYMPTOM", "HAS_Complication", "HAS_Treatment", "HAS_Drug", "Cure_Period", "Cure_Rate", "NEED_Money"]

def clear_existing_files():
    """清理当前目录下可能存在的旧 CSV 和 JSON 文件"""
    files_to_delete = ["Disease.csv", "import_config.json"] + [f"{n}.csv" for n in node_name_list] + [f"{e}.csv" for e in edge_name_list]
    for f in files_to_delete:
        if os.path.exists(f):
            os.remove(f)
    print("已清理旧的中间文件。")

def process_medical_data(csv_path):
    """
    读取原始 CSV 并拆分为 TuGraph 导入所需的多个 CSV 文件
    """
    if not os.path.exists(csv_path):
        print(f"错误: 找不到文件 {csv_path}")
        return

    df = pd.read_csv(csv_path, encoding='utf-8')
    
    # 1. 生成 Disease 节点文件 (带表头)
    disease_list = df.iloc[:, 0].unique()
    pd.DataFrame(disease_list, columns=["name"]).to_csv("Disease.csv", index=False)

    # 2. 初始化其他节点和边的文件字典（用于收集数据后一次性写入，提高性能且方便去重）
    nodes_data = {label: set() for label in node_name_list}
    edges_data = {label: [] for label in edge_name_list}

    # 遍历每一行数据
    for i in range(len(df)):
        disease_name = df.iloc[i, 0]
        
        for k in range(1, len(node_name_list) + 1):
            if k >= df.shape[1]: break
            
            node_label = node_name_list[k-1]
            edge_label = edge_name_list[k-1]
            
            cell_value = str(df.iloc[i, k])
            if cell_value == 'nan' or not cell_value.strip():
                continue
                
            # 处理空格分隔的多值属性
            sub_values = cell_value.split()
            
            for val in sub_values:
                nodes_data[node_label].add(val)
                edges_data[edge_label].append([disease_name, val])

    # 写入文件
    for label, values in nodes_data.items():
        pd.DataFrame(list(values), columns=["name"]).to_csv(f"{label}.csv", index=False)
    
    for label, values in edges_data.items():
        pd.DataFrame(values, columns=["SRC_ID", "DST_ID"]).to_csv(f"{label}.csv", index=False)

    print("CSV 数据拆分与预处理完成。")

def generate_tugraph_schema():
    """
    生成 TuGraph 导入配置文件 import_config.json
    """
    schema = []
    files = []

    # 1. 定义核心节点: Disease
    schema.append({
        "label": "Disease",
        "type": "VERTEX",
        "primary": "name",
        "properties": [{"name": "name", "type": "STRING", "optional": False, "index": True}]
    })
    files.append({
        "path": "Disease.csv",
        "format": "CSV",
        "label": "Disease",
        "header": 1,
        "columns": ["name"]
    })

    # 2. 循环定义关联节点、边以及它们的文件映射
    for node_label, edge_label in zip(node_name_list, edge_name_list):
        schema.append({
            "label": node_label,
            "type": "VERTEX",
            "primary": "name",
            "properties": [{"name": "name", "type": "STRING", "optional": False, "index": True}]
        })
        schema.append({
            "label": edge_label,
            "type": "EDGE",
            "properties": [],
            "constraints": [["Disease", node_label]]
        })
        
        files.append({
            "path": f"{node_label}.csv",
            "format": "CSV",
            "label": node_label,
            "header": 1,
            "columns": ["name"]
        })
        files.append({
            "path": f"{edge_label}.csv",
            "format": "CSV",
            "label": edge_label,
            "header": 1,
            "SRC_ID": "Disease",
            "DST_ID": node_label,
            "columns": ["SRC_ID", "DST_ID"]
        })

    config = {"schema": schema, "files": files}
    with open('import_config.json', 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=4, ensure_ascii=False)
    
    print("已成功生成 import_config.json。")

if __name__ == "__main__":
    clear_existing_files()
    process_medical_data('./disease3.csv')
    generate_tugraph_schema()