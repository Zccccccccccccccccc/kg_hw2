import pandas as pd
from neo4j import GraphDatabase
import os

# --- 配置信息 ---
URI = "bolt://localhost:7687"
AUTH = ("admin", "73@TuGraph")
DB_NAME = "default"

node_name_list = ["Alias", "Part", "Age", "Infection", "Insurance", "Department", "Checklist", "Symptom", "Complication", "Treatment", "Drug", "Period", "Rate", "Money"]
edge_name_list = ["HAS_ALIAS", "IS_OF_PART", "IS_OF_AGE", "IS_INFECTIOUS", "In_Insurance", "IS_OF_Department", "HAS_Checklist", "HAS_SYMPTOM", "HAS_Complication", "HAS_Treatment", "HAS_Drug", "Cure_Period", "Cure_Rate", "NEED_Money"]

def import_data():
    driver = GraphDatabase.driver(URI, auth=AUTH)
    with driver.session(database=DB_NAME) as session:
        print("1. 初始化 Schema...")
        # 建立 Disease Label
        session.run("CALL db.createVertexLabel('Disease', 'name', 'name', 'STRING', false)")
        
        for i in range(len(node_name_list)):
            node_label = node_name_list[i]
            edge_label = edge_name_list[i]
            try:
                session.run(f"CALL db.createVertexLabel('{node_label}', 'name', 'name', 'STRING', false)")
                session.run(f"CALL db.createEdgeLabel('{edge_label}', '[[\"Disease\",\"{node_label}\"]]')")
            except: pass 

        print("2. 导入顶点...")
        # 导入 Disease (注意 header=0 自动跳过首行并以列名匹配)
        if os.path.exists("Disease.csv"):
            df_dis = pd.read_csv("Disease.csv") 
            for name in df_dis["name"].dropna().unique():
                session.run("MERGE (n:Disease {name: $name})", name=str(name).strip())

        # 3. 循环导入属性和边
        for i in range(len(node_name_list)):
            node_label = node_name_list[i]
            edge_label = edge_name_list[i]
            node_csv = f"{node_label}.csv"
            edge_csv = f"{edge_label}.csv"

            if os.path.exists(node_csv):
                df_node = pd.read_csv(node_csv)
                for name in df_node["name"].dropna().unique():
                    session.run(f"MERGE (n:{node_label} {{name: $name}})", name=str(name).strip())

            if os.path.exists(edge_csv):
                df_edge = pd.read_csv(edge_csv) # 必须确保列名是 SRC_ID 和 DST_ID
                for _, row in df_edge.iterrows():
                    # 关键修复点：使用 strip() 去除不可见空格，并添加错误检查
                    src = str(row['SRC_ID']).strip()
                    dst = str(row['DST_ID']).strip()
                    
                    # 使用 MERGE 确保边被创建。如果 MATCH 不到，说明前面的节点导入有误
                    result = session.run(f"""
                        MATCH (a:Disease {{name: $src}})
                        MATCH (b:{node_label} {{name: $dst}})
                        MERGE (a)-[r:{edge_label}]->(b)
                        RETURN r
                    """, src=src, dst=dst)
                    
                    # 调试：检查是否匹配成功
                    if not result.peek():
                        print(f"警告: 关系匹配失败 -> {src} ({node_label}) {dst}")

    driver.close()
    print("✨ 数据导入任务完成！")

if __name__ == "__main__":
    import_data()
 