import pandas as pd
from neo4j import GraphDatabase
import os

# --- TuGraph 配置 ---
URI = "bolt://localhost:7687"
AUTH = ("admin", "73@TuGraph")
DB_NAME = "default"

class LocalMedicalKGQA:
    def __init__(self):
        self.driver = GraphDatabase.driver(URI, auth=AUTH)
        self.diseases = self._load_entities()
        
        # 调试信息：确认加载了多少疾病
        if not self.diseases:
            print("[警告] 词库为空，请检查 Disease.csv 是否存在且有数据！")
        else:
            print(f"[信息] 词库加载成功，共计 {len(self.diseases)} 个疾病实体。")

        # 意图关键词映射表 (根据你的 edge_name_list 定义)
        self.intent_map = {
            "HAS_SYMPTOM": ["症状", "表现", "怎么了", "反应", "情况"],
            "HAS_Drug": ["药", "吃什么", "治疗方案", "用药"],
            "IS_OF_Department": ["科室", "挂什么号", "看哪科", "部门"],
            "NEED_Money": ["钱", "费用", "多少钱", "价格", "开销"],
            "HAS_Complication": ["并发症", "引起什么病", "诱发"],
            "IS_INFECTIOUS": ["传染", "传染性", "传染吗"],
            "HAS_Checklist": ["检查", "化验", "查什么"],
            "In_Insurance": ["医保", "报销"]
        }

    def _load_entities(self):
        """加载疾病词库并进行清洗"""
        if os.path.exists("Disease.csv"):
            try:
                # 读取时跳过空行，并确保读取为字符串
                df = pd.read_csv("Disease.csv")
                # 关键：清除名称前后的空格、换行符等不可见字符
                disease_list = df['name'].dropna().astype(str).map(lambda x: x.strip()).unique().tolist()
                # 按长度降序排序，防止如“流行性感冒”只匹配到“感冒”的情况
                disease_list.sort(key=len, reverse=True)
                return disease_list
            except Exception as e:
                print(f"[错误] 加载词库失败: {e}")
                return []
        return []

    def _extract_entity(self, question):
        """从问题中抽取疾病实体"""
        # 预处理问题：去空格
        clean_question = question.strip()
        for disease in self.diseases:
            if disease in clean_question:
                return disease
        return None

    def _detect_intent(self, question):
        """意图识别：根据关键词匹配关系类型"""
        for intent, keywords in self.intent_map.items():
            for word in keywords:
                if word in question:
                    return intent
        return None

    def query_graph(self, disease, intent):
        """执行 Cypher 查询"""
        cypher = f"""
        MATCH (d:Disease {{name: $name}})-[:{intent}]->(m)
        RETURN m.name as result
        """
        try:
            with self.driver.session(database=DB_NAME) as session:
                res = session.run(cypher, name=disease)
                return [record["result"] for record in res]
        except Exception as e:
            return [f"查询出错: {e}"]

    def answer(self, question):
        # 1. 识别疾病
        disease = self._extract_entity(question)
        if not disease:
            return "抱歉，我没能识别出问题中的疾病名称。请确保提问中包含库中存在的疾病全名。"

        # 2. 识别意图
        intent = self._detect_intent(question)
        if not intent:
            return f"我知道关于'{disease}'的信息，但我不确定你想问它的什么？（例如：症状、药、科室）"

        # 3. 检索知识
        results = self.query_graph(disease, intent)
        
        # 4. 组织回答
        if not results:
            return f"抱歉，我的知识图谱中暂时没有关于'{disease}'的相关信息。"
        
        res_str = "、".join(results)
        return f"针对您的提问，'{disease}'相关的信息如下：{res_str}。"

    def close(self):
        self.driver.close()

# --- 测试运行 ---
if __name__ == "__main__":
    handler = LocalMedicalKGQA()
    
    print("--- 欢迎使用本地医疗知识图谱问答助手 ---")
    while True:
        user_input = input("\n请输入您的问题 (输入 'quit' 退出): ")
        if not user_input.strip():
            continue
        if user_input.lower() == 'quit':
            break
            
        response = handler.answer(user_input)
        print(f"助手回复: {response}")
    
    handler.close()