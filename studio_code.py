import streamlit as st
import re
import datetime
import time
import threading
from sqlglot import parse_one, exp

# --- ID生成器逻辑 ---
class SnowflakeIdGenerator:
    def __init__(self, worker_id=1, datacenter_id=1):
        self.worker_id = worker_id
        self.datacenter_id = datacenter_id
        self.sequence = 0
        self.last_timestamp = -1
        self._lock = threading.Lock()

    def next_id(self):
        with self._lock:
            timestamp = int(time.time() * 1000)
            if timestamp == self.last_timestamp:
                self.sequence = (self.sequence + 1) & 4095
                if self.sequence == 0:
                    while timestamp <= self.last_timestamp:
                        timestamp = int(time.time() * 1000)
            else:
                self.sequence = 0
            self.last_timestamp = timestamp
            return ((timestamp - 1288834974657) << 22) | (self.datacenter_id << 17) | (self.worker_id << 12) | self.sequence

if 'id_gen' not in st.session_state:
    st.session_state.id_gen = SnowflakeIdGenerator()

def to_camel_case(snake_str):
    if not snake_str: return ""
    components = snake_str.split('_')
    return components[0] + ''.join(x.title() for x in components[1:])

def parse_ddl_robust(ddl_text):
    table_dict = {}
    table_blocks = re.findall(r'CREATE\s+TABLE\s+`?(\w+)`?\s*\((.*?)\)\s*(?:ENGINE|COMMENT|COLLATE|;)', ddl_text, re.S | re.I)
    for table_name, content in table_blocks:
        cols = {}
        lines = content.split('\n')
        for line in lines:
            line = line.strip()
            col_match = re.search(r'^`?(\w+)`?\s+.*COMMENT\s+\'([^\']+)\'', line, re.I)
            if col_match:
                cols[col_match.group(1)] = col_match.group(2)
        table_dict[table_name] = cols
    return table_dict

# --- 界面部分 ---
st.set_page_config(page_title="SQL报表字典工具", layout="wide")

st.title("📊 报表字典 Insert 语句生成器")
st.info("通过 SQL 和 DDL 自动关联字段备注，生成 report_model_dict 插入语句")

with st.sidebar:
    st.header("⚙️ 参数配置")
    model_code = st.text_input("Model Code", value="REPORT_NAME")
    model_type = st.selectbox("Model Type", options=["PRINT", "EXPORT", "QUERY"])
    parent_code = st.text_input("Parent Column Code (可选)", value="")

col1, col2 = st.columns(2)
with col1:
    sql_input = st.text_area("1. 粘贴查询 SQL", height=250)
with col2:
    ddl_input = st.text_area("2. 粘贴 DDL 语句", height=250)

if st.button("🚀 开始生成", type="primary", use_container_width=True):
    if sql_input and ddl_input:
        try:
            ddl_data = parse_ddl_robust(ddl_input)
            expr = parse_one(sql_input, read="mysql")
            alias_map = {t.alias_or_name: t.name for t in expr.find_all(exp.Table)}
            
            inserts = []
            now_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            for selection in expr.find(exp.Select).expressions:
                col_name, table_alias = "", ""
                if isinstance(selection, exp.Column):
                    col_name, table_alias = selection.name, selection.table
                elif isinstance(selection, exp.Alias):
                    col_name = selection.alias
                    if isinstance(selection.this, exp.Column):
                        table_alias = selection.this.table
                
                if not col_name: continue
                
                raw_camel = to_camel_case(col_name)
                column_code = f"{table_alias}{raw_camel[0].upper()}{raw_camel[1:]}" if table_alias else raw_camel
                
                real_table = alias_map.get(table_alias, "")
                cn_name = "未知字段"
                if real_table in ddl_data and col_name in ddl_data[real_table]:
                    cn_name = ddl_data[real_table][col_name]
                else:
                    for t in ddl_data:
                        if col_name in ddl_data[t]:
                            cn_name = ddl_data[t][col_name]
                            break
                
                dict_id = st.session_state.id_gen.next_id()
                p_val = f"'{parent_code}'" if parent_code else "NULL"
                
                sql = (f"INSERT INTO base_report_model_dict "
                       f"(dict_id, tenant_id, tenant_bu_id, column_code, column_name_cn, column_name_en, model_code, model_type, parent_column_code, column_type, create_user_id, create_user, create_time) "
                       f"VALUES({dict_id}, 1, 1, '{column_code}', '{cn_name}', '{column_code}', '{model_code}', '{model_type}', {p_val}, 'STRING', 1, '1', '{now_str}');")
                inserts.append(sql)
            st.success(f"成功生成 {len(inserts)} 条数据！")
            st.download_button(
                label="📥 下载生成的 SQL 文件",
                data="\n".join(inserts),
                file_name=f"insert_statements_{datetime.datetime.now().strftime('%Y%m%d%H%M')}.sql",
                mime="text/sql"
            )
            st.code("\n".join(inserts), language="sql")
        except Exception as e:
            st.error(f"解析失败，请检查输入格式。错误详情: {e}")