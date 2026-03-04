import streamlit as st
import re
import datetime
import time
import threading
from sqlglot import parse_one, exp, errors

# --- ID生成器逻辑 (保持不变) ---
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

# --- 辅助函数 ---
def to_camel_case(snake_str):
    if not snake_str: return ""
    components = snake_str.split('_')
    return components[0] + ''.join(x.title() for x in components[1:])

def parse_ddl_robust(ddl_text):
    table_dict = {}
    # 匹配 CREATE TABLE 块
    table_blocks = re.findall(r'CREATE\s+TABLE\s+`?(\w+)`?\s*\((.*?)\)\s*(?:ENGINE|COMMENT|COLLATE|;)', ddl_text, re.S | re.I)
    for table_name, content in table_blocks:
        cols = {}
        lines = content.split('\n')
        for line in lines:
            line = line.strip()
            # 匹配列名和注释
            col_match = re.search(r'^`?(\w+)`?\s+.*COMMENT\s+\'([^\']+)\'', line, re.I)
            if col_match:
                cols[col_match.group(1)] = col_match.group(2)
        table_dict[table_name] = cols
    return table_dict

# --- 校验函数 ---
def validate_sql_syntax(sql):
    if not sql.strip(): return None, ""
    try:
        parsed = parse_one(sql, read="mysql")
        return True, "✅ SQL 语法正确"
    except errors.ParseError as e:
        # 只取前100个字符避免UI撑爆
        return False, f"❌ SQL 语法错误: {str(e)[:100]}..."
    except Exception:
        return False, "❌ SQL 解析异常"

def validate_ddl_content(ddl):
    if not ddl.strip(): return None, ""
    data = parse_ddl_robust(ddl)
    if not data:
        return False, "❌ 未检测到有效的 CREATE TABLE 语句或 COMMENT 备注"
    tables_found = ", ".join(data.keys())
    return True, f"✅ 已识别表: {tables_found}"

# --- 界面部分 ---
st.set_page_config(page_title="SQL报表字典工具", layout="wide")

st.title("📊 报表字典 Insert 语句生成器")

# 侧边栏配置
with st.sidebar:
    st.header("⚙️ 参数配置")
    model_code = st.text_input("Model Code", value="REPORT_NAME", help="对应表中的 model_code 字段")
    model_type = st.selectbox("Model Type", options=["PRINT", "EXPORT", "QUERY"], help="报表类型")
    parent_code = st.text_input("Parent Column Code (可选)", value="")
    st.divider()
    st.markdown("### 使用说明")
    st.caption("1. 粘贴查询 SQL (支持别名)\n2. 粘贴 DDL (需包含字段 COMMENT)\n3. 点击开始生成")

# 主界面布局
col1, col2 = st.columns(2)

with col1:
    sql_input = st.text_area("1. 粘贴查询 SQL", height=300, placeholder="SELECT a.id FROM table_a a...")
    # SQL 实时校验反馈
    sql_ok, sql_msg = validate_sql_syntax(sql_input)
    if sql_ok is True:
        st.success(sql_msg)
    elif sql_ok is False:
        st.error(sql_msg)

with col2:
    ddl_input = st.text_area("2. 粘贴 DDL 语句", height=300, placeholder="CREATE TABLE `xxx` ( `col` varchar(1) COMMENT '备注' )...")
    # DDL 实时校验反馈
    ddl_ok, ddl_msg = validate_ddl_content(ddl_input)
    if ddl_ok is True:
        st.info(ddl_msg)
    elif ddl_ok is False:
        st.warning(ddl_msg)

st.divider()

# 生成按钮逻辑
if st.button("✨ 开始生成 INSERT 语句", type="primary", use_container_width=True):
    # 最终校验
    if not sql_input or not ddl_input:
        st.error("请输入 SQL 和 DDL 内容后再执行！")
    elif sql_ok is False:
        st.error("SQL 语法校验未通过，请修正后再试。")
    elif not ddl_ok:
        st.error("DDL 未能解析到任何表结构，请确认是否包含 COMMENT。")
    else:
        try:
            ddl_data = parse_ddl_robust(ddl_input)
            expr = parse_one(sql_input, read="mysql")
            
            # 建立别名映射
            alias_map = {t.alias_or_name: t.name for t in expr.find_all(exp.Table)}
            
            inserts = []
            now_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # 解析 SELECT 字段
            selections = expr.find(exp.Select).expressions
            
            for selection in selections:
                col_name, table_alias = "", ""
                
                if isinstance(selection, exp.Column):
                    col_name, table_alias = selection.name, selection.table
                elif isinstance(selection, exp.Alias):
                    col_name = selection.alias
                    if isinstance(selection.this, exp.Column):
                        table_alias = selection.this.table
                
                if not col_name: continue
                
                # 转换 column_code
                raw_camel = to_camel_case(col_name)
                # 别名逻辑：如果带了别名，拼接别名以防多表同名冲突
                column_code = f"{table_alias}{raw_camel[0].upper()}{raw_camel[1:]}" if table_alias else raw_camel
                
                # 查找备注
                real_table = alias_map.get(table_alias, "")
                cn_name = "未知字段"
                if real_table in ddl_data and col_name in ddl_data[real_table]:
                    cn_name = ddl_data[real_table][col_name]
                else:
                    # 全局匹配
                    for t in ddl_data:
                        if col_name in ddl_data[t]:
                            cn_name = ddl_data[t][col_name]
                            break
                
                # 生成 SQL
                dict_id = st.session_state.id_gen.next_id()
                p_val = f"'{parent_code}'" if parent_code else "NULL"
                
                sql = (f"INSERT INTO base_report_model_dict "
                       f"(dict_id, tenant_id, tenant_bu_id, column_code, column_name_cn, column_name_en, model_code, model_type, parent_column_code, column_type, create_user_id, create_user, create_time) "
                       f"VALUES({dict_id}, 1, 1, '{column_code}', '{cn_name}', '{column_code}', '{model_code}', '{model_type}', {p_val}, 'STRING', 1, '1', '{now_str}');")
                inserts.append(sql)
            
            if inserts:
                st.success(f"🎊 生成成功！共解析出 {len(inserts)} 个字段。")
                st.download_button("📥 下载 SQL 结果", data="\n".join(inserts), file_name="report_dict.sql")
                st.code("\n".join(inserts), language="sql")
            else:
                st.warning("未能在 SQL 中解析到有效的字段。")
                
        except Exception as e:
            st.error(f"🔥 程序运行时崩溃: {e}")