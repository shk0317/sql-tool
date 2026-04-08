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

def escape_sql_value(value):
    return value.replace("'", "''") if value else value

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

def build_insert_sql(column_code, column_name_cn, model_code, model_type, parent_code, now_str, column_type="STRING"):
    dict_id = st.session_state.id_gen.next_id()
    p_val = f"'{escape_sql_value(parent_code)}'" if parent_code else "NULL"
    safe_column_code = escape_sql_value(column_code)
    safe_column_name_cn = escape_sql_value(column_name_cn)
    return (
        f"INSERT INTO base_report_model_dict "
        f"(dict_id, tenant_id, tenant_bu_id, column_code, column_name_cn, column_name_en, model_code, model_type, parent_column_code, column_type, create_user_id, create_user, create_time) "
        f"VALUES({dict_id}, 1, 1, '{safe_column_code}', '{safe_column_name_cn}', '{safe_column_code}', '{escape_sql_value(model_code)}', '{escape_sql_value(model_type)}', {p_val}, '{escape_sql_value(column_type)}', 1, '1', '{now_str}');"
    )

def render_sql_result(inserts, download_key, file_prefix):
    sql_content = "\n".join(inserts)
    st.success(f"成功生成 {len(inserts)} 条数据！")
    st.code(sql_content, language="sql")
    st.download_button(
        label="📥 下载 SQL 文件",
        data=sql_content,
        file_name=f"{file_prefix}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.sql",
        mime="text/sql",
        use_container_width=True,
        key=download_key,
    )

def extract_schema_description(annotation_lines):
    annotation_text = " ".join(annotation_lines)
    schema_match = re.search(r'@Schema\s*\((.*?)\)', annotation_text)
    if not schema_match:
        return ""
    description_match = re.search(r'description\s*=\s*"([^"]*)"', schema_match.group(1))
    return description_match.group(1).strip() if description_match else ""

def analyze_java_type(field_type):
    normalized = re.sub(r'@\w+(?:\([^)]*\))?\s*', '', field_type).strip()
    normalized = re.sub(r'\b(final|static|transient|volatile)\b', '', normalized).strip()
    normalized = normalized.replace("?", "")
    is_list = bool(re.search(r'\b(List|Set|Collection|ArrayList|LinkedList|HashSet)\s*<', normalized)) or normalized.endswith("[]")
    generic_match = re.search(r'<\s*([A-Za-z_]\w*)\s*>', normalized)
    base_type = generic_match.group(1) if generic_match else re.sub(r'[\[\]\s]', '', normalized).split('.')[-1]
    return {
        "raw_type": normalized,
        "base_type": base_type,
        "is_list": is_list,
    }

def parse_java_entities(java_text):
    entities = {}
    class_order = []
    entity_diagnostics = {}
    annotation_buffer = []
    pending_description = ""
    current_class = None
    brace_depth = 0

    for raw_line in java_text.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        class_match = re.search(r'\bclass\s+(\w+)\b', line)
        if class_match:
            current_class = class_match.group(1)
            if current_class not in entities:
                entities[current_class] = []
                class_order.append(current_class)
                entity_diagnostics[current_class] = {"field_count": 0}
            brace_depth += line.count("{") - line.count("}")
            annotation_buffer = []
            pending_description = ""
            continue

        if current_class is None:
            continue

        if line.startswith("@"):
            annotation_buffer.append(line)
            description = extract_schema_description(annotation_buffer)
            if description:
                pending_description = description
            brace_depth += line.count("{") - line.count("}")
            continue

        field_match = re.match(r'^(?:private|protected|public)\s+(.+?)\s+(\w+)\s*(?:=[^;]*)?;', line)
        if field_match:
            field_type_info = analyze_java_type(field_match.group(1))
            field_name = field_match.group(2)
            if field_name != "serialVersionUID":
                entities[current_class].append(
                    {
                        "field_name": field_name,
                        "field_type": field_type_info["base_type"],
                        "is_list": field_type_info["is_list"],
                        "column_name_cn": pending_description or field_name,
                    }
                )
                entity_diagnostics[current_class]["field_count"] += 1
            annotation_buffer = []
            pending_description = ""

        brace_depth += line.count("{") - line.count("}")
        if brace_depth <= 0:
            current_class = None
            brace_depth = 0
            annotation_buffer = []
            pending_description = ""

    return entities, class_order, entity_diagnostics

def expand_java_entity_fields(entities, root_class_name):
    expanded_fields = []
    primitive_types = {
        "String", "Long", "Integer", "Short", "Byte", "Boolean", "Double", "Float",
        "BigDecimal", "BigInteger", "Date", "LocalDate", "LocalDateTime", "LocalTime",
        "Timestamp", "Object", "Map", "JSONObject"
    }

    def walk(class_name, seen_classes=None, inherited_parent_code=""):
        if class_name not in entities:
            return

        seen_classes = seen_classes or set()
        if class_name in seen_classes:
            return

        next_seen_classes = seen_classes | {class_name}
        for field in entities[class_name]:
            next_code = field["field_name"]
            next_name = field["column_name_cn"]
            field_type = field["field_type"]
            is_nested_entity = field_type in entities and field_type not in primitive_types
            current_column_type = "LIST" if field.get("is_list") else "OBJECT"

            if is_nested_entity:
                expanded_fields.append(
                    {
                        "source_entity": class_name,
                        "source_field": field["field_name"],
                        "source_field_type": field_type,
                        "column_code": next_code,
                        "column_name_cn": next_name,
                        "column_type": current_column_type,
                        "parent_column_code": inherited_parent_code,
                    }
                )
                walk(field_type, next_seen_classes, next_code)
            else:
                expanded_fields.append(
                    {
                        "source_entity": class_name,
                        "source_field": field["field_name"],
                        "source_field_type": field_type,
                        "column_code": next_code,
                        "column_name_cn": next_name,
                        "column_type": "STRING",
                        "parent_column_code": inherited_parent_code,
                    }
                )

    walk(root_class_name)
    return expanded_fields

# --- 界面部分 ---
st.set_page_config(page_title="SQL报表字典工具", layout="wide")

st.title("📊 报表字典 Insert 语句生成器")
st.info("支持通过 SQL + DDL 或 Java 实体类识别字段，生成 report_model_dict 插入语句")

with st.sidebar:
    st.header("⚙️ 参数配置")
    model_code = st.text_input("Model Code", value="REPORT_NAME")
    model_type = st.selectbox("Model Type", options=["PRINT", "EXPORT", "QUERY"])
    parent_code = st.text_input("Parent Column Code (可选)", value="")

tab_sql, tab_java = st.tabs(["SQL + DDL", "Java 实体类"])

with tab_sql:
    col1, col2 = st.columns(2)
    with col1:
        sql_input = st.text_area("1. 粘贴查询 SQL", height=250)
    with col2:
        ddl_input = st.text_area("2. 粘贴 DDL 语句", height=250)

    if st.button("🚀 通过 SQL + DDL 生成", type="primary", use_container_width=True):
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

                    if not col_name:
                        continue

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

                    inserts.append(build_insert_sql(column_code, cn_name, model_code, model_type, parent_code, now_str))

                render_sql_result(inserts, "download_sql_ddl", "sql_ddl_insert")
            except Exception as e:
                st.error(f"解析失败，请检查输入格式。错误详情: {e}")
        else:
            st.warning("请先同时输入查询 SQL 和 DDL 语句。")

with tab_java:
    java_input = st.text_area("粘贴 Java 实体类代码", height=520, placeholder='public class OrderVO {\n    @Schema(description = "主单号id")\n    private String mainOrderId;\n\n    @Schema(description = "明细信息")\n    private DetailVO detail;\n}\n\npublic class DetailVO {\n    @Schema(description = "商品编码")\n    private String itemCode;\n}')

    if st.button("🚀 通过 Java 实体类生成", type="primary", use_container_width=True):
        if java_input:
            try:
                entities, class_order, entity_diagnostics = parse_java_entities(java_input)
                if not class_order:
                    raise ValueError("未识别到实体类定义")

                fields = expand_java_entity_fields(entities, class_order[0])
                now_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                inserts = [
                    build_insert_sql(
                        field["column_code"],
                        field["column_name_cn"],
                        model_code,
                        model_type,
                        field.get("parent_column_code") or parent_code,
                        now_str,
                        field.get("column_type", "STRING")
                    )
                    for field in fields
                ]
                compare_rows = [
                    {
                        "来源实体类": field["source_entity"],
                        "实体字段": field["source_field"],
                        "字段类型": field["source_field_type"],
                        "生成column_code": field["column_code"],
                        "生成column_name_cn": field["column_name_cn"],
                        "生成column_type": field["column_type"],
                        "parent_column_code": field["parent_column_code"] or "",
                        "对应SQL": inserts[index],
                    }
                    for index, field in enumerate(fields)
                ]

                st.subheader("识别结果")
                st.write(f"识别到 {len(class_order)} 个实体类，默认以 `{class_order[0]}` 作为主实体。")
                st.table(
                    [
                        {
                            "实体类": class_name,
                            "识别字段数": entity_diagnostics[class_name]["field_count"],
                            "格式": "正常" if entity_diagnostics[class_name]["field_count"] > 0 else "未识别到字段",
                        }
                        for class_name in class_order
                    ]
                )

                if inserts:
                    st.subheader("字段与 SQL 对照")
                    st.dataframe(compare_rows, use_container_width=True)
                    render_sql_result(inserts, "download_java_entity", "java_entity_insert")
                else:
                    st.warning("未识别到可生成的实体类字段，请检查字段定义、嵌套关系或 @Schema 注解格式。")
            except Exception as e:
                st.error(f"解析失败，请检查 Java 实体类格式。错误详情: {e}")
        else:
            st.warning("请先输入 Java 实体类代码。")
