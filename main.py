from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
import re
import json
from typing import Dict, List, Any

app = FastAPI(title="SQL Schema Parser API")

# ---------- Core Functions (same as before) ----------

def extract_tables_and_ddl(sql_text: str) -> Dict[str, Dict[str, Any]]:
    tables: Dict[str, Dict[str, Any]] = {}

    create_pattern = re.compile(
        r'(CREATE TABLE\s+"(?P<schema>[^"]+)"\."(?P<table>[^"]+)"[\s\S]*?;)',
        re.IGNORECASE
    )

    for m in create_pattern.finditer(sql_text):
        full_stmt = m.group(1)
        schema = m.group('schema')
        table = m.group('table')
        tables[table] = {
            'name': table,
            'schema_name': schema,
            'schema_stmt': full_stmt,
            'constraints_raw': [],
            'primary_keys': [],
            'foreign_keys': [],
            'dependencies': set(),
        }

    return tables


def parse_constraints(sql_text: str, tables: Dict[str, Dict[str, Any]]) -> None:
    alter_pattern = re.compile(
        r'(ALTER TABLE\s+"(?P<schema>[^"]+)"\."(?P<table>[^"]+)"[\s\S]*?;)',
        re.IGNORECASE
    )

    for m in alter_pattern.finditer(sql_text):
        stmt = m.group(1)
        table = m.group('table')
        if table not in tables:
            continue
        tables[table]['constraints_raw'].append(stmt)

        # Primary keys
        for pm in re.finditer(r'PRIMARY\s+KEY\s*\((?P<cols>[^\)]+)\)', stmt, re.IGNORECASE):
            cols = [c.strip().strip('"') for c in pm.group('cols').split(',')]
            for c in cols:
                if c and c not in tables[table]['primary_keys']:
                    tables[table]['primary_keys'].append(c)

        # Foreign keys
        for fkm in re.finditer(
            r'FOREIGN\s+KEY\s*\((?P<local>[^\)]+)\)[\s\S]*?REFERENCES\s+"(?P<ref_schema>[^"]+)"\."(?P<ref_table>[^"]+)"\s*\((?P<ref_cols>[^\)]+)\)',
            stmt, re.IGNORECASE
        ):
            local_cols = ', '.join([c.strip().strip('"') for c in fkm.group('local').split(',')])
            ref_table = fkm.group('ref_table')
            ref_cols = ', '.join([c.strip().strip('"') for c in fkm.group('ref_cols').split(',')])
            tables[table]['foreign_keys'].append({
                'column': local_cols,
                'references': f"{ref_table}({ref_cols})",
            })
            tables[table]['dependencies'].add(ref_table)


def to_serializable(items: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for table_name in sorted(items.keys()):
        item = items[table_name]
        constraints_joined = ''
        if item['constraints_raw']:
            constraints_joined = '\n  '.join(s.strip() for s in item['constraints_raw'])
        out.append({
            'name': item['name'],
            'primary_keys': item['primary_keys'],
            'foreign_keys': item['foreign_keys'],
            'schema': item['schema_stmt'],
            'constraints': constraints_joined,
            'dependencies': sorted(item['dependencies']),
        })
    return out


# ---------- API Routes ----------

@app.get("/")
def root():
    return {"message": "SQL Schema Parser API is running!"}


@app.post("/parse-sql")
async def parse_sql_file(file: UploadFile = File(...)):
    """
    Upload a .sql file and receive parsed JSON output.
    """
    if not file.filename.endswith(".sql"):
        raise HTTPException(status_code=400, detail="Please upload a .sql file")

    content = await file.read()
    sql_text = content.decode("utf-8", errors="ignore")

    # Parse
    tables = extract_tables_and_ddl(sql_text)
    parse_constraints(sql_text, tables)
    json_data = to_serializable(tables)

    return JSONResponse(content=json_data)


@app.post("/parse-text")
async def parse_sql_text(payload: dict):
    """
    Pass SQL as plain text in a JSON payload:
    { "sql_text": "CREATE TABLE ...;" }
    """
    sql_text = payload.get("sql_text")
    if not sql_text:
        raise HTTPException(status_code=400, detail="Missing 'sql_text' in request")

    tables = extract_tables_and_ddl(sql_text)
    parse_constraints(sql_text, tables)
    json_data = to_serializable(tables)

    return JSONResponse(content=json_data)
