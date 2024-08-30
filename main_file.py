import openai
import os
import pandas as pd
import warnings
warnings.filterwarnings("ignore")
import random
import json 
import sqlite3
from tqdm import tqdm
import sqlglot
from dotenv import load_dotenv
import sys 
import logging

load_dotenv()
openai_api_key = os.getenv("OPENAI_API_KEY")

openai.api_key = openai_api_key

log_file_path = 'error.log'
if os.path.exists(log_file_path):
    os.remove(log_file_path)

logging.basicConfig(
    filename=log_file_path,            
    level=logging.ERROR,          
    format='%(asctime)s - %(levelname)s - %(message)s' 
)

def get_context_msg(schema):

    context = [ {'role':'system', 'content':schema}]
    return context

def text_to_sql(question):
    tables = get_table_names()

    prompt = f"""Given the following tables:
    {tables}

Convert the following question to SQL, and please don't give any unnecessary character in output:
Question: {question}

SQL:"""
    schema = get_schemas()
    context = get_context_msg(schema)
    context.append({'role': 'user', 'content': prompt})

    response = openai.chat.completions.create(
        model="gpt-3.5-turbo",  # You can also use "gpt-4" if you have access
        messages=context,
        temperature=0.9
    )
    return response.choices[0].message.content.strip('```').strip('sql')

def compare_results(df1, df2):
    if df1 is None or df2 is None:
        return False
    if df1.empty and df2.empty:
        return True
    return len(df1) == len(df2)

# Dev file for BIRD
path = 'dev.json'
with open(path) as f:
    data = json.load(f)

def get_table_names():
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        tables = [t[0] for t in tables]
    except Exception as e:
        logging.error("An error occurred connecting %s db %s\n", db_name, e)
    return ','.join(tables)

def get_schemas():
    cursor = conn.cursor()
    try:

        cursor.execute("SELECT sql FROM sqlite_master WHERE type='table';")
        table_schema = cursor.fetchall()
        table_schema = [t[0] for t in table_schema]
    except Exception as e:
        logging.error("An error occurred connecting %s db %s\n", db_name, e)

    return '\n'.join(table_schema)



db_name = sys.argv[1]

conn = sqlite3.connect(db_name + '.sqlite')

data = [d for d in data if d['db_id'] == db_name]
random.shuffle(data)
data = data[:30]

def validate_sql_expression(sql_expression):
    try:
        parsed_expression = sqlglot.parse_one(sql_expression, dialect="mysql")
        print("The SQL expression is valid.")
        return parsed_expression
    except Exception as e:
        logging.error("Invalid SQL expression %s\n", e)


total, pred = 0, 0
for i, d in tqdm(enumerate(data)):

    test_question = d['question']
    test_sql = d['SQL']
    pred_sql = text_to_sql(test_question)
    validate_sql_expression(pred_sql)
    test_df = pd.read_sql(test_sql, con = conn)
    try:
        pred_df = pd.read_sql(pred_sql, con = conn)
        flag = compare_results(test_df, pred_df)
        if flag:
            pred += 1
    except Exception as e:
        logging.error('Error occurred fetching results %s\n', e)
    total += 1

accuracy = pred / total
print(f"Accuracy: {accuracy:.2f}")