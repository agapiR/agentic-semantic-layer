import re
import json
import argparse
import autogen
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.database import SQLiteDatabase
from src.process_sql import Schema, get_sql
from src.database_utils import get_view_name_from_definition


def get_llm_assistant():
    llm_assistant = autogen.ConversableAgent(
        "Assistant",
        system_message="""Assistant. You are given the transcript of the conversation between a group of agents. The Critic makes suggestions and the Analyst writes code.
    Please, summarize each conversation by providing the analysis tasks and the corresponding database views that the Analyst wrote. 
    You should identify the high-level analysis task and provide a simple but thorough description of the task as it would be expressed by a user.
    Do not include specific requests for views. Do not include implementation instructions. Provide only the high-level description of the analysis task, as a user would express it.
    Respond with just the tasks and views, without the conversation context.
    Your response should follow the format:
    {"task description": task, "views": views}
    where task is a string and views is a list of strings. Note, DROP VIEW statements should not be included in your response. 
    Do not include more that one definitions with the same view name. If the view is defined multiple times, include only the final definition. 
    Make sure to use the final view definition after the code correction, if any.
    Make sure to include the complete SQL query that defines the view each view, not just the view name. If you cannot find the complete SQL query that defines the view, do not include this view in the response.
    For example your response could be:
    {"task description": "Rank the customers according to the amount of orders and find the most popular occupation among the top 100.", "views": ["CREATE VIEW Customer_Order_Count AS SELECT customer_id, COUNT(order_id) FROM orders GROUP BY customer_id;"]}
    You can assume that the database schema is provided in the conversation transcript.
    Each conversation usually corresponds to one pair of tasks and final views. You can ignore the intermediate steps for task and view refinement. Just provide the final task and views.
    """,
        llm_config = {
                "cache_seed": None,
                "temperature": 0.0,
                "config_list": autogen.config_list_from_json("OAI_CONFIG_LIST",
                                                            filter_dict={"model": 'gpt-4o'}),
                "timeout": 240,
            },
        human_input_mode="NEVER",
    )

    return llm_assistant

def parse_chats_from_log_without_schema(chat_log_file):
    # Read the chat history
    with open(chat_log_file, 'r') as f:
        log_content = f.read()
    log_content = re.sub(r'\[autogen\.oai\.client:.*', ' ', log_content) # remove lines starting with [autogen.oai.client
    # Split the log content into individual chats. For each chat, remove the schema (for brevity).
    start_string = "BEGIN SCHEMA"
    end_string = "First, please suggest an analysis task for me to work on."
    schema_wording_content = re.escape(start_string) + '(.*?)' + re.escape(end_string)
    log_content = re.sub(schema_wording_content, 'SCHEMA REMOVED', log_content, flags=re.DOTALL) # remove schema
    parsed_chats = [chat.strip() for chat in log_content.split('SCHEMA REMOVED')[1:] if chat.strip()] # split chats and remove empty chats
    parsed_chats = [chat.replace('\n\n--------------------------------------------------------------------------------', ' ') for chat in parsed_chats] # remove boundary lines
    return parsed_chats

def parse_chats_from_log(chat_log_file):
    # Read the chat history
    with open(chat_log_file, 'r') as f:
        log_content = f.read()
    log_content = re.sub(r'\[autogen\.oai\.client:.*', ' ', log_content) # remove lines starting with [autogen.oai.client
    # Split the log content into individual chats.
    chat_start_string = "Critic, I have the following database schema."
    parsed_chats = [chat for chat in log_content.split(chat_start_string)[1:] if chat.strip()] # split chats and remove empty chats
    parsed_chats = [chat.strip() for chat in parsed_chats] # remove leading and trailing whitespaces
    # parsed_chats = [chat[:-len("Analyst (to chat_manager):")] for chat in parsed_chats if chat.endswith("Analyst (to chat_manager):")] # remove the suffix
    return parsed_chats

def parse_chats_from_autogen_log(chat_log_file):
    raise NotImplementedError("This function is not implemented yet.")

def instruction_generation(parsed_chats, instructions_file, chats_file=None, run_name='log'):
    # For each chat, ask the LLM agent to generate task - view pairs
    llm_assistant = get_llm_assistant()
    for chatid, chat in enumerate(parsed_chats):
        # Generate task - view pairs
        print(f"Generating task - view pairs for {run_name}, chat {chatid+1} / {len(parsed_chats)}...")
        try:
            response = llm_assistant.generate_reply(messages=[{"content": f"Please help me summarize the following conversation transcript: {chat}", "role": "user"}])
        except:
            print("Error in generating response. Skipping this chat.")
            continue
        # find all the task - view pairs from the response using regex .findall "{any string here}"
        try:
            task_view_pairs = re.findall(r'{"task description": "(.*?)", "views": \[(.*?)\]}', response)
            print(f"Found {len(task_view_pairs)} task - view pairs for {run_name}, chat {chatid+1} / {len(parsed_chats)}")
        except:
            print("Parsing error. Skipping this chat.")
            continue
        # for every task - view pair you find in the response, save the instruction in the instructions_file
        for task_view in task_view_pairs:
            task_text = task_view[0]
            view_text = task_view[1].replace('\\n', ' ').replace('\n', ' ')
            system_mssg = "Lens is a virtual assistant that helps the user navigate their database."
            task_view_dict =  {"messages": [{"role": "system", "content": system_mssg}, {"role": "user", "content": f"Lens, which parts of my data are useful to solve the following task: {task_text}"}, {"role": "assistant", "content": f"You might find the following views useful to solve the task: {view_text}"}]}
            with open(instructions_file, 'a') as f:
                f.write(json.dumps(task_view_dict) + '\n')
    
    return

def get_sql_from_text(sql_text):
    sql_text = sql_text.split(':', 1)[1].strip()
    sql_statements = [stmt.strip().strip('"') for stmt in sql_text.split('", "')] # split the SQL statements
    sql_statements = [stmt for stmt in sql_statements if stmt] # remove empty strings
    # remove comment lines (starting with --, /*, #)
    # TODO: figure out why this doesn't work
    for i, stmt in enumerate(sql_statements):
        sql_statements[i] = re.sub(r'--.*', ' ', stmt)
        sql_statements[i] = re.sub(r'/\*.*?\*/', ' ', stmt, flags=re.DOTALL)
        sql_statements[i] = re.sub(r'#.*', ' ', stmt)
    sql_statements = [stmt.replace('\\n', ' ').replace('\n', ' ') for stmt in sql_statements] # remove newlines
    return sql_statements

def get_query_from_view_definition(view_definition):
    start_string = "CREATE "
    end_string = "AS "
    pattern = re.escape(start_string) + '(.*?)' + re.escape(end_string)
    parsed_view_def = re.sub(pattern, 'CREATE STATEMENT', view_definition, flags=re.DOTALL)
    query = parsed_view_def.split('CREATE STATEMENT')[1:]
    if query:
        return query[0].strip()
    else:
        return None
    
def simplify_join_condition(sql_statement):
    """
    Replace all JOIN expressions with JOIN. For easier parsing of the SQL queries.
    """
    for join_opt in ['INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'FULL JOIN', 'LEFT OUTER JOIN', 'RIGHT OUTER JOIN', 'FULL OUTER JOIN']:
        sql_statement = sql_statement.replace(join_opt, 'JOIN')
        sql_statement = sql_statement.replace(join_opt.lower(), 'join')
    return sql_statement

def parse_sql(db, sql):
    schema = Schema(db.schema_dictionary(include_views=True))
    try:
        # parse the SQL given the schema
        sql_parsed = get_sql(schema, sql)
    except Exception as e:
        # error parsing the SQL
        return str(e)
    return sql_parsed

def process_views(database, workspace, chat_log_file, generate_instructions=False):
    # Parse the chat log into individual chats
    parsed_chats_file = os.path.join(workspace, f'refine_{database.db_name}_chats.jsonl')
    parsed_chats = parse_chats_from_log(chat_log_file)
    with(open(parsed_chats_file, "a")) as f:
        for chatid, chat in enumerate(parsed_chats):
            f.write(json.dumps({"chat_id": chatid, "chat": chat}) + '\n')

    # Generate Task-View pairs for each chat in the log file, to be used for instruction tuning
    instructions_file = os.path.join(workspace, f'refine_{database.db_name}_task_views.jsonl')
    if generate_instructions:
        instruction_generation(parsed_chats, instructions_file, run_name=chat_log_file.split('.')[0])
    if not os.path.exists(instructions_file):
        assert False, f"Task-View pairs file {instructions_file} does not exist. Please generate the instructions first."
    
    # Parse the SQL queries for the views generated by the LLM
    views_parsed_file = os.path.join(workspace, f'refine_{database.db_name}_sql_parsed.jsonl')
    instructions_file_parsed_only = os.path.join(workspace, f'refine_{database.db_name}_task_views_parsed_only.jsonl')
    with open(views_parsed_file, 'w') as f_views_parsed:
        f_views_parsed.write('')
        with open(instructions_file, 'r') as f, open(instructions_file_parsed_only, 'w') as f_parsed:
            for line in f:
                entry = json.loads(line)
                # Extract the SQL queries
                view_text = entry['messages'][2]['content']
                sql_statements = get_sql_from_text(view_text)
                # Process the SQL queries
                parsing_successful = True
                for stmt in sql_statements:
                    sql_parsed = parse_sql(database, get_query_from_view_definition(simplify_join_condition(stmt)))
                    if not isinstance(sql_parsed, dict):
                        parsing_successful = False
                    view_name = get_view_name_from_definition(stmt)
                    f_views_parsed.write(json.dumps({"view_name": view_name, "sql": stmt, "sql_parsed": sql_parsed}) + '\n')
                if parsing_successful:
                    f_parsed.write(json.dumps(entry) + '\n')


def main():
    """
    Run the post-processing script using a local SQLite database.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--workspace", type=str, help="Results workspace directory.")
    parser.add_argument("--db_name", type=str, help="Database name.")
    parser.add_argument("--db_file", type=str, help="Path to the '.db' file.")
    parser.add_argument("--log_file", type=str, help="Path to the chat log file.")
    parser.add_argument("--gen_instruct", action='store_true', help="Generate instructions for fine-tuning.")
    args = parser.parse_args()
    db = SQLiteDatabase(args.db_name, args.db_file)
    process_views(db, args.workspace, args.log_file, generate_instructions=args.gen_instruct)

if __name__ == "__main__":
    main()