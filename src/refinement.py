import yaml
import json
import argparse
import autogen
from typing import List
from autogen.coding import LocalCommandLineCodeExecutor, MarkdownCodeExtractor
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.database import SQLiteDatabase
from src.database_utils import get_view_name_from_definition
from src.graph import schema_subgraph


def extract_codeblock_from_message_history(chat_history):
    """Extract code blocks from chat history."""
    code_blocks = []
    for message in chat_history:
        message_content = message['content']
        code_blocks += MarkdownCodeExtractor().extract_code_blocks(message_content)
    return code_blocks

def extract_view_definitions_from_code(chat_code_history):
    """
    Retrieves the code blocks from the chat history, if they contain the VIEW keyword.
    Includes: CREATE VIEW statements, DROP VIEW statements, ALTER VIEW statements, REPLACE VIEW statements, CREATE OR REPLACE VIEW statements, etc.
    """
    view_def_code = []
    for codeblock in chat_code_history:
        code = codeblock.code
        if "VIEW" in code:
            view_def_code.append(code)
    return view_def_code

def extract_view_names_from_code(chat_code_history):
    """
    Retrieves the view names from the code blocks containing the view definitions.
    """
    # TODO: code might contain several view definition statements. We need to separate them to extract all view names.
    view_names = []
    for codeblock in chat_code_history:
        code = codeblock.code
        # separate the code into lines
        code_lines = code.split("\n")
        for line in code_lines:
            view_name = get_view_name_from_definition(line)
            if view_name:
                view_names.append(view_name)
    # Remove duplicates
    view_names = list(set(view_names))
    return view_names

def run_analytics_chat_with_verification(analyst, critic, coder, verifier, schema_wording, chat_manager_config, n_rounds=8, n_chats=40, n_verification_rounds=6):
    """
    Run the group chat with verification. The chat involves the analyst, critic, coder, and verifier.
    The analyst and critic discuss to define the analysis task and views. The coder and verifier discuss to verify the views.
    """

    def state_transition(last_speaker, groupchat):
        """
        State transition logic for the group chat.
        1. The analyst and critic discuss for n_rounds rounds.
        2. The coder and verifier discuss until the view code is verified or max_verification_rounds is reached.
        """    
        # Retrieve the messages from the group chat
        messages = groupchat.messages
        last_message = messages[-1]['content'].lower()
        # Check if the task refinement phase should terminate
        if len(messages)==n_rounds or "goodbye" in last_message:
            # Allow execution only after task refinement
            if last_speaker is critic:
                return coder
            else:
                return None
        # Check if the verification phase should terminate
        elif len(messages)==n_rounds+n_verification_rounds:
            return None
        # State transition logic
        else:
            if last_speaker is analyst:
                return critic
            elif last_speaker is critic:
                return analyst
            elif last_speaker is coder:
                return verifier
            elif last_speaker is verifier:
                return coder
            else:
                return analyst

    # Initiate the chat
    manager_system_message = """Chat manager. Manage the group chat. The goal of this chat is to define useful database views for the user to access their data. 
First, let the analyst and critic discuss until they finalize their views. Then, let the coder and verifier check the views defined by the analyst. 
The verifier does not execute SQL code. The verifier only executes the view materialization tool provided by the coder.
If an error is detected, let the coder and verifier make the necessary corrections until the verifier reports no error."""
    chat_history = []
    code_history = []
    prev_chat_summaries = []
    prev_defined_views = []
    for chat_iter in range(n_chats):
        init_message = f"""Critic, I have the following database schema.

BEGIN SCHEMA

{schema_wording}
END SCHEMA

"""
        if chat_iter > 0:
            init_message += "From our previous discussion(s) I have taken the following notes: \n\n{}\n\n".format('\n'.join([f"{i+1}. {s}" for i, s in enumerate(prev_chat_summaries)]))
            init_message += "Here are some database views we defined in previous discussion(s): \n\n{}\n\n".format('\n'.join([f"{i+1}. {v}" for i, v in enumerate(prev_defined_views)]))
            init_message += "Let's try something different this time. We need to explore more aspects of the data and define new views.\n\n"
        init_message += "First, please suggest an analysis task for me to work on."
        try:
            groupchat = autogen.GroupChat(agents=[analyst, critic, coder, verifier], messages=[], max_round=n_rounds+n_verification_rounds, speaker_selection_method=state_transition)
            manager = autogen.GroupChatManager(groupchat=groupchat, llm_config=chat_manager_config, system_message=manager_system_message, human_input_mode="NEVER")
            result = analyst.initiate_chat(
                manager,
                message=init_message,
                summary_method="reflection_with_llm",
                is_termination_msg=lambda msg: "goodbye" in msg["content"].lower(),
            )
            prev_chat_summaries.append(result.summary)
            chat_history.append(result.chat_history)
            chat_code = extract_codeblock_from_message_history(result.chat_history)
            code_history += extract_view_definitions_from_code(chat_code)
            prev_defined_views += extract_view_names_from_code(chat_code)
        # Handle any chat error: e.g., maximum context length error, etc. 
        # Gracefully exit the sequential session, returning the progress so far.
        except Exception as e:
            print(f"Error in chat {chat_iter+1} / {n_chats}: {e}.")
            break

    return chat_history, code_history


def run_analytics_chat(analyst, critic, schema_wording, n_rounds=8, n_chats=40):
    """
    Run the group chat without verification. The chat involves the analyst and critic only.
    The analyst and critic discuss to define the analysis task and views.
    """
    # Initiate the chat
    chat_history = []
    code_history = []
    prev_chat_summaries = []
    prev_defined_views = []
    for chat_iter in range(n_chats):
        init_message = f"""Critic, I have the following database schema.

BEGIN SCHEMA

{schema_wording}
END SCHEMA

"""
        if chat_iter > 0:
            init_message += "From our previous discussion(s) I have taken the following notes: \n\n{}\n\n".format('\n'.join([f"{i+1}. {s}" for i, s in enumerate(prev_chat_summaries)]))
            init_message += "Here are some database views we defined in previous discussion(s): \n\n{}\n\n".format('\n'.join([f"{i+1}. {v}" for i, v in enumerate(prev_defined_views)]))
            init_message += "Let's try something different this time. We need to explore more aspects of the data and define new views.\n\n"
        init_message += "First, please suggest an analysis task for me to work on."
        try:
            result = analyst.initiate_chat(
                critic,
                message=init_message,
                summary_method="reflection_with_llm",
                max_round=n_rounds,
                is_termination_msg=lambda msg: "goodbye" in msg["content"].lower(),
            )
            prev_chat_summaries.append(result.summary)
            chat_history.append(result.chat_history)
            chat_code = extract_codeblock_from_message_history(result.chat_history)
            code_history += extract_view_definitions_from_code(chat_code)
            prev_defined_views += extract_view_names_from_code(chat_code)
        # Handle any chat error: e.g., maximum context length error, etc. 
        # Gracefully exit the sequential session, returning the progress so far.
        except Exception as e:
            print(f"Error in chat {chat_iter+1} / {n_chats}: {e}.")
            break

    return chat_history, code_history


def refine_schema(database, workspace, instructions_file, cache_seed=0, temperature=0.2, llm_timeout=240, model="gpt-4", verify=False, n_chats=10, n_rounds=8, n_verification_rounds=6, exec_timeout=60, subsample=False, n_samples=50, sample_size=5, sample_data=False):
    # Start runtime logging
    logging_session_id = autogen.runtime_logging.start(logger_type="file", config={"filename": f'refine_{database.db_name}_{cache_seed}.log'})

    # Define the default LLM configuration 
    llm_config = {
        "cache_seed": cache_seed,
        "temperature": temperature,
        "config_list": autogen.config_list_from_json("OAI_CONFIG_LIST",
                                                    filter_dict={"model": model}),
        "timeout": llm_timeout,
    }

    # Load the instructions file
    with open(instructions_file, "r") as f:
        instructions = yaml.safe_load(f)
        instructions_for_agents = {agent["name"]: agent["instructions"] for agent in instructions["agents"]}
    assert "Analyst" in instructions_for_agents, "Analyst instructions not found."
    assert "Critic" in instructions_for_agents, "Critic instructions not found."
    if verify:
        assert "Coder" in instructions_for_agents, "Coder instructions not found."
        assert "Verifier" in instructions_for_agents, "Verifier instructions not found"

    # Define the agents
    analyst = autogen.ConversableAgent(
        name="Analyst",
        llm_config=llm_config,
        system_message=instructions_for_agents["Analyst"],
        code_execution_config=False,
        human_input_mode="NEVER",
        is_termination_msg=lambda msg: "goodbye" in msg["content"].lower() or "good bye" in msg["content"].lower(),
    )

    critic = autogen.ConversableAgent(
        name="Critic",
        llm_config=llm_config,
        system_message=instructions_for_agents["Critic"],
        code_execution_config=False,
        human_input_mode="NEVER",
    )

    if verify:
        verifier = autogen.UserProxyAgent(
            name="Verifier",
            system_message=instructions_for_agents["Verifier"],
            llm_config=False,
            human_input_mode="NEVER",
            code_execution_config={'executor': LocalCommandLineCodeExecutor(timeout=exec_timeout, work_dir=workspace,)},
        )

        coder = autogen.ConversableAgent(
        name="Coder",
        llm_config=llm_config,
        system_message=instructions_for_agents["Coder"],
        code_execution_config=False,
        human_input_mode="NEVER",
    )

    # Set the agent descriptions
    analyst.description = "The Analyst is responsible for analyzing the data in the database. The Analyst writes SQL queries to analyze the data in the database. The Analyst works together with the Critic to improve the analysis. The Analyst defines views of the database to make the analysis easier."
    critic.description = "The Critic evaluates the code written by the Analyst. The Critic does not write code. The Critic provides feedback to the Analyst. The Critic requests an analysis task from the Analyst and refines the task. The Critic suggests views to be defined by the Analyst. The Critic provides feedback to the Analyst on the views defined by the Analyst."
    if verify:
        coder.description = "The Coder implements and corrects the code written by the Analyst. The Coder communicates with he Verifier to materialize and validate the views. The Coder corrects syntax errors and makes other necessary changes in the view definitions provided by the Analyst."
        verifier.description = "The Verifier executes the code written by the Analyst and reports the results. The Verifier provides only execution feedback."

    # Setup verification via execution
    if verify:
        # Register the view materialization tool
        def materialize_view_tool(view_definitions_list: List[str]) -> List[str]:
            # TODO: Persist the views only if the verification is successful, within a single chat sequence.
            exec_feedback = []
            for view_definition in view_definitions_list:
                exec_feedback.append(database.materialize_view(view_definition, persist=True))
            return exec_feedback
        # Register the tool signature with the assistant agent.
        coder.register_for_llm(name="materialize_view_tool", description="A python function that helps one materialize a database view defined in SQL.")(materialize_view_tool)
        # Register the tool function with the user proxy agent.
        verifier.register_for_execution(name="materialize_view_tool")(materialize_view_tool)

    if not subsample:
        # Get the schema wording
        schema_wording = database.schema_wording(selected_tables=None, include_sample_data=sample_data)

        # Setup the multi-agent chat
        if verify:
            chat_history, code_history = run_analytics_chat_with_verification(analyst, critic, coder, verifier, schema_wording, chat_manager_config=llm_config, n_chats=n_chats, n_rounds=n_rounds, n_verification_rounds=n_verification_rounds)
        else:
            chat_history, code_history = run_analytics_chat(analyst, critic, schema_wording, n_chats=n_chats, n_rounds=n_rounds)
    else:
        # Construct the schema graph
        schema_graph = database.schema_graph()
        chat_history = []
        code_history = []
        for i in range(n_samples):
            # Subsample the database schema and get the schema wording for the sample
            selected_tables = schema_subgraph(schema_graph, n_nodes=sample_size)
            schema_wording_i = database.schema_wording(selected_tables=selected_tables, include_sample_data=sample_data)
            
            # Setup the multi-agent chat
            if verify:
                chat_history_i, code_history_i = run_analytics_chat_with_verification(analyst, critic, coder, verifier, schema_wording_i, chat_manager_config=llm_config, n_chats=n_chats, n_rounds=n_rounds, n_verification_rounds=n_verification_rounds)
            else:
                chat_history_i, code_history_i = run_analytics_chat(analyst, critic, schema_wording_i, n_chats=n_chats, n_rounds=n_rounds)

            # Append the chat and code history
            chat_history += chat_history_i
            code_history += code_history_i

    # End logging
    autogen.runtime_logging.stop()

    return chat_history, code_history

def main():
    """
    Run the schema refinement process using a local SQLite database.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--workspace", type=str, help="Workspace directory to store the code files.")
    parser.add_argument("--db_name", type=str, help="Database name.")
    parser.add_argument("--db_file", type=str, help="Path to the '.db' file.")
    parser.add_argument("--instr_file", type=str, help="Path to the instructions file.")
    parser.add_argument("--cache_seed", type=int, default=13, help="Cache seed for the LLM.")
    parser.add_argument("--temperature", type=float, default=0.2, help="Temperature for the LLM.")
    parser.add_argument("--timeout", type=int, default=240, help="Timeout for the LLM.")
    parser.add_argument("--model", type=str, default="gpt-4o", help="Model for the LLM.")
    parser.add_argument("--verify", action="store_true", help="Enable verification of the views.")
    parser.add_argument("--n_chats", type=int, default=10, help="Number of chats to run in a sequence.")
    parser.add_argument("--n_rounds", type=int, default=8, help="Number of rounds in each chat.")
    parser.add_argument("--n_verification_rounds", type=int, default=6, help="Number of verification rounds in each chat.")
    parser.add_argument("--subsample", action="store_true", help="Subsample the database schema / data.")
    parser.add_argument("--n_samples", type=int, default=20, help="Number of schema samples.")
    parser.add_argument("--n_sampled_tables", type=int, default=5, help="Number of tables in each schema sample.")
    parser.add_argument("--sample_data", action="store_true", help="Sample data from the database to include in the schema wording.")
    args = parser.parse_args()
    os.makedirs(args.workspace, exist_ok=True)
    db = SQLiteDatabase(args.db_name, args.db_file)
    chat_history, code_history = refine_schema(db, args.workspace, args.instr_file, cache_seed=args.cache_seed, temperature=args.temperature, llm_timeout=args.timeout, model=args.model, verify=args.verify, n_chats=args.n_chats, n_rounds=args.n_rounds, n_verification_rounds=args.n_verification_rounds, subsample=args.subsample, n_samples=args.n_samples, sample_size=args.n_sampled_tables, sample_data=args.sample_data)
    with open(os.path.join(args.workspace, "chat_history.txt"), "w") as f:
        for chat in chat_history:
            f.write(json.dumps(chat) + "\n")
    with open(os.path.join(args.workspace, "code_history.sql"), "w") as f:
        for code in code_history:
            f.write(code + "\n")


if __name__ == "__main__":
    main()
