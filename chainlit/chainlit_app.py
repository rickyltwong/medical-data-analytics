import chainlit as cl
from langchain_openai import AzureChatOpenAI
from langchain_community.utilities import SQLDatabase
from snowflake.snowpark import Session
from langchain.chains import create_sql_query_chain
from dotenv import load_dotenv
from langchain.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough
from langchain_core.messages import HumanMessage
from langchain.schema.runnable.config import RunnableConfig
import os
import re
import urllib.parse

# Load environment variables
load_dotenv()
username = os.getenv("SNOWFLAKE_USER")
password = os.getenv("SNOWFLAKE_PASSWORD")
snowflake_account = os.getenv("SNOWFLAKE_ACCOUNT")
database = os.getenv("SNOWFLAKE_DATABASE")
schema = os.getenv("SNOWFLAKE_SCHEMA")
warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
role = os.getenv("SNOWFLAKE_ROLE")

# Properly escape password for URL
escaped_password = urllib.parse.quote_plus(password)

snowflake_url = f"snowflake://{username}:{escaped_password}@{snowflake_account}/{database}/{schema}?warehouse={warehouse}&role={role}"

include_tables = ["fct_medicare_spending", "dim_drugs"]

db = SQLDatabase.from_uri(
    snowflake_url, sample_rows_in_table_info=1, include_tables=include_tables
)

print(db.table_info)

llm = AzureChatOpenAI(
    temperature=0,
    api_key=os.environ["AZURE_OPENAI_API_KEY"],
    azure_endpoint=os.environ["AZURE_OPENAI_ENDPOINT"],
    azure_deployment=os.environ["AZURE_OPENAI_DEPLOYMENT_NAME"],
    openai_api_version=os.environ["AZURE_OPENAI_API_VERSION"],
)

# Template for generating SQL queries
query_prompt = PromptTemplate.from_template('''
Given an input question, create a query with syntactically correct dialect to run.
Don't hesitate to use Common Table Expressions (CTEs) if needed.

Only use the following tables:
{table_info}

Question: {question}
SQL Query:
''')

answer_prompt = PromptTemplate.from_template('''
Given an input question and SQL query results, provide a natural language answer.

Question: {question}
SQL Query: {query}
SQL Result: {result}

Answer:
''')

sql_query_chain = create_sql_query_chain(llm, db)

def execute_query(query):
    try:
        connection_parameters = {
            "account": snowflake_account,
            "user": username,
            "password": password,
            "role": role,
            "warehouse": warehouse,
            "database": database,
            "schema": schema,
        }
        session = Session.builder.configs(connection_parameters).create()
        
        result = session.sql(query).collect()
        return str(result)
    except Exception as e:
        return f"Error executing query: {str(e)}"


def process_question(question):
    formatted_response = sql_query_chain.invoke({"question": question})
    print(f"Raw LLM response:\n{formatted_response}")
    
    sql_query = None

    sql_code_block_match = re.search(r'```sql\s*(.*?)\s*```', formatted_response, re.DOTALL)
    if sql_code_block_match:
        sql_query = sql_code_block_match.group(1).strip()
    else:
        sql_query_match = re.search(r'SQLQuery:\s*"([^"]*)"', formatted_response)
        if sql_query_match:
            sql_query = sql_query_match.group(1).strip()
        else:
            sql_query_match = re.search(r'SQLQuery:\s*(.*?)(?:\n|$)', formatted_response)
            if sql_query_match:
                sql_query = sql_query_match.group(1).strip()
            else:
                sql_query = formatted_response
                for marker in ["Question:", "SQLQuery:", "SQLResult:", "Answer:", "```sql", "```"]:
                    sql_query = sql_query.replace(marker, "")
                sql_query = sql_query.strip().rstrip(';')
    
    print(f"Extracted SQL Query: {sql_query}")
    
    result = execute_query(sql_query)
    print(f"SQL Result: {result}")
    
    answer = llm.invoke(
        answer_prompt.format(
            question=question,
            query=sql_query,
            result=result
        )
    )
    
    return answer.content

# Extract SQL query from LLM response
def extract_sql_query(formatted_response):
    sql_query = None
    sql_code_block_match = re.search(r'```sql\s*(.*?)\s*```', formatted_response, re.DOTALL)
    if sql_code_block_match:
        sql_query = sql_code_block_match.group(1).strip()
    else:
        sql_query_match = re.search(r'SQLQuery:\s*"([^"]*)"', formatted_response)
        if sql_query_match:
            sql_query = sql_query_match.group(1).strip()
        else:
            sql_query_match = re.search(r'SQLQuery:\s*(.*?)(?:\n|$)', formatted_response)
            if sql_query_match:
                sql_query = sql_query_match.group(1).strip()
            else:
                sql_query = formatted_response
                for marker in ["Question:", "SQLQuery:", "SQLResult:", "Answer:", "```sql", "```"]:
                    sql_query = sql_query.replace(marker, "")
                sql_query = sql_query.strip().rstrip(';')
    return sql_query

@cl.on_chat_start
async def on_chat_start():
    """Initialize the chat session"""
    # Welcome message
    await cl.Message(
        content="Welcome to the Medicare Data Assistant! Ask me any question about Medicare spending data.",
        author="Medicare Assistant"
    ).send()
    
    # Add example questions as suggested messages
    await cl.Message(
        content="Here are some example questions you can ask:",
        author="Medicare Assistant"
    ).send()
    
    examples = [
        "What is the total Medicare spending for the year 2022?",
        "What is the number of high-spending drugs in 2022?",
        "What is the generic name of the drug which has the highest total sum of spending in all records?",
        "What is the average spending per claim in 2021?"
    ]
    
    for example in examples:
        await cl.Message(
            content=example,
            author="Medicare Assistant",
            actions=[
                cl.Action(name="ask", payload={"question": example}, label="Ask this question")
            ]
        ).send()

@cl.on_message
async def on_message(message: cl.Message):
    """Process incoming messages"""
    # Create a new message for the response
    msg = cl.Message(content="")
    await msg.send()
    
    # Get the raw response from the LLM
    formatted_response = sql_query_chain.invoke({"question": message.content})
    
    # Extract the SQL query
    sql_query = extract_sql_query(formatted_response)
    
    # Execute the query
    result = execute_query(sql_query)
    
    # Get the final answer
    answer = llm.invoke(
        answer_prompt.format(
            question=message.content,
            query=sql_query,
            result=result
        )
    ).content
    
    # Show the final answer
    await msg.stream_token(f"{answer}\n\n")

    await msg.stream_token("Here is the SQL query used to answer your question:\n\n")

    await msg.stream_token(f"```sql\n{sql_query}\n```\n\n")

@cl.action_callback("ask")
async def on_action(action):
    """Handle action button clicks"""
    example_question = action.payload["question"]
    
    # Create a new user message with the example question
    await cl.Message(content=example_question, author="User").send()
    
    # Process the example question
    await on_message(cl.Message(content=example_question)) 

