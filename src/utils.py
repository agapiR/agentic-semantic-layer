import re
import json
from openai import OpenAI
from collections.abc import Iterable

def prompt_llm(user_message, system_message, tokens=2048, model='gpt-4o'):
    api_key = [c['api_key'] for c in json.load(open('OAI_CONFIG_LIST')) if c['model'] == model][0]
    client = OpenAI(api_key=api_key)

    response = client.chat.completions.create(
        model=model,
        messages=[
                    {
                        "role": "system",
                        "content": system_message
                    },
                    {
                        "role": "user",
                        "content": user_message
                    }
                ],
        temperature=0.0,
        max_tokens=2048,
        top_p=1
    )
    response = response.choices[0].message.content

    return response

def extract_json_from_llm_response(response, verbose=True):
    """
    Extract a valid JSON object from a given response string.
    
    Args:
        response (str): The response string from a language model.
    
    Returns:
        dict or None: The parsed JSON object if found, otherwise None.
    """
    # Find the first code block containing JSON
    match = re.search(r"```json(.*?)```", response, re.DOTALL) 
    if match: 
        json_str = match.group(1) 
        try: 
            # Parse the extracted JSON
            json_data = json.loads(json_str)
        except json.JSONDecodeError as e: 
            if verbose:
                print("Error decoding JSON:", e)
            return None
    else: 
        # Try to parse the JSON data from the response string
        try:
            json_data = json.loads(response)
        except json.JSONDecodeError as e:
            if verbose:
                print("Error decoding JSON:", e)
            return None

    return json_data

def text_embedding(text, model="text-embedding-3-small"):
    api_key = [c['api_key'] for c in json.load(open('OAI_CONFIG_LIST')) if c['model'] == model][0]
    client = OpenAI(api_key=api_key)
    return client.embeddings.create(input = [text], model=model).data[0].embedding

def flatten(xs):
    for x in xs:
        if isinstance(x, Iterable) and not isinstance(x, (str, bytes)):
            yield from flatten(x)
        else:
            yield x