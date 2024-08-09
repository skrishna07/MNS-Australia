import json
import os
import requests
import logging
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())


def split_openai(text, initial_prompt):
    url = os.environ.get('url')
    prompt = str(text) + ' ' + '\n' + '--------------------------------' + '\n' + initial_prompt
    logging.info(prompt)
    payload = json.dumps({
        "model": "gpt-4o",
        "messages": [
            {
                "role": "user",
                "content": prompt
            }
        ],
        "temperature": 0.7,
        "max_tokens": 4096,
        "top_p": 1,
        "frequency_penalty": 0,
        "presence_penalty": 0
    })
    headers = {
        'Authorization': os.environ.get('OPENAI_API_KEY_Vietnam'),
        'Content-Type': 'application/json',
        'Cookie': os.environ.get('cookie')
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    json_response = response.json()
    print(json_response)
    content = json_response['choices'][0]['message']['content']
    print(content)
    return content
