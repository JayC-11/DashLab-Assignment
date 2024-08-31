import google.generativeai as genai
import json
import time


genai.configure(api_key="AIzaSyCPeVktb4WctYynvspUPkdIYnvXRW85MRA")

def promptfunction(prompt):
    start_time = int(time.time())

    chat = genai.GenerativeModel("gemini-1.5-flash").start_chat(
        history=[
            {"role": "user", "parts": "Hello"},
            {"role": "model", "parts": "Great to meet you. What would you like to know?"}
        ]
    )

    response = chat.send_message(prompt)
    end_time = int(time.time())

    message = response.text.strip()

    return {
        "Prompt": prompt,
        "Message": message,
        "TimeSent": start_time,
        "TimeRecvd": end_time,
        "Source": "Gemini"
    }

with open("input.txt","r") as file:
    prompts = [line.strip() for line in file.readlines()]

responses = []
for prompt in prompts:
    response = promptfunction(prompt)
    responses.append(response)

with open("output.json","w") as file:
    json.dump(responses,file,  indent=3)