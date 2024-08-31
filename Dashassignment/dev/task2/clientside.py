import socket
import json
import sys

def sendtoserv(prompt, client_id, server_host='localhost', server_port=8080):
    
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((server_host, server_port))

    request = {
        "Prompt": prompt,
        "ClientID": client_id
    }

    client_socket.send(json.dumps(request).encode('utf-8'))

    response_data = ""
    while True:
        recdata = client_socket.recv(1024).decode('utf-8')
        response_data += recdata
        if '\n' in response_data:
            break

    response_data = response_data.strip().split('\n')[0]
    response = json.loads(response_data)
    
    client_socket.close()
    return response

def main(client_id):
    inputf = f"input{client_id}.txt"
    outputf = f"output{client_id}.json"
    
    with open(inputf, 'r') as file:
        prompts = [line.strip() for line in file.readlines() if line.strip()]
    
    if not prompts:
        return
    
    responses = []
    
    for prompt in prompts:
        response = sendtoserv(prompt, client_id)
        
        if response["Prompt"] != prompt:
            response["Source"] = "user"
        
        responses.append(response)
    
    with open(outputf, 'w') as file:
        json.dump(responses, file, indent=3)

    print(f"saved to {outputf}")

if __name__ == "__main__":

    client_id = sys.argv[1]
    main(client_id)
