import socket
import json
import time
import threading
import google.generativeai as genai

genai.configure(api_key="AIzaSyCPeVktb4WctYynvspUPkdIYnvXRW85MRA")

def clientfunc(client_socket, client_address):
    print(f"Connection from {client_address}.")
    
    while True:
        try:
            data = client_socket.recv(1024).decode('utf-8')
            # if not data:
            #     break

            request = json.loads(data)
            prompt = request.get("Prompt")
            client_id = request.get("ClientID")

            # if not prompt:
            #     print(f"Received empty prompt from {client_address}")
            #     continue
            
            start_time = int(time.time())
            chat = genai.GenerativeModel("gemini-1.5-flash").start_chat()
            response = chat.send_message(prompt)
            end_time = int(time.time())
            
            message = response.text.strip()

            response_object = {
                "Prompt": prompt,
                "Message": message,
                "TimeSent": start_time,
                "TimeRecvd": end_time,
                "Source": "Gemini",
                "ClientID": client_id
            }

            client_socket.send((json.dumps(response_object) + '\n').encode('utf-8'))
        
        except Exception as e:
           
            break

    client_socket.close()
    print(f"Connection from {client_address} closed.")

def startserv(host='localhost', port=8080):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen(5)
    print(f"Server listening on {host}:{port}")
    
    while True:
        client_socket, client_address = server_socket.accept()
        client_handler = threading.Thread(target=clientfunc, args=(client_socket, client_address))
        client_handler.start()

if __name__ == "__main__":
    startserv()
