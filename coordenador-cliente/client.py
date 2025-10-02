import socket

def main():
    host = '127.0.0.1'
    port = 8000

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((host, port))

    msg = "hello from client"
    while True:
        s.send(msg.encode('ascii'))
        data = s.recv(1024)
        print('Received from server:', data.decode('ascii'))

        ans = input('Do you want to continue (y/n): ')
        if ans.lower() != 'y':
            break

    s.close()

if __name__ == '__main__':
    main()