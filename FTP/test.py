import paramiko
import os

# Define the SFTP server details
hostname = "103.25.173.57"
port = 142 # Replace with your actual port number
username = "rabyte"
password = "F32@Sector11"  # Or use a private key for authentication

local = 'POS.txt'
remote= "/api.rabyte.com/AKS"

if not os.path.exists(local):
    print(f"Error: Local file '{local}' does not exist.")
else:
    try:
        # Set up the paramiko SSH client
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        # Connect to the SFTP server
        client.connect(hostname, port=port, username=username, password=password)

        # Open an SFTP session
        sftp = client.open_sftp()

        # Ensure the remote directory exists
        try:
            sftp.listdir(remote)
        except IOError:
            print(
                f"Remote directory '{remote}' does not exist or cannot be accessed."
            )
            sftp.close()
            client.close()
            exit()

        # Upload the file
        sftp.put(local ,remote)
        print(
            f"File '{local}' uploaded to '{remote}' successfully."
        )

        # Close the SFTP session and SSH client
        sftp.close()
        client.close()

    except paramiko.AuthenticationException:
        print("Authentication failed. Please check your credentials.")
    except IOError as e:
        print(f"IOError occurred: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")
