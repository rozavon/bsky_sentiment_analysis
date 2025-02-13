from atproto import Client

def setup_bluesky_client(handle, password):
    client = Client()
    try:
        client.login(handle, password)
        return client
    except Exception as e:
        print(f"Authentication failed: {e}")
        return None

