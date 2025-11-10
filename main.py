from redis import Redis

def main():
    server = Redis()
    try:
        server.start()
    except KeyboardInterrupt:
        print("\n Shutting down server...")
        server.stop()
    
if __name__ == "__main__":
    main()