import asyncio
import json
import websockets
import os
import sys

FASTAPI_WS_URL = os.environ.get("FASTAPI_WS_URL", "ws://fastapi:8000/ws")
TCP_PORT = int(os.environ.get("BRIDGE_TCP_PORT", "9999"))

clients = set()

async def handle_client(reader, writer):
    addr = writer.get_extra_info('peername')
    print(f"New connection from {addr}")
    clients.add(writer)
    try:
        # Keep connection open until client disconnects
        await reader.read() 
    except Exception as e:
        print(f"Connection error {addr}: {e}")
    finally:
        print(f"Connection closed {addr}")
        clients.discard(writer)
        try:
            writer.close()
            await writer.wait_closed()
        except:
            pass

async def broadcast_loop():
    while True:
        try:
            print(f"Connecting to WebSocket {FASTAPI_WS_URL}...")
            async with websockets.connect(FASTAPI_WS_URL) as ws:
                print("Connected to WebSocket.")
                async for msg in ws:
                    if not clients:
                        continue
                    
                    data = msg.encode("utf-8") + b"\n"
                    # Create a list copy to iterate safely
                    for writer in list(clients):
                        try:
                            writer.write(data)
                            await writer.drain()
                        except Exception as e:
                            print(f"Error writing to client: {e}")
                            clients.discard(writer)
        except Exception as e:
            print(f"WebSocket error: {e}. Retrying in 5 seconds...")
            await asyncio.sleep(5)

async def main():
    server = await asyncio.start_server(
        handle_client, '0.0.0.0', TCP_PORT)

    addrs = ', '.join(str(sock.getsockname()) for sock in server.sockets)
    print(f"Bridge listening on {addrs}")

    # Run broadcast loop and server concurrently
    await asyncio.gather(
        server.serve_forever(),
        broadcast_loop()
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
