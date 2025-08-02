#!/usr/bin/env python3
import uvicorn

if __name__ == "__main__":
    # Binding to 0.0.0.0 makes the server accessible from any network interface
    # This allows other machines on the network to connect to your API
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
