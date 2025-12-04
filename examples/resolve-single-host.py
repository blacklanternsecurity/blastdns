import sys
import json
import asyncio
from blastdns import Client, ClientConfig


async def main():
    resolvers = ["1.1.1.1:53"]
    client = Client(resolvers, ClientConfig())

    if len(sys.argv) != 2:
        print("Usage: python resolve-single-host.py <host>")
        sys.exit(1)

    host = sys.argv[1]
    response = await client.resolve(host, "A")
    print(json.dumps(response, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
