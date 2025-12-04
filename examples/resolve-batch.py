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

    hosts_file = sys.argv[1]

    def host_gen():
        with open(hosts_file, "r") as f:
            for line in f:
                yield line.strip()

    async for host, response in client.resolve_batch(host_gen(), "A"):
        print(json.dumps(response))


if __name__ == "__main__":
    asyncio.run(main())
