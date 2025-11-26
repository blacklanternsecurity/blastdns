#!/usr/bin/env python3 -u
import subprocess
import sys
import tempfile
import os
import json
import select

def main():
    # Build first
    print("Building release binary...", flush=True)
    subprocess.run(["cargo", "build", "--release"])
    print("Build complete", flush=True)
    
    binary = "./target/release/blastdns"
    
    # Create temp resolver file
    resolver_fd, resolver_path = tempfile.mkstemp(suffix=".txt")
    try:
        with os.fdopen(resolver_fd, "w") as f:
            f.write("8.8.8.8\n")
        
        print(f"Starting blastdns with stdin...", flush=True)
        
        # Start process - stderr goes to stderr, stdout to pipe
        proc = subprocess.Popen(
            [binary, "--resolvers", resolver_path],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=sys.stderr,
            text=True,
            bufsize=1
        )
        
        print(f"Process running (PID: {proc.pid}), no stdin provided yet...", flush=True)
        
        # Write first host
        print("Writing 'google.com' to stdin...", flush=True)
        print("google.com", file=proc.stdin, flush=True)
        
        # Read the result
        print("Waiting for result...", flush=True)
        if select.select([proc.stdout], [], [], 5.0)[0]:
            result_line = proc.stdout.readline()
        else:
            print("✗ Timeout waiting for result")
            proc.terminate()
            proc.wait(timeout=2)
            sys.exit(1)
        
        if result_line:
            print("✓ Got result immediately:", flush=True)
            print(result_line.strip(), flush=True)
            
            result = json.loads(result_line)
            if result.get("host") == "google.com":
                print("✓ Result contains expected host")
            else:
                print("✗ Result doesn't contain expected host")
                sys.exit(1)
        else:
            print("✗ No output received")
            sys.exit(1)
        
        # Write second host
        print("\nWriting 'example.com' to stdin...")
        print("example.com", file=proc.stdin, flush=True)
        
        # Read second result
        if select.select([proc.stdout], [], [], 5.0)[0]:
            result_line = proc.stdout.readline()
        else:
            print("✗ Timeout waiting for second result")
            proc.terminate()
            proc.wait(timeout=2)
            sys.exit(1)
        
        if result_line:
            print("✓ Got second result:")
            print(result_line.strip())
        else:
            print("✗ Didn't get second result")
            sys.exit(1)
        
        # Clean up
        proc.stdin.close()
        proc.terminate()
        proc.wait(timeout=2)
        
        print("\n✓ All tests passed!")
    finally:
        os.unlink(resolver_path)

if __name__ == "__main__":
    main()
