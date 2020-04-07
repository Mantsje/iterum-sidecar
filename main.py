import time
import sys
from socket import socket, AF_UNIX, SHUT_RDWR
import struct
import os
import json

def encode_fragment_desc_size(size: int) -> bytes:
    return struct.pack("<I", size)

def decode_fragment_desc_size(size_bytes: bytes) -> int:
    return struct.unpack("<I", size_bytes)[0]

def encode_bytes(content: bytes) -> bytes:
    size = len(content)
    return encode_fragment_desc_size(size) + content

def decode_fragment_desc(target: socket) -> dict:
    enc_fragment_size = target.recv(ENC_FRAGMENT_SIZE_LENGTH)
    fragment_size = decode_fragment_desc_size(enc_fragment_size)
    enc_fragment = target.recv(fragment_size)
    return json.loads(enc_fragment)

def encode_fragment_desc(content: str) -> bytes:
    return encode_bytes(content.encode("utf-8"))


# Single input of cat images
# input_name denotes the address of the input. Set via config file, and gathered via getenv()
def cat_images(input_name:str) -> str:
    while True:
        try:
            sidecar_cat_input = socket(AF_UNIX) 
            sidecar_cat_input.connect(input_name)
            while True:
                # Decode messages sennd over this socket one-by-one
                fragment = decode_fragment_desc(sidecar_cat_input)
                yield fragment
        except Exception as err:
            print(f"Exception: {err}, retrying connection in 5 seconds...", flush=True)
            time.sleep(5)
        finally: 
            # Clean up before trying again. Letting the other side know we quit this connection
            sidecar_cat_input.shutdown(SHUT_RDWR)
            sidecar_cat_input.close()


if __name__ == "__main__":
    ENC_FRAGMENT_SIZE_LENGTH = 4
    INPUT = os.getenv("PWD") + "/build/go.sock"

    for image_file in cat_images(INPUT):
        print(image_file, flush=True)