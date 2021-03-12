import hashlib
import os.path
from typing import List

# Hash in 16k blocks.
BLOCK_SIZE = 16 * 1024


def output_key_for(output_key_base: str, path: str, extras: List[str] = []) -> str:
    # Compute the hash of the file.
    sha = hashlib.sha1()
    with open(path, "rb") as fp:
        while True:
            data = fp.read(BLOCK_SIZE)
            if not data:
                break
            sha.update(data)

    # Compute the final key.
    if extras:
        extras_str = "_" + "+".join(extras)
    else:
        extras_str = ""
    root, ext = os.path.splitext(output_key_base)
    return f"{root}{extras_str}_{sha.hexdigest()}{ext}"
