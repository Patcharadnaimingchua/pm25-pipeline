"""Fill empty FERNET_KEY / AIRFLOW_JWT_SECRET in a .env file. Safe to run repeatedly."""

import base64
import os
import secrets
import sys
from pathlib import Path


def fernet_key() -> str:
    # A Fernet key is 32 random bytes, url-safe base64 encoded.
    return base64.urlsafe_b64encode(os.urandom(32)).decode()


def main(path: str) -> None:
    env = Path(path)
    lines = env.read_text(encoding="utf-8").splitlines()
    generated = {"FERNET_KEY": fernet_key, "AIRFLOW_JWT_SECRET": lambda: secrets.token_urlsafe(32)}
    out = []
    for line in lines:
        key, sep, value = line.partition("=")
        if sep and key in generated and not value.strip():
            line = f"{key}={generated[key]()}"
            print(f"generated {key}")
        out.append(line)
    env.write_text("\n".join(out) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else ".env")
