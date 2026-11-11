"""Lossless response-body fields shared by raw collectors.

Legacy envelopes omit ``body_encoding`` and hold the original UTF-8 text.
When decoding fails, ``body_encoding="base64"`` marks a standard base64 body;
readers recover its exact bytes with ``base64.b64decode(body, validate=True)``.
"""

import base64


def body_fields(body: bytes | str) -> dict[str, str]:
    """Keep text envelopes unchanged; represent undecodable bytes losslessly."""
    if isinstance(body, str):
        return {"body": body}
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError:
        return {"body": base64.b64encode(body).decode("ascii"), "body_encoding": "base64"}
    return {"body": text}
