import base64
import hashlib
import json
import urllib.parse
import pyaes


__REQUEST_TO_STRING = {}
__STRING_TO_REQUEST = {}


def __encode_token(secret_key: str, **arguments):
    aes = pyaes.AESModeOfOperationCTR(hashlib.sha256(secret_key.encode()).digest())
    raw_encoded = aes.encrypt(json.dumps(arguments).encode())
    return urllib.parse.quote(base64.b64encode(raw_encoded).decode())


def __decode_token(secret_key: str, encoded):
    # noinspection PyBroadException
    try:
        raw_encoded = base64.b64decode(urllib.parse.unquote(encoded).encode())
        aes = pyaes.AESModeOfOperationCTR(hashlib.sha256(secret_key.encode()).digest())
        return json.loads(aes.decrypt(raw_encoded).decode())
    except Exception:
        raise ValueError("Failed to decode arguments")


def register_token_handler(func_name):
    def decorator(func):
        __REQUEST_TO_STRING[func] = func_name
        __STRING_TO_REQUEST[func_name] = func
        return func
    return decorator


def create_request_token(secret_key: str, request_function,  **request_args):
    return __encode_token(secret_key, tr=__REQUEST_TO_STRING[request_function], ra=request_args, v=2)


def parse_request_token(secret_key: str, token):
    token_data = __decode_token(secret_key, token)
    if 'v' in token_data:
        version = token_data['v']
        if version == 2:
            target_request = token_data['tr']
            request_args = token_data['ra']
            return __STRING_TO_REQUEST[target_request], request_args
        else:
            ValueError("Unknown token version")

    else:  # Legacy version only for "requests/get" endpoint
        return __STRING_TO_REQUEST['readings/get'], token_data
