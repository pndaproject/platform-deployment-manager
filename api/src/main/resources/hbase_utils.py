def decode(data):
    if isinstance(data, bytes):  return data.decode('utf-8')
    if isinstance(data, dict):   return dict(map(decode, data.items()))
    if isinstance(data, tuple):  return map(decode, data)
    if isinstance(data, list):  return list(map(decode, data))
    return data

def encode(data):
    if isinstance(data, str):  return data.encode('utf-8')
    if isinstance(data, dict):   return dict(map(encode, data.items()))
    if isinstance(data, tuple):  return map(encode, data)
    if isinstance(data, list):  return list(map(encode, data))
    return data
