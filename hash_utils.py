import hashlib

def calcular_hash_util(valor: str, algoritmo: str) -> str | None:
    algos = {
        "MD5": hashlib.md5,
        "SHA-1": hashlib.sha1,
        "SHA-256": hashlib.sha256
    }
    algo_upper = algoritmo.upper()
    if algo_upper not in algos:
        return None
    
    h = algos[algo_upper](valor.encode("utf-8"))
    return h.hexdigest()