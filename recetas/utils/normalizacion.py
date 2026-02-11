from unidecode import unidecode

def normalizar_nombre(texto: str) -> str:
    if not texto:
        return ""
    texto = unidecode(str(texto))
    texto = texto.lower().strip()
    texto = " ".join(texto.split())
    return texto
