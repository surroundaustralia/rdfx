from rdfx_cli import clean_ttl as clean_ttl
from pathlib import Path

if __name__ == "__main__":
    inputPath = Path('./test.ttl')
    clean_ttl(inputPath)