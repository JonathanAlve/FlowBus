import os
import tempfile
import zipfile
from deltalake import DeltaTable

def gerar_arquivo_zip(dt: DeltaTable, entidade: str) -> str:
    # Processamento em disco para evitar estouro de memória
    tmp_csv = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    tmp_zip = tempfile.NamedTemporaryFile(delete=False, suffix=".zip")
    
    scanner = dt.to_pyarrow_dataset().scanner()
    primeiro_lote = True
    for batch in scanner.to_batches():
        df = batch.to_pandas()
        tmp_csv.write(df.to_csv(index=False, header=primeiro_lote).encode('utf-8'))
        primeiro_lote = False
    tmp_csv.close()
    
    with zipfile.ZipFile(tmp_zip.name, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.write(tmp_csv.name, f"{entidade}.csv")
    tmp_zip.close()
    
    os.unlink(tmp_csv.name)
    return tmp_zip.name