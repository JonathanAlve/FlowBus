from deltalake import DeltaTable

def gerar_streaming_csv(dt: DeltaTable):
    # Streaming: lê o disco em batches para proteger a memória RAM
    scanner = dt.to_pyarrow_dataset().scanner()
    primeiro_lote = True
    for batch in scanner.to_batches():
        df = batch.to_pandas()
        yield df.to_csv(index=False, header=primeiro_lote)
        primeiro_lote = False