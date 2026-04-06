import os
from deltalake import DeltaTable, write_deltalake
import pyarrow as pa
import pandas as pd

class DeltaRepository:
    def __init__(self, entity_name: str):
        # Garante que a pasta 'data' principal exista
        os.makedirs("data", exist_ok=True)
        
        # Agora salva tudo DENTRO da pasta data/
        self.table_path = f"data/{entity_name}"
        self.seq_path = f"data/{entity_name}.seq"
        self._initialize_sequence()

    def _initialize_sequence(self):
        if not os.path.exists(self.seq_path):
            with open(self.seq_path, "w") as f:
                f.write("0")

    def _get_next_id(self) -> int:
        with open(self.seq_path, "r+") as f:
            current_id = int(f.read())
            next_id = current_id + 1
            f.seek(0)
            f.write(str(next_id))
            f.truncate()
            return next_id

    def insert(self, data: dict):
        data["id"] = self._get_next_id()
        df = pd.DataFrame([data])
        write_deltalake(self.table_path, df, mode="append")
        return data

    def list_paginated(self, page: int, size: int):
        """F2: Paginação real via streaming (funciona para todas as entidades)."""
        try:
            dt = DeltaTable(self.table_path)
            # O scanner lê os arquivos físicos de forma eficiente
            scanner = dt.to_pyarrow_dataset().scanner()
            
            offset = (page - 1) * size
            count = 0
            results = []
            
            for batch in scanner.to_batches():
                for row in batch.to_pylist():
                    if count < offset:
                        count += 1
                        continue
                    
                    results.append(row)
                    if len(results) == size:
                        return results
            return results
        except Exception:
            return []

    def get_by_id(self, record_id: int):
        try:
            dt = DeltaTable(self.table_path)
            # Lemos o arquivo em pequenos blocos (streaming) sem estourar a RAM
            scanner = dt.to_pyarrow_dataset().scanner()
            for batch in scanner.to_batches():
                for row in batch.to_pylist():
                    if row["id"] == record_id:
                        return row
            return None
        except Exception:
            return None

    def update(self, record_id: int, updates: dict):
        try:
            # 1. Verifica se o registro realmente existe antes de tentar atualizar
            if not self.get_by_id(record_id):
                return None
                
            dt = DeltaTable(self.table_path)
            formatted_updates = {}
            
            # 2. Formata os dados no padrão exato que o SQL do DeltaLake exige
            for k, v in updates.items():
                if isinstance(v, str):
                    formatted_updates[k] = f"'{v}'"
                elif isinstance(v, bool):
                    formatted_updates[k] = "true" if v else "false"
                elif v is None:
                    formatted_updates[k] = "null"
                else:
                    formatted_updates[k] = str(v)
                    
            dt.update(predicate=f"id = {record_id}", updates=formatted_updates)
            return self.get_by_id(record_id)
        except Exception as e:
            print(f"Erro interno no update: {e}") # Ajuda a debugar se der erro no terminal
            return None

    def delete(self, record_id: int):
        try:
            dt = DeltaTable(self.table_path)
            dt.delete(predicate=f"id = {record_id}")
            return True
        except Exception:
            return False

    def count(self) -> int:
        try:
            dt = DeltaTable(self.table_path)
            return dt.to_pyarrow_dataset().count_rows()
        except Exception:
            return 0

    def vacuum(self):
        try:
            dt = DeltaTable(self.table_path)
            dt.vacuum(retention_hours=0, enforce_retention_duration=False)
        except Exception:
            pass