from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import StreamingResponse, FileResponse
from deltalake import DeltaTable

from database import DeltaRepository
from models.onibus import Onibus
from models.rota import Rota
from models.viagem import Viagem

# Importações dos seus módulos utilitários (que agora estão na raiz)
from hash_utils import calcular_hash_util
from csv_utils import gerar_streaming_csv
from zip_utils import gerar_arquivo_zip

app = FastAPI(title="BusFlow API")

# 1. Instanciando os repositórios
repo_onibus = DeltaRepository("onibus")
repo_rota = DeltaRepository("rota")
repo_viagem = DeltaRepository("viagem")

# ==========================================
# ENDPOINTS: ÔNIBUS
# ==========================================

@app.post("/onibus/", status_code=201)
def criar_onibus(onibus: Onibus):
    return repo_onibus.insert(onibus.model_dump(exclude={"id"}))

@app.get("/onibus/")
def listar_onibus(pagina: int = Query(1, ge=1), tamanho_pagina: int = Query(10, ge=1)):
    return repo_onibus.list_paginated(page=pagina, size=tamanho_pagina)

@app.get("/onibus/count")
def contar_onibus():
    return {"total": repo_onibus.count()}

@app.get("/onibus/{id_onibus}")
def buscar_onibus(id_onibus: int):
    registro = repo_onibus.get_by_id(id_onibus)
    if not registro:
        raise HTTPException(status_code=404, detail="Ônibus não encontrado")
    return registro

@app.put("/onibus/{id_onibus}")
def atualizar_onibus(id_onibus: int, onibus: Onibus):
    registro_atualizado = repo_onibus.update(id_onibus, onibus.model_dump(exclude={"id"}))
    if not registro_atualizado:
        raise HTTPException(status_code=404, detail="Ônibus não encontrado para atualização")
    return registro_atualizado

@app.delete("/onibus/{id_onibus}", status_code=204)
def deletar_onibus(id_onibus: int):
    sucesso = repo_onibus.delete(id_onibus)
    if not sucesso:
        raise HTTPException(status_code=404, detail="Ônibus não encontrado")
    return None

# ==========================================
# ENDPOINTS: ROTA
# ==========================================

@app.post("/rota/", status_code=201)
def criar_rota(rota: Rota):
    return repo_rota.insert(rota.model_dump(exclude={"id"}))

@app.get("/rota/")
def listar_rota(pagina: int = Query(1, ge=1), tamanho_pagina: int = Query(10, ge=1)):
    return repo_rota.list_paginated(page=pagina, size=tamanho_pagina)

@app.get("/rota/count")
def contar_rota():
    return {"total": repo_rota.count()}

@app.get("/rota/{id_rota}")
def buscar_rota(id_rota: int):
    registro = repo_rota.get_by_id(id_rota)
    if not registro:
        raise HTTPException(status_code=404, detail="Rota não encontrada")
    return registro

@app.put("/rota/{id_rota}")
def atualizar_rota(id_rota: int, rota: Rota):
    registro_atualizado = repo_rota.update(id_rota, rota.model_dump(exclude={"id"}))
    if not registro_atualizado:
        raise HTTPException(status_code=404, detail="Rota não encontrada para atualização")
    return registro_atualizado

@app.delete("/rota/{id_rota}", status_code=204)
def deletar_rota(id_rota: int):
    sucesso = repo_rota.delete(id_rota)
    if not sucesso:
        raise HTTPException(status_code=404, detail="Rota não encontrada")
    return None

# ==========================================
# ENDPOINTS: VIAGEM
# ==========================================

@app.post("/viagem/", status_code=201)
def criar_viagem(viagem: Viagem):
    return repo_viagem.insert(viagem.model_dump(exclude={"id"}))

@app.get("/viagem/")
def listar_viagem(pagina: int = Query(1, ge=1), tamanho_pagina: int = Query(10, ge=1)):
    return repo_viagem.list_paginated(page=pagina, size=tamanho_pagina)

@app.get("/viagem/count")
def contar_viagem():
    return {"total": repo_viagem.count()}

@app.get("/viagem/{id_viagem}")
def buscar_viagem(id_viagem: int):
    registro = repo_viagem.get_by_id(id_viagem)
    if not registro:
        raise HTTPException(status_code=404, detail="Viagem não encontrada")
    return registro

@app.put("/viagem/{id_viagem}")
def atualizar_viagem(id_viagem: int, viagem: Viagem):
    registro_atualizado = repo_viagem.update(id_viagem, viagem.model_dump(exclude={"id"}))
    if not registro_atualizado:
        raise HTTPException(status_code=404, detail="Viagem não encontrada para atualização")
    return registro_atualizado

@app.delete("/viagem/{id_viagem}", status_code=204)
def deletar_viagem(id_viagem: int):
    sucesso = repo_viagem.delete(id_viagem)
    if not sucesso:
        raise HTTPException(status_code=404, detail="Viagem não encontrada")
    return None

# ==========================================
# UTILITÁRIOS E EXPORTAÇÃO (F5, F6, F7)
# ==========================================

@app.get("/export/{entidade}/csv")
def exportar_csv(entidade: str):
    if entidade not in ["onibus", "rota", "viagem"]:
        raise HTTPException(status_code=404, detail="Entidade inválida")
    
    try:
        dt = DeltaTable(f"data/{entidade}")
        return StreamingResponse(
            gerar_streaming_csv(dt), 
            media_type="text/csv", 
            headers={"Content-Disposition": f"attachment; filename={entidade}.csv"}
        )
    except Exception:
        raise HTTPException(status_code=404, detail="Dados não encontrados. Rode o seed.")

@app.get("/export/{entidade}/zip")
def exportar_zip(entidade: str):
    if entidade not in ["onibus", "rota", "viagem"]:
        raise HTTPException(status_code=404, detail="Entidade inválida")
    
    try:
        dt = DeltaTable(f"data/{entidade}")
        path_zip = gerar_arquivo_zip(dt, entidade)
        return FileResponse(
            path_zip, 
            media_type="application/x-zip-compressed", 
            filename=f"{entidade}.zip"
        )
    except Exception:
        raise HTTPException(status_code=404, detail="Erro ao gerar arquivo ZIP.")

@app.get("/utils/hash")
def calcular_hash(valor: str, algoritmo: str):
    resultado = calcular_hash_util(valor, algoritmo)
    if not resultado:
        raise HTTPException(status_code=400, detail="Algoritmo não suportado. Use MD5, SHA-1 ou SHA-256")
    
    return {"valor": valor, "algoritmo": algoritmo.upper(), "hash": resultado}