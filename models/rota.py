from pydantic import BaseModel

class Rota(BaseModel):
    id: int | None = None
    cidade_origem: str
    cidade_destino: str
    distancia_km: float
    valor_passagem_base: float
    tempo_estimado_minutos: int