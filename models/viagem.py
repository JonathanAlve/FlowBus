from pydantic import BaseModel

class Viagem(BaseModel):
    id: int | None = None
    id_rota: int
    id_onibus: int
    data_hora_partida: str
    motorista_nome: str
    status: str = "Agendada"