from pydantic import BaseModel, Field

class Onibus(BaseModel):
    id: int | None = None
    placa: str = Field(..., min_length=7, max_length=8)
    modelo: str
    capacidade: int = Field(default=30)
    ano_fabricacao: int
    em_manutencao: bool = False