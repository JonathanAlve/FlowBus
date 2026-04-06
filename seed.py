from database import DeltaRepository
from faker import Faker
import random
from datetime import datetime, timedelta

fake = Faker('pt_BR')

repo_onibus = DeltaRepository("onibus")
repo_rota = DeltaRepository("rota")
repo_viagem = DeltaRepository("viagem")

def popular_banco():
    print("Iniciando carga de dados...")

    # 1. Gerar 300 Ônibus
    print("Gerando Ônibus...")
    for _ in range(300):
        repo_onibus.insert({
            "placa": fake.license_plate().replace("-", ""),
            "modelo": random.choice(["Mercedes-Benz O500", "Volvo B340M", "Scania K310", "Marcopolo Paradiso"]),
            "capacidade": random.choice([30, 42, 50]),
            "ano_fabricacao": random.randint(2012, 2024),
            "em_manutencao": random.choice([True, False, False, False])
        })

    # 2. Gerar 200 Rotas
    print("Gerando Rotas...")
    for _ in range(200):
        repo_rota.insert({
            "cidade_origem": fake.city(),
            "cidade_destino": fake.city(),
            "distancia_km": round(random.uniform(30.0, 800.0), 2),
            "valor_passagem_base": round(random.uniform(25.0, 300.0), 2),
            "tempo_estimado_minutos": random.randint(40, 720)
        })

    # 3. Gerar 600 Viagens (Totalizando 1100 registros no banco)
    print("Gerando Viagens...")
    for _ in range(600):
        data_base = datetime.now() + timedelta(days=random.randint(1, 60))
        repo_viagem.insert({
            "id_rota": random.randint(1, 200),
            "id_onibus": random.randint(1, 300),
            "data_hora_partida": data_base.isoformat(),
            "motorista_nome": fake.name(),
            "status": random.choice(["Agendada", "Concluída", "Em Curso"])
        })

    print("Carga finalizada com sucesso! Mais de 1.000 registros criados.")

if __name__ == "__main__":
    popular_banco()