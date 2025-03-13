import pandas as pd

# Carregar os arquivos (substituir pelos reais quando necessário)
df_escalas_slots = pd.read_csv("escalas_slots.csv")
df_agendamentos = pd.read_csv("agendamentos.csv")
df_unidades = pd.read_csv("unidades.csv")

# Converter colunas de data e hora para datetime
df_agendamentos["data_hora_agendamento"] = pd.to_datetime(df_agendamentos["data_hora_agendamento"], errors='coerce')
df_escalas_slots["horarioSlot"] = pd.to_datetime(df_escalas_slots["horarioSlot"], errors='coerce')

# Vincular agendamentos com escalas
df_merged = df_agendamentos.merge(df_escalas_slots, left_on="IdEscala", right_on="EscalaId", how="left")

# Criar a coluna 'slot_ocupado' verificando se o horário do agendamento bate com um slot disponível
df_merged["slot_ocupado"] = df_merged["data_hora_agendamento"] == df_merged["horarioSlot"]

# Criar coluna de sugestão de horário dentro da margem de 1 hora
def encontrar_slot_proximo(row, df_escalas):
    filtro = (df_escalas["Unidade"] == row["Unidade"]) &              (df_escalas["Especialidade"] == row["especialidade"]) &              ((df_escalas["horarioSlot"] - row["data_hora_agendamento"]).abs() <= pd.Timedelta(minutes=60))
    
    slot_mais_proximo = df_escalas.loc[filtro].sort_values("horarioSlot").head(1)
    
    if not slot_mais_proximo.empty:
        return slot_mais_proximo.iloc[0]["horarioSlot"]
    return None

df_merged["horarioSlot_sugestao"] = df_merged.apply(lambda row: encontrar_slot_proximo(row, df_escalas_slots), axis=1)
df_merged["slot_ajustado"] = df_merged["horarioSlot_sugestao"].notnull()

# Analisar ocupação total dos slots
total_slots = len(df_escalas_slots)
slots_ocupados = df_merged["slot_ocupado"].sum()
taxa_ocupacao = (slots_ocupados / total_slots) * 100

# Identificar demanda não alocada
demanda_nao_alocada = len(df_merged[df_merged["slot_ocupado"] == False])
taxa_demanda_nao_alocada = (demanda_nao_alocada / len(df_merged)) * 100

# Gerar relatório consolidado
relatorio = f"""
### Relatório de Ocupação e Demandas Não Alocadas ###

Taxa de ocupação total dos slots: {taxa_ocupacao:.2f}%
Taxa de demanda não alocada: {taxa_demanda_nao_alocada:.2f}%

### Detalhamento por Especialidade e Unidade ###

"""

ocupacao_especialidade = df_merged.groupby("especialidade")["slot_ocupado"].mean() * 100
ocupacao_unidade = df_merged.groupby("Unidade")["slot_ocupado"].mean() * 100

for especialidade, taxa in ocupacao_especialidade.items():
    relatorio += f"Especialidade {especialidade}: {taxa:.2f}% de ocupação\n"

relatorio += "\n"

for unidade, taxa in ocupacao_unidade.items():
    relatorio += f"Unidade {unidade}: {taxa:.2f}% de ocupação\n"

# Salvar outputs
df_merged.to_csv("output_agendamentos_vinculados.csv", index=False)
with open("relatorio_ocupacao.txt", "w") as file:
    file.write(relatorio)

print("Processamento concluído! Outputs gerados.")
