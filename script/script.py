# Importações
import sqlalchemy
import pandas as pd
import numpy as np

# Criação da engine do sql alchemy para a tabela
db_connection = sqlalchemy.create_engine(
    'postgresql+pg8000://postgres:123456@localhost:5433/folhadb',
    client_encoding='utf8',
)

# 1. Extract

# Extração da tabela cargos para dataframe do pandas
cargos_df = pd.read_sql('SELECT * FROM folha.cargos', db_connection)

# Extração da tabela carreiras para dataframe do pandas
carreiras_df = pd.read_sql('SELECT * FROM folha.carreiras', db_connection)

# Extração da tabela unidades para dataframe do pandas
unidades_df = pd.read_sql('SELECT * FROM folha.unidades', db_connection)

# Extração da tabela setores para dataframe do pandas
setores_df = pd.read_sql('SELECT * FROM folha.setores', db_connection)

# Extração da tabela evolucoes_funcionais para dataframe do pandas
evolucoes_funcionais_df = pd.read_sql(
    'SELECT * FROM folha.evolucoes_funcionais', db_connection
)

# Extração da tabela colaboradores para dataframe do pandas
colaboradores_df = pd.read_sql(
    'SELECT * FROM folha.colaboradores', db_connection
)

# Extração da tabela lancamentos para dataframe do pandas
lancamentos_df = pd.read_sql('SELECT * FROM folha.lancamentos', db_connection)

# Extração da tabela folhas_pagamentos para dataframe do pandas
folhas_pagamentos_df = pd.read_sql(
    'SELECT * FROM folha.folhas_pagamentos', db_connection
)

# Extração da tabela rubricas para dataframe do pandas
rubricas_df = pd.read_sql('SELECT * FROM folha.rubricas', db_connection)

# Extração da tabela grupos_rubricas para dataframe do pandas
grupos_rubricas_df = pd.read_sql(
    'SELECT * FROM folha.grupos_rubricas', db_connection
)


# 2. Transform

## dm_cargos
# Merge de cargos e carreiras
dm_cargos_df = pd.merge(
    left=cargos_df, right=carreiras_df, how='left', on='cod_carreira'
)

# Remoção da linha cod_carreira
dm_cargos_df.drop(columns=['cod_carreira'], inplace=True)

## dm_setores
# Merge de setores e unidades
dm_setores_df = pd.merge(
    left=setores_df, right=unidades_df, how='left', on='cod_und'
)

# Renomeando colunas
dm_setores_df.rename(
    columns={
        'dsc_und': 'dsc_unidade',
        'cid_und': 'cidade_unidade',
        'uf_und': 'uf_unidade',
    },
    inplace=True,
)

# Remoção das linhas cod_und e cod_colab_chefe
dm_setores_df.drop(columns=['cod_und', 'cod_colab_chefe'], inplace=True)

## dm_rubricas
# Merge de rubricas e grupos_rubricas
dm_rubricas_df = pd.merge(
    left=rubricas_df, right=grupos_rubricas_df, how='left', on='cod_grupo'
)

# Remoção da linha cod_grupo
dm_rubricas_df.drop(columns=['cod_grupo'], inplace=True)

## dm_faixas_etarias
# Criação do dataframe dm_faixas_etárias
dm_faixas_etarias_df = pd.DataFrame(
    {
        'cod_faixa': [1, 2, 3, 4],
        'dsc_faixa': [
            'até 21 anos',
            'de 21 a 30 anos',
            'de 31 a 45 anos',
            'acima de 45 anos',
        ],
        'idade_inicial': [0, 22, 31, 45],
        'idade_final': [21, 30, 45, 100],
    }
)

## dm_tempos_servicos
# Criação da do dataframe dm_tempo_servicos
dm_tempos_servicos_df = pd.DataFrame(
    {
        'cod_tempo_serv': [1, 2, 3, 4, 5],
        'dsc_tempo_serv': [
            'até 1 ano',
            'de 1 a 10 anos',
            'de 11 a 20 anos',
            'de 21 a 30 anos',
            'acima de 31 anos',
        ],
        'ano_inicial': [0, 1, 11, 21, 31],
        'ano_final': [0, 10, 20, 30, 100],
    }
)

## dm_tempos_folhas
# Criação da do dataframe dm_tempos_folhas a partir de folhas_pagamentos, removendo as linha tpo_folha e dsc_folha
dm_tempos_folhas_df = folhas_pagamentos_df.drop(
    columns=['tpo_folha', 'dsc_folha'], inplace=False
)

# Criação da coluna id_ano_mes a partir de ano e mês
dm_tempos_folhas_df['id_ano_mes'] = (
    dm_tempos_folhas_df['ano'].astype(str)
    + dm_tempos_folhas_df['mes'].astype(str)
).astype(int)

## ft_lancamentos
# Merge das tabelas lancamentos, folhas_pagamento, colaboradores e evoluções funcionais
ft_lancamentos_df = pd.merge(
    left=lancamentos_df,
    right=folhas_pagamentos_df,
    how='left',
    on=['ano', 'mes', 'tpo_folha'],
)
ft_lancamentos_df = pd.merge(
    left=ft_lancamentos_df, right=colaboradores_df, how='left', on='cod_colab'
)
ft_lancamentos_df = pd.merge(
    left=ft_lancamentos_df,
    right=evolucoes_funcionais_df,
    how='right',
    on='cod_colab',
)

# Criação da coluna id_ano_mes a partir de ano e mês
ft_lancamentos_df['id_ano_mes'] = (
    ft_lancamentos_df['ano'].astype(str) + ft_lancamentos_df['mes'].astype(str)
).astype(int)

# Coluna cod_faixa
# Criação da coluna idade_colab a partir da data de nascimento do colaborador e data do lançamento
ft_lancamentos_df['idade_colab'] = (
    pd.to_datetime(ft_lancamentos_df['dat_lanc'])
    - pd.to_datetime(ft_lancamentos_df['dat_nasc'])
) // np.timedelta64(1, 'Y')

# Função para obter o cod_faixa a partir da idade_colab
def get_cod_faixa(idade_colab):
    list_cod_faixa = []
    for idade in idade_colab:
        if idade < 21:
            list_cod_faixa.append(1)
        elif idade <= 30:
            list_cod_faixa.append(2)
        elif idade <= 45:
            list_cod_faixa.append(3)
        else:
            list_cod_faixa.append(4)
    return list_cod_faixa


ft_lancamentos_df['cod_faixa'] = get_cod_faixa(
    ft_lancamentos_df['idade_colab']
)

# Coluna cod_tempo_serv
# Criação da coluna tempo_serv a partir da data de admissão do colaborador e data do lançamento
ft_lancamentos_df['tempo_serv'] = (
    pd.to_datetime(ft_lancamentos_df['dat_lanc'])
    - pd.to_datetime(ft_lancamentos_df['dat_admissao'])
) // np.timedelta64(1, 'Y')

# Função para obter o cod_tempo_serv a partir do tempo_serv
def get_cod_tempo_serv(tempo_serv):
    list_tempo_serv = []
    for idade in tempo_serv:
        if idade == 0:
            list_tempo_serv.append(1)
        elif idade <= 10:
            list_tempo_serv.append(2)
        elif idade <= 20:
            list_tempo_serv.append(3)
        elif idade <= 30:
            list_tempo_serv.append(4)
        else:
            list_tempo_serv.append(5)
    return list_tempo_serv


ft_lancamentos_df['cod_tempo_serv'] = get_cod_tempo_serv(
    ft_lancamentos_df['tempo_serv']
)

# Remoção de colunas desnecessárias
ft_lancamentos_df.drop(
    columns=[
        'dat_lanc',
        'ano',
        'mes',
        'tpo_folha',
        'dsc_folha',
        'nom_colab',
        'dat_nasc',
        'dat_admissao',
        'cod_colab',
        'dat_ini',
        'tempo_serv',
    ],
    inplace=True,
)

# Renomeando val_lanc para total_lanc
ft_lancamentos_df.rename(columns={'val_lanc': 'total_lanc'}, inplace=True)

# Reordenação das colunas
ft_lancamentos_df = ft_lancamentos_df[
    [
        'cod_rubrica',
        'cod_setor',
        'cod_cargo',
        'cod_faixa',
        'cod_tempo_serv',
        'id_ano_mes',
        'total_lanc',
    ]
]

# Criação das colunas valor_bruto, valor_desconto e valor_liquido  #FIXME
ft_lancamentos_df['valor_bruto'] = ft_lancamentos_df['total_lanc']
ft_lancamentos_df['valor_desconto'] = ft_lancamentos_df['total_lanc'] * 0.1
ft_lancamentos_df['valor_liquido'] = (
    ft_lancamentos_df['valor_bruto'] - ft_lancamentos_df['valor_desconto']
)


# 3. Load
# Função para calculo do chunksize
def get_chunksize(table_columns):
    cs = 2097 // len(table_columns)
    cs = 1000 if cs > 1000 else cs
    return cs


# Exportação do dataframe dm_cargos_df do pandas para a tabela dm_cargos
dm_cargos_df.to_sql(
    name='dm_cargos',
    schema='folhadw',
    con=db_connection,
    index=False,
    if_exists='append',
    chunksize=get_chunksize(dm_cargos_df.columns),
)

# Exportação do dataframe dm_setores_df do pandas para a tabela dm_setores
dm_setores_df.to_sql(
    name='dm_setores',
    schema='folhadw',
    con=db_connection,
    index=False,
    if_exists='append',
    chunksize=get_chunksize(dm_setores_df.columns),
)

# Exportação do dataframe dm_rubricas_df do pandas para a tabela dm_rubricas
dm_rubricas_df.to_sql(
    name='dm_rubricas',
    schema='folhadw',
    con=db_connection,
    index=False,
    if_exists='append',
    chunksize=get_chunksize(dm_rubricas_df.columns),
)

# Exportação do dataframe dm_faixas_etarias_df do pandas para a tabela dm_faixas_etarias
dm_faixas_etarias_df.to_sql(
    name='dm_faixas_etarias',
    schema='folhadw',
    con=db_connection,
    index=False,
    if_exists='append',
    chunksize=get_chunksize(dm_faixas_etarias_df.columns),
)

# Exportação do dataframe dm_tempos_servicos_df do pandas para a tabela dm_tempos_servicos
dm_tempos_servicos_df.to_sql(
    name='dm_tempos_servicos',
    schema='folhadw',
    con=db_connection,
    index=False,
    if_exists='append',
    chunksize=get_chunksize(dm_tempos_servicos_df.columns),
)

# Exportação do dataframe dm_tempos_folhas_df do pandas para a tabela dm_tempos_folhas
dm_tempos_folhas_df.to_sql(
    name='dm_tempos_folhas',
    schema='folhadw',
    con=db_connection,
    index=False,
    if_exists='append',
    chunksize=get_chunksize(dm_tempos_folhas_df.columns),
)
# Exportação do dataframe ft_lancamentos_df do pandas para a tabela ft_lancamentos
ft_lancamentos_df.to_sql(
    name='ft_lancamentos',
    schema='folhadw',
    con=db_connection,
    index=False,
    if_exists='append',
    chunksize=get_chunksize(ft_lancamentos_df.columns),
)
