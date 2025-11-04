# ------------+
# FUNÇÃO BASE |
# ------------+

def std(cliente, DATASET_ORIGEM, TABELA_ORIGEM, DATASET_DESTINO, TABELA_DESTINO, cols, cols_pt):

    q = cliente.montar_query(dataset=DATASET_ORIGEM,
                        tabela=TABELA_ORIGEM)
    df = cliente.query(q)

    df = df[cols]

    df.columns = cols_pt

    df['data_criacao'] = df['data_criacao'].apply(lambda x: x.strftime('%Y-%m-%d'))

    df.drop_duplicates(inplace=True)

    cliente.load_dataframe(df, DATASET_DESTINO, TABELA_DESTINO, existente='replace')

# ---------------+
# FUNÇÕES TABELA |
# ---------------+

def dim_ads_tiktok(cliente):

    DATASET_ORIGEM = "staging"
    TABELA_ORIGEM = "stg_ads"
    DATASET_DESTINO = 'tiktok'
    TABELA_DESTINO = 'dim_ads'

    cols = [
        'ad_id',
        'adgroup_id',
        'campaign_id',
        'campaign_name',
        'create_time'
    ]

    cols_pt = [
        'id_anuncio',
        'id_grupo_anuncio',
        'id_campanha',
        'nome',
        'data_criacao'
    ]

    try:

        std(cliente, DATASET_ORIGEM, TABELA_ORIGEM, DATASET_DESTINO, TABELA_DESTINO,  cols, cols_pt)

        print(f'{DATASET_DESTINO}.{TABELA_DESTINO} OK')

    except:

        print(f'{DATASET_DESTINO}.{TABELA_DESTINO} --- ERROR ---')

def dim_ad_groups_tiktok(cliente):

    DATASET_ORIGEM = "staging"
    TABELA_ORIGEM = "stg_ad_groups"
    DATASET_DESTINO = 'tiktok'
    TABELA_DESTINO = 'dim_ad_groups'

    cols = [
        'adgroup_id',
        'campaign_id',
        'campaign_name',
        'create_time'
    ]

    cols_pt = [
        'id_grupo_anuncio',
        'id_campanha',
        'nome',
        'data_criacao'
    ]

    try:

        std(cliente, DATASET_ORIGEM, TABELA_ORIGEM, DATASET_DESTINO, TABELA_DESTINO,  cols, cols_pt)

        print(f'{DATASET_DESTINO}.{TABELA_DESTINO} OK')

    except:

        print(f'{DATASET_DESTINO}.{TABELA_DESTINO} --- ERROR ---')

def dim_campaigns_tiktok(cliente):

    DATASET_ORIGEM = "staging"
    TABELA_ORIGEM = "stg_campaigns"
    DATASET_DESTINO = 'tiktok'
    TABELA_DESTINO = 'dim_campaigns'

    cols = [
        'campaign_id',
        'campaign_name',
        'create_time'
    ]

    cols_pt = [
        'id_campanha',
        'nome',
        'data_criacao'
    ]

    try:

        std(cliente, DATASET_ORIGEM, TABELA_ORIGEM, DATASET_DESTINO, TABELA_DESTINO,  cols, cols_pt)

        print(f'{DATASET_DESTINO}.{TABELA_DESTINO} OK')

    except:

        print(f'{DATASET_DESTINO}.{TABELA_DESTINO} --- ERROR ---')

