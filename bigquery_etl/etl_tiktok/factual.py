# --------------+
# FUNÇÕES BASES |
# --------------+

def stf_lifetime(cliente, DATASET_ORIGEM, TABELA_ORIGEM, DATASET_DESTINO, TABELA_DESTINO, cols, cols_pt, ids):

    q = cliente.montar_query(DATASET_ORIGEM, TABELA_ORIGEM)

    df = cliente.query(q)

    df_fct = df[cols]

    df_fct[ids] = df_fct[ids].astype(str)

    df_fct['metrics_spend'] = round(df_fct['metrics_spend'].astype(float), 2)

    df_fct['metrics_impressions'] = df_fct['metrics_impressions'].astype(int)
    df_fct['metrics_reach'] = df_fct['metrics_reach'].astype(int)
    df_fct['metrics_video_watched_6s'] = df_fct['metrics_video_watched_6s'].astype(int)
    df_fct['metrics_follows'] = df_fct['metrics_follows'].astype(int)
    df_fct['metrics_clicks'] = df_fct['metrics_clicks'].astype(int)
    df_fct['metrics_likes'] = df_fct['metrics_likes'].astype(int)
    df_fct['metrics_comments'] = df_fct['metrics_comments'].astype(int)

    df_fct.columns = cols_pt

    cliente.load_dataframe(df=df_fct,
                      dataset=DATASET_DESTINO,
                      tabela=TABELA_DESTINO,
                      existente='replace')

def stf_daily(cliente, DATASET_ORIGEM, TABELA_ORIGEM, DATASET_DESTINO, TABELA_DESTINO, cols, cols_pt, ids):
    
    q = cliente.montar_query(DATASET_ORIGEM, TABELA_ORIGEM)

    df = cliente.query(q)

    df_fct = df[cols]

    df_fct[ids] = df_fct[ids].astype(str)

    df_fct['stat_time_day'] = df_fct['stat_time_day'].apply(lambda x: x.strftime('%Y-%m-%d'))

    df_fct['metrics_spend'] = round(df_fct['metrics_spend'].astype(float), 2)

    df_fct['metrics_impressions'] = df_fct['metrics_impressions'].astype(int)
    df_fct['metrics_reach'] = df_fct['metrics_reach'].astype(int)
    df_fct['metrics_video_watched_6s'] = df_fct['metrics_video_watched_6s'].astype(int)
    df_fct['metrics_follows'] = df_fct['metrics_follows'].astype(int)
    df_fct['metrics_clicks'] = df_fct['metrics_clicks'].astype(int)
    df_fct['metrics_likes'] = df_fct['metrics_likes'].astype(int)
    df_fct['metrics_comments'] = df_fct['metrics_comments'].astype(int)

    df_fct.columns = cols_pt

    cliente.load_dataframe(df=df_fct,
                      dataset=DATASET_DESTINO,
                      tabela=TABELA_DESTINO,
                      existente='replace')

# ------+
# DAILY |
# ------+

def fct_ads_reports_daily_tiktok(cliente):

    DATASET_ORIGEM    = "staging"
    TABELA_ORIGEM     = "stg_ads_reports_daily"
    DATASET_DESTINO   = "tiktok"
    TABELA_DESTINO    = "fct_ads_reports_daily"

    cols = [
        'stat_time_day'
        'ad_id',
        'metrics_spend',
        'metrics_impressions',
        'metrics_reach',
        'metrics_video_watched_6s',
        'metrics_follows',
        'metrics_clicks',
        'metrics_likes',
        'metrics_comments'
        ]

    cols_pt = [
        'data',
        'id_anuncio',
        'gasto',
        'impressoes',
        'alcance',
        'video_6s',
        'seguidores',
        'cliques',
        'curtidas',
        'comentarios']

    ids = [
        'ad_id'
    ]

    try:

        stf_daily(cliente, DATASET_ORIGEM, TABELA_ORIGEM, DATASET_DESTINO, TABELA_DESTINO,  cols, cols_pt)

        print(f'{DATASET_DESTINO}.{TABELA_DESTINO} OK')

    except:

        print(f'{DATASET_DESTINO}.{TABELA_DESTINO} --- ERROR ---')

def fct_ad_groups_reports_daily_tiktok(cliente):

    DATASET_ORIGEM    = "staging"
    TABELA_ORIGEM     = "stg_ad_groups_reports_daily"
    DATASET_DESTINO   = "tiktok"
    TABELA_DESTINO    = "fct_ad_groups_reports_daily"

    cols = [
        'stat_time_day'
        'adgroup_id',
        'metrics_spend',
        'metrics_impressions',
        'metrics_reach',
        'metrics_video_watched_6s',
        'metrics_follows',
        'metrics_clicks',
        'metrics_likes',
        'metrics_comments'
        ]

    cols_pt = [
        'data',
        'id_grupo_anuncio',
        'gasto',
        'impressoes',
        'alcance',
        'video_6s',
        'seguidores',
        'cliques',
        'curtidas',
        'comentarios']

    ids = [
        'adgroup_id'
    ]

    try:

        stf_daily(cliente, DATASET_ORIGEM, TABELA_ORIGEM, DATASET_DESTINO, TABELA_DESTINO,  cols, cols_pt)

        print(f'{DATASET_DESTINO}.{TABELA_DESTINO} OK')

    except:

        print(f'{DATASET_DESTINO}.{TABELA_DESTINO} --- ERROR ---')

def fct_campaigns_reports_daily_tiktok(cliente):

    DATASET_ORIGEM    = "staging"
    TABELA_ORIGEM     = "stg_campaigns_reports_daily"
    DATASET_DESTINO   = "tiktok"
    TABELA_DESTINO    = "fct_campaigns_reports_daily"

    cols = [
        'stat_time_day'
        'campaign_id',
        'metrics_spend',
        'metrics_impressions',
        'metrics_reach',
        'metrics_video_watched_6s',
        'metrics_follows',
        'metrics_clicks',
        'metrics_likes',
        'metrics_comments'
        ]

    cols_pt = [
        'data',
        'id_campanha',
        'gasto',
        'impressoes',
        'alcance',
        'video_6s',
        'seguidores',
        'cliques',
        'curtidas',
        'comentarios']

    ids = [
        'campaign_id'
    ]

    try:

        stf_daily(cliente, DATASET_ORIGEM, TABELA_ORIGEM, DATASET_DESTINO, TABELA_DESTINO,  cols, cols_pt)

        print(f'{DATASET_DESTINO}.{TABELA_DESTINO} OK')

    except:

        print(f'{DATASET_DESTINO}.{TABELA_DESTINO} --- ERROR ---')

# ---------+
# LIFETIME |
# ---------+

def fct_ads_reports_lifetime_tiktok(cliente):

    DATASET_ORIGEM    = "staging"
    TABELA_ORIGEM     = "stg_ads_reports_lifetime"
    DATASET_DESTINO   = "tiktok"
    TABELA_DESTINO    = "fct_ads_reports_lifetime"

    cols = [
        'ad_id',
        'metrics_spend',
        'metrics_impressions',
        'metrics_reach',
        'metrics_video_watched_6s',
        'metrics_follows',
        'metrics_clicks',
        'metrics_likes',
        'metrics_comments'
        ]

    cols_pt = [
        'id_anuncio',
        'gasto',
        'impressoes',
        'alcance',
        'video_6s',
        'seguidores',
        'cliques',
        'curtidas',
        'comentarios']

    ids = [
        'ad_id'
    ]

    try:

        stf_lifetime(cliente, DATASET_ORIGEM, TABELA_ORIGEM, DATASET_DESTINO, TABELA_DESTINO,  cols, cols_pt)

        print(f'{DATASET_DESTINO}.{TABELA_DESTINO} OK')

    except:

        print(f'{DATASET_DESTINO}.{TABELA_DESTINO} --- ERROR ---')

def fct_ad_groups_reports_lifetime_tiktok(cliente):

    DATASET_ORIGEM    = "staging"
    TABELA_ORIGEM     = "stg_ad_groups_reports_lifetime"
    DATASET_DESTINO   = "tiktok"
    TABELA_DESTINO    = "fct_ad_groups_reports_lifetime"

    cols = [
        'adgroup_id',
        'metrics_spend',
        'metrics_impressions',
        'metrics_reach',
        'metrics_video_watched_6s',
        'metrics_follows',
        'metrics_clicks',
        'metrics_likes',
        'metrics_comments'
        ]

    cols_pt = [
        'id_grupo_anuncio',
        'gasto',
        'impressoes',
        'alcance',
        'video_6s',
        'seguidores',
        'cliques',
        'curtidas',
        'comentarios']

    ids = [
        'adgroup_id'
    ]

    try:

        stf_lifetime(cliente, DATASET_ORIGEM, TABELA_ORIGEM, DATASET_DESTINO, TABELA_DESTINO,  cols, cols_pt)

        print(f'{DATASET_DESTINO}.{TABELA_DESTINO} OK')

    except:

        print(f'{DATASET_DESTINO}.{TABELA_DESTINO} --- ERROR ---')

def fct_campaigns_reports_lifetime_tiktok(cliente):

    DATASET_ORIGEM    = "staging"
    TABELA_ORIGEM     = "stg_campaigns_reports_lifetime"
    DATASET_DESTINO   = "tiktok"
    TABELA_DESTINO    = "fct_campaigns_reports_lifetime"

    cols = [
        'campaign_id',
        'metrics_spend',
        'metrics_impressions',
        'metrics_reach',
        'metrics_video_watched_6s',
        'metrics_follows',
        'metrics_clicks',
        'metrics_likes',
        'metrics_comments'
        ]

    cols_pt = [
        'id_campanha',
        'gasto',
        'impressoes',
        'alcance',
        'video_6s',
        'seguidores',
        'cliques',
        'curtidas',
        'comentarios']

    ids = [
        'campaign_id'
    ]

    try:

        stf_lifetime(cliente, DATASET_ORIGEM, TABELA_ORIGEM, DATASET_DESTINO, TABELA_DESTINO,  cols, cols_pt)

        print(f'{DATASET_DESTINO}.{TABELA_DESTINO} OK')

    except:

        print(f'{DATASET_DESTINO}.{TABELA_DESTINO} --- ERROR ---')

