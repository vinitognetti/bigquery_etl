# ------------+
# FUNÇÂO BASE |
# ------------+

def rts(cliente, DATASET_ORIGEM, TABELA_ORIGEM, DATASET_DESTINO, TABELA_DESTINO):

    q = cliente.montar_query(dataset=DATASET_ORIGEM,
                                tabela=TABELA_ORIGEM)

    df = cliente.query(q)

    df_exp = cliente.expand_json_columns_recursive(df)

    cliente.load_dataframe(df=df_exp,
                            dataset=DATASET_DESTINO,
                            tabela=TABELA_DESTINO,
                            existente="replace")

# -----------+
# ATTRIBUTES |
# -----------+

def stg_ads_tiktok(cliente):

    DATASET_ORIGEM = "raw_tiktok"
    TABELA_ORIGEM = "ads"
    DATASET_DESTINO = "staging"
    TABELA_DESTINO = "stg_ads"

    try:

        rts(cliente, DATASET_ORIGEM, TABELA_ORIGEM, DATASET_DESTINO, TABELA_DESTINO)

        print(f'{DATASET_DESTINO}.{TABELA_DESTINO} OK')

    except:

        print(f'{DATASET_DESTINO}.{TABELA_DESTINO} --- ERROR ---')

def stg_ad_groups_tiktok(cliente):

    DATASET_ORIGEM = "raw_tiktok"
    TABELA_ORIGEM = "ad_groups"
    DATASET_DESTINO = "staging"
    TABELA_DESTINO = "stg_ad_groups"

    try:

        rts(cliente, DATASET_ORIGEM, TABELA_ORIGEM, DATASET_DESTINO, TABELA_DESTINO)

        print(f'{DATASET_DESTINO}.{TABELA_DESTINO} OK')

    except:

        print(f'{DATASET_DESTINO}.{TABELA_DESTINO} --- ERROR ---')

def stg_campaigns_tiktok(cliente):

    DATASET_ORIGEM = "raw_tiktok"
    TABELA_ORIGEM = "ad_groups"
    DATASET_DESTINO = "staging"
    TABELA_DESTINO = "stg_campaigns"

    try:

        rts(cliente, DATASET_ORIGEM, TABELA_ORIGEM, DATASET_DESTINO, TABELA_DESTINO)

        print(f'{DATASET_DESTINO}.{TABELA_DESTINO} OK')

    except:

        print(f'{DATASET_DESTINO}.{TABELA_DESTINO} --- ERROR ---')

# ------+
# DAILY |
# ------+

def stg_ads_reports_daily_tiktok(cliente):

    DATASET_ORIGEM = "raw_tiktok"
    TABELA_ORIGEM = "ads_reports_daily"
    DATASET_DESTINO = "staging"
    TABELA_DESTINO = "stg_ads_reports_daily"

    try:

        rts(cliente, DATASET_ORIGEM, TABELA_ORIGEM, DATASET_DESTINO, TABELA_DESTINO)

        print(f'{DATASET_DESTINO}.{TABELA_DESTINO} OK')

    except:

        print(f'{DATASET_DESTINO}.{TABELA_DESTINO} --- ERROR ---')

def stg_ad_groups_reports_daily_tiktok(cliente):

    DATASET_ORIGEM = "raw_tiktok"
    TABELA_ORIGEM = "ad_groups_reports_daily"
    DATASET_DESTINO = "staging"
    TABELA_DESTINO = "stg_ad_groups_reports_daily"

    try:

        rts(cliente, DATASET_ORIGEM, TABELA_ORIGEM, DATASET_DESTINO, TABELA_DESTINO)

        print(f'{DATASET_DESTINO}.{TABELA_DESTINO} OK')

    except:

        print(f'{DATASET_DESTINO}.{TABELA_DESTINO} --- ERROR ---')

def stg_campaigns_reports_daily_tiktok(cliente):

    DATASET_ORIGEM = "raw_tiktok"
    TABELA_ORIGEM = "campaigns"
    DATASET_DESTINO = "staging"
    TABELA_DESTINO = "stg_campaigns_reports_daily"

    try:

        rts(cliente, DATASET_ORIGEM, TABELA_ORIGEM, DATASET_DESTINO, TABELA_DESTINO)

        print(f'{DATASET_DESTINO}.{TABELA_DESTINO} OK')

    except:

        print(f'{DATASET_DESTINO}.{TABELA_DESTINO} --- ERROR ---')

# ---------+
# LIFETIME |
# ---------+

def stg_ads_reports_lifetime_tiktok(cliente):

    DATASET_ORIGEM = "raw_tiktok"
    TABELA_ORIGEM = "ads_reports_lifetime"
    DATASET_DESTINO = "staging"
    TABELA_DESTINO = "stg_ads_reports_lifetime"

    try:

        rts(cliente, DATASET_ORIGEM, TABELA_ORIGEM, DATASET_DESTINO, TABELA_DESTINO)

        print(f'{DATASET_DESTINO}.{TABELA_DESTINO} OK')

    except:

        print(f'{DATASET_DESTINO}.{TABELA_DESTINO} --- ERROR ---')

def stg_ad_groups_reports_lifetime_tiktok(cliente):

    DATASET_ORIGEM = "raw_tiktok"
    TABELA_ORIGEM = "ad_groups_reports_lifetime"
    DATASET_DESTINO = "staging"
    TABELA_DESTINO = "stg_ad_groups_reports_lifetime"

    try:

        rts(cliente, DATASET_ORIGEM, TABELA_ORIGEM, DATASET_DESTINO, TABELA_DESTINO)

        print(f'{DATASET_DESTINO}.{TABELA_DESTINO} OK')

    except:

        print(f'{DATASET_DESTINO}.{TABELA_DESTINO} --- ERROR ---')

def stg_campaigns_reports_lifetime_tiktok(cliente):

    DATASET_ORIGEM = "raw_tiktok"
    TABELA_ORIGEM = "campaigns_reports_lifetime"
    DATASET_DESTINO = "staging"
    TABELA_DESTINO = "stg_campaigns_reports_lifetime"

    try:

        rts(cliente, DATASET_ORIGEM, TABELA_ORIGEM, DATASET_DESTINO, TABELA_DESTINO)

        print(f'{DATASET_DESTINO}.{TABELA_DESTINO} OK')

    except:

        print(f'{DATASET_DESTINO}.{TABELA_DESTINO} --- ERROR ---')
