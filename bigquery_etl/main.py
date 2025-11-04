# -*- coding: utf-8 -*-
"""
Exemplo de uso da classe BigQueryPipeline.
- Conecta ao BigQuery usando chave JSON.
- Executa consulta.
- Expande colunas JSON.
- Salva dados transformados em um schema de staging.
"""

# Import do cliente inhouse do BigQuery
from bigquery import BigQueryPipeline

# Imports para as dimensões auxiliares
from dims_auxs.dim_datas import *

# Imports para tiktok
from etl_tiktok.staging import *
from etl_tiktok.factual import *
from etl_tiktok.dimension import *

# -------------- FUNÇÕES ------------------

def auxiliares(cliente):

    # ----------+
    # DIMS AUXS |
    # ----------+
    print('-' * 40)
    print('Dimensões Auxiliares')
    print('-' * 40)

    dim_datas(cliente)

def tiktok(cliente):

    # -------+
    # Tiktok |
    # -------+
    print()
    print('-' * 40)
    print('ETL - Tiktok')
    print('-' * 40)

    # Raw to Staging
    print('\nRaw to Staging')
    print('-' * 40)

    stg_ads_tiktok(cliente)
    stg_ad_groups_tiktok(cliente)
    stg_campaigns_tiktok(cliente)

    stg_ads_reports_daily_tiktok(cliente)
    stg_ad_groups_reports_daily_tiktok(cliente)
    stg_campaigns_reports_daily_tiktok(cliente)

    stg_ads_reports_lifetime_tiktok(cliente)
    stg_ad_groups_reports_lifetime_tiktok(cliente)
    stg_campaigns_reports_lifetime_tiktok(cliente)

    # Staging to Factual
    print('\nStaging to Factual')
    print('-' * 40)

    fct_ads_reports_daily_tiktok(cliente)
    fct_ad_groups_reports_daily_tiktok(cliente)
    fct_campaigns_reports_daily_tiktok(cliente)

    fct_ads_reports_lifetime_tiktok(cliente)
    fct_ad_groups_reports_lifetime_tiktok(cliente)
    fct_campaigns_reports_lifetime_tiktok(cliente)

    # Staging to Dimension
    print('\nStaging to Dimension')
    print('-' * 40)
    
    dim_ads_tiktok(cliente)
    dim_ad_groups_tiktok(cliente)
    dim_campaigns_tiktok(cliente)

    # Factual/Dimension to Views
    print('\nFactual/Dimension to Views')
    print('-' * 40)

def main():

    # Configurações
    CAMINHO_CREDENCIAIS = "chave_bigquery.json"
    PROJECT_ID = ""

    # Cliente do BigQuery
    bq = BigQueryPipeline(CAMINHO_CREDENCIAIS, PROJECT_ID)

    # ETLs
    auxiliares(bq)
    tiktok(bq)

# ------------- MAIN --------------

if __name__ == "__main__":

    main()


