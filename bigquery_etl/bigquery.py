# Importa o instalador de dependências
from requirements import instalar_dependencias

# Garante dependências antes de seguir
instalar_dependencias()

# Agora é seguro importar bibliotecas externas
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import json
from datetime import datetime
import pytz


class BigQueryPipeline:
    """
    Classe para gerenciar operações ETL com BigQuery.
    Agora, no momento da criação do objeto, já garante que as dependências foram instaladas.
    """

    def __init__(self, credenciais_path: str, project_id: str):
        """
        Inicializa a instância da pipeline com credenciais e projeto.
        Args:
            credenciais_path (str): Caminho do arquivo JSON da conta de serviço.
            project_id (str): ID do projeto no Google Cloud.
        """
        self.credenciais_path = credenciais_path
        self.project_id = project_id
        self.credentials = service_account.Credentials.from_service_account_file(credenciais_path)
        self.client = bigquery.Client(project=project_id, credentials=self.credentials, location='US')
        self.project_name = self.credentials.project_id

    # ===== Métodos de instância =====
    def query(self, query_str: str) -> pd.DataFrame:
        """Executa uma query SQL no BigQuery."""
        job = self.client.query(query_str)
        return job.result().to_dataframe()

    def criar_schema(self, dataset: str, location="US", descricao=None):
        """Cria um dataset no BigQuery."""
        dataset_ref = bigquery.Dataset(f"{self.project_id}.{dataset}")
        dataset_ref.location = location
        if descricao:
            dataset_ref.description = descricao
        self.client.create_dataset(dataset_ref, exists_ok=True)
        print(f"Schema '{dataset}' criado no projeto '{self.project_id}' na região {location}.")

    def load_dataframe(self, df: pd.DataFrame, dataset: str, tabela: str, existente='append'):
        """Carrega DataFrame no BigQuery."""
        df.to_gbq(
            destination_table=f"{dataset}.{tabela}",
            project_id=self.project_id,
            credentials=self.credentials,
            if_exists=existente
        )
        print(f"DataFrame enviado para {dataset}.{tabela} no BigQuery!")

    def expand_json_columns_recursive(self, df: pd.DataFrame) -> pd.DataFrame:
        """Expande colunas JSON recursivamente."""
        df_result = df.copy()
        json_cols = [col for col in df_result.columns if df_result[col].apply(self.is_json_string).any()]

        for col in json_cols:
            print(f"📦 Expandindo coluna JSON: {col}")
            parsed = df_result[col].apply(lambda x: json.loads(x) if self.is_json_string(x) else x)

            if parsed.apply(lambda x: isinstance(x, list)).any():
                df_result = df_result.drop(columns=[col]).join(parsed.rename(col))
                df_result = df_result.explode(col, ignore_index=True)

                if df_result[col].apply(lambda x: isinstance(x, dict)).any():
                    dict_df = pd.json_normalize(df_result[col])
                    dict_df.columns = [f"{col}_{c}" for c in dict_df.columns]
                    df_result = pd.concat([df_result.drop(columns=[col]), dict_df], axis=1)

            elif parsed.apply(lambda x: isinstance(x, dict)).any():
                dict_df = pd.json_normalize(parsed)
                dict_df.columns = [f"{col}_{c}" for c in dict_df.columns]
                df_result = pd.concat([df_result.drop(columns=[col]), dict_df], axis=1)

        if any(df_result[col].apply(self.is_json_string).any() for col in df_result.columns):
            return self.expand_json_columns_recursive(df_result)

        return df_result

    def montar_query(self, dataset: str, tabela: str, cols='*', limite=None) -> str:
        """Monta query SELECT simples."""
        if limite is not None:
            return f"SELECT {cols} FROM `{self.project_name}.{dataset}.{tabela}` LIMIT {limite}"
        else:
            return f"SELECT {cols} FROM `{self.project_name}.{dataset}.{tabela}`"

    def listar_tabelas(self, dataset_id: str):

        """
        Retorna uma lista com os nomes completos (project.dataset.table) de todas as tabelas
        em um dataset do BigQuery.

        Parâmetros:
            dataset_id (str): ID completo do dataset no formato 'projeto.dataset'.

        Retorno:
            List[str]: Nomes das tabelas encontradas.
        """

        try:
            tables = self.client.list_tables(dataset_id)  # Requisição de API
        except Exception as e:
            print(f"Erro ao listar tabelas no dataset {dataset_id}: {e}")
            return []

        full_names = [
            f"{table.table_id}"
            for table in tables
        ]

        return full_names

    # ===== Métodos estáticos =====
    @staticmethod
    def is_json_string(value) -> bool:
        """Verifica se string é JSON válido."""
        if not isinstance(value, str):
            return False
        try:
            obj = json.loads(value)
            return isinstance(obj, (dict, list))
        except (ValueError, TypeError):
            return False

    @staticmethod
    def converter_para_brasilia(data_str: str) -> str:
        """Converte datetime UTC para Brasília."""
        utc = pytz.UTC
        brasilia = pytz.timezone("America/Sao_Paulo")
        dt_utc = datetime.fromisoformat(data_str).astimezone(utc)
        dt_brasilia = dt_utc.astimezone(brasilia)
        return dt_brasilia.strftime("%Y-%m-%d %H:%M:%S")

