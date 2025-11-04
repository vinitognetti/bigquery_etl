def criacao_dim_datas(cliente):

    import holidays

    # Configuração do intervalo de datas
    data_inicio = "2020-01-01"
    data_fim = "2030-12-31"

    # Cria um DataFrame com todas as datas
    df = pd.DataFrame({"data": pd.date_range(start=data_inicio, end=data_fim)})

    # Colunas derivadas
    df["ano"] = df["data"].dt.year
    df["mes"] = df["data"].dt.month
    df["nome_mes"] = df["data"].dt.strftime("%B")  # Nome do mês
    df["trimestre"] = df["data"].dt.quarter
    df["dia_semana"] = df["data"].dt.weekday + 1   # 1=Segunda ... 7=Domingo
    df["nome_dia_semana"] = df["data"].dt.strftime("%A")
    df["ano_mes"] = df["data"].dt.strftime("%Y%m").astype(int)
    df["eh_fim_semana"] = df["dia_semana"].isin([6, 7]).astype(int)

    feriados_br = holidays.Brazil(years=range(df["ano"].min(), df["ano"].max() + 1))
    df["eh_feriado"] = df["data"].isin(feriados_br).astype(int)

    MESES_EN_PT = {
        "January": "Janeiro",
        "February": "Fevereiro",
        "March": "Março",
        "April": "Abril",
        "May": "Maio",
        "June": "Junho",
        "July": "Julho",
        "August": "Agosto",
        "September": "Setembro",
        "October": "Outubro",
        "November": "Novembro",
        "December": "Dezembro"
    }

    df['nome_mes'] = df['nome_mes'].map(MESES_EN_PT)

    DIAS_EN_PT = {
        "Monday": "Segunda-feira",
        "Tuesday": "Terça-feira",
        "Wednesday": "Quarta-feira",
        "Thursday": "Quinta-feira",
        "Friday": "Sexta-feira",
        "Saturday": "Sábado",
        "Sunday": "Domingo"
    }

    df['nome_dia_semana'] = df['nome_dia_semana'].map(DIAS_EN_PT).apply(lambda x: x.replace('-feira', ''))

    df['data'] = df['data'].apply(lambda x: x.strftime("%Y-%m-%d"))

def dim_datas(cliente):

    DATASET_DESTINO = "dims_auxs"
    TABELA_DESTINO = "dim_datas"

    try:

        df = criacao_dim_datas(cliente)

        cliente.load_dataframe(df=df,
                            dataset=DATASET_DESTINO,
                            tabela=TABELA_DESTINO,
                            existente="replace")

        print(f'{DATASET_DESTINO}.{TABELA_DESTINO} OK')

    except:

        print(f'{DATASET_DESTINO}.{TABELA_DESTINO} --- ERROR ---')