# -*- coding: utf-8 -*-
import os
import sys
import subprocess
import platform

def instalar_dependencias():
    """
    Garante que todas as dependências do requirements.txt estão instaladas.
    Funciona em Windows, Linux e Google Colab.
    """
    requirements_file = os.path.join(os.path.dirname(__file__), "requirements.txt")

    if not os.path.exists(requirements_file):
        print("⚠️ Arquivo requirements.txt não encontrado, pulando instalação...")
        return

    # Pacote para testar rapidamente se já temos o ambiente básico
    pacotes_teste = ["pandas", "google.cloud", "pytz"]

    faltando = []
    for pacote in pacotes_teste:
        try:
            __import__(pacote)
        except ImportError:
            faltando.append(pacote)

    if not faltando:
        print("✅ Todas as dependências principais já estão instaladas.")
        return

    print(f"📦 Instalando dependências ausentes: {faltando}")

    # Comando de instalação (usa o Python que está executando o script)
    cmd = [sys.executable, "-m", "pip", "install", "-r", requirements_file]

    # Ajuste para Google Colab (evita erros de permissão)
    if "google.colab" in sys.modules:
        cmd.insert(1, "pip")  # Colab lida bem assim

    # Detecta sistema operacional
    so = platform.system().lower()
    print(f"💻 Sistema detectado: {so}")

    try:
        subprocess.check_call(cmd)
        print("✅ Dependências instaladas com sucesso.")
    except subprocess.CalledProcessError as e:
        print(f"❌ Erro ao instalar dependências: {e}")
        sys.exit(1)


# ===================== Garantir dependências antes dos imports =====================
instalar_dependencias()