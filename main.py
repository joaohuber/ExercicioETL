import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

# Configurações do Banco de Dados (Usando Variáveis de Ambiente)
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "sua_senha")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "etl_project")

# Caminho para o arquivo CSV
csv_file_path = "weather_data.csv"

# Função ETL
def etl_from_csv():
    engine = None  # Inicializa o engine como None

    try:
        # Extração
        if not os.path.exists(csv_file_path):
            raise FileNotFoundError(f"Erro: O arquivo {csv_file_path} não foi encontrado.")

        df = pd.read_csv(csv_file_path)

        # Transformação (exemplo: arredondar temperatura)
        if "temperature" in df.columns:
            df["temperature"] = df["temperature"].apply(lambda x: round(x, 1))
        
        # Conexão com o banco de dados
        engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

        # Carregamento no Banco de Dados
        with engine.begin() as conn:
            df.to_sql("weather_data", conn, if_exists="append", index=False)
        
        print("✅ Dados inseridos no banco de dados com sucesso!")

    except FileNotFoundError as e:
        print(f"❌ Erro: {e}")
    except pd.errors.EmptyDataError:
        print("❌ Erro: O arquivo CSV está vazio.")
    except SQLAlchemyError as e:
        print(f"❌ Erro ao conectar ao banco de dados: {e}")
    except ModuleNotFoundError as e:
        print("❌ Erro: O módulo 'psycopg2' não está instalado. Instale com 'pip install psycopg2-binary'.")
    except Exception as e:
        print(f"❌ Erro inesperado: {e}")
    finally:
        if engine is not None:
            engine.dispose()  # Libera os recursos da conexão

if __name__ == "__main__":
    etl_from_csv()
