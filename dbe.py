from sqlalchemy import create_engine

from model_mssql import metadata

class DbEngine:
    DBEngine = None

    @classmethod
    def init_dbe(cls, engine_str: str):
        print(f"Инициализация db engine...")
        cls.DBEngine = create_engine(engine_str, echo=False)
        metadata.create_all(cls.DBEngine)

    @classmethod
    def get_dbe(cls):
        if cls.DBEngine:
            print(cls.DBEngine)
            return cls.DBEngine
        else:
            raise ValueError('NO CONNECTION !!')

# ---------------------------------------------------------------------------------------
if __name__ == "__main__": pass