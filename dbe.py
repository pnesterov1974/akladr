from sqlalchemy import create_engine

class DBEngine:
    DBE = None

    @classmethod
    def init_dbe(cls, connection_string: str):
        cls.DBE = create_engine(url=connection_string)

    @classmethod
    def dbe(cls):
        if cls.DBE:
            return cls.DBE
        else:
            raise ValueError('NO CONNECTION !!!')


# ---------------------------------------------------------------------------------------
if __name__ == "__main__":
    pass