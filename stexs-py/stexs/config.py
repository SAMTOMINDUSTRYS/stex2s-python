import os

def get_sqlite_url():
    SQLITE_URL = "sqlite:///:memory:"
    #SQLITE_URL = "sqlite:///./test_sql.db"
    return SQLITE_URL

def get_socket_host_and_port():
    return (
        os.getenv("STEX_EXCHANGE_HOST"),
        int(os.getenv("STEX_EXCHANGE_PORT"))
    )

