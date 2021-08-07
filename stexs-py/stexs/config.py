def get_sqlite_url():
    SQLITE_URL = "sqlite:///:memory:"
    #SQLITE_URL = "sqlite:///./test_sql.db"
    return SQLITE_URL

def get_socket_host_and_port():
    return ("localhost", 5412)

