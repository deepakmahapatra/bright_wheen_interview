import os


def config():
    # get section, default to postgresql
    db = {}
    db["host"] = os.getenv("DB_HOST", "localhost")
    db["user"] = os.getenv("DB_SUER", "postgres")
    db["password"] = os.getenv("DB_PASS", "postgres")
    db["database"] = os.getenv("DB_DATABASE", "postgres")
    return db
