import psycopg2

from configuration import config


class PostgresConn:
    def __init__(self):
        params = config()
        # connect to the PostgreSQL database
        self.conn = psycopg2.connect(**params)
        self.create_table()

    def insert_vendor_list(self, providers_list):
        """ insert multiple vendors into the vendors table  """
        sql = "INSERT INTO providers(provider_name, type_of_care, address, " \
              " city, state, zip, phone, email)" \
              " VALUES (%(provider_name)s, %(type_of_care)s, %(address)s, %(city)s," \
              " %(state)s, %(zip)s, %(phone)s, %(email)s)" \
              " ON CONFLICT ON CONSTRAINT providers_pkey" \
              " DO UPDATE SET phone = COALESCE(EXCLUDED.phone, providers.phone)," \
              " address = COALESCE(EXCLUDED.address, providers.address)," \
              " city = COALESCE(EXCLUDED.city, providers.city)," \
              " state = COALESCE(EXCLUDED.state, providers.state)," \
              " email = COALESCE(EXCLUDED.email, providers.email) ;"
        try:
            # create a new cursor
            with self.conn.cursor() as cur:
                # execute the INSERT statement
                cur.executemany(sql, providers_list)
                # commit the changes to the database
                self.conn.commit()
            # close communication with the database
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def insert_owner_list(self, providers_list):
        """ insert multiple vendors into the vendors table  """
        sql = " INSERT INTO providers_contact(id, provider_name, phone, email, owner_name)" \
              " VALUES (%(id)s, %(provider_name)s, %(phone)s, %(email)s," \
              " %(owner_name)s)" \
              " ON CONFLICT ON CONSTRAINT providers_contact_pkey" \
              " DO UPDATE SET phone = COALESCE(EXCLUDED.phone, providers_contact.phone)," \
              " email = COALESCE(EXCLUDED.email, providers_contact.email)," \
              " owner_name = COALESCE(EXCLUDED.owner_name, providers_contact.owner_name)"
        try:
            # create a new cursor
            with self.conn.cursor() as cur:
                # execute the INSERT statement
                cur.executemany(sql, providers_list)
                # commit the changes to the database
                self.conn.commit()
            # close communication with the database
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def update_vendor_owner(self, owner_list):
        """ insert multiple vendors into the vendors table  """
        sql = "UPDATE providers " \
              "SET owner_name = COALESCE(%(owner_name)s, providers.owner_name ) " \
              "WHERE provider_name = %(provider_name)s AND phone = %(phone)s " \
              "AND email = %(email)s "
        try:
            # create a new cursor
            with self.conn.cursor() as cur:
                # execute the INSERT statement
                cur.executemany(sql, owner_list)
                # commit the changes to the database
                self.conn.commit()
            # close communication with the database
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def create_table(self):
        query = """
        CREATE TABLE IF NOT EXISTS providers (
                provider_name varchar(255) NOT NULL, \
                type_of_care varchar(450) NOT NULL,\
                address varchar(450), \
                city varchar(100),\
                state varchar(100), \
                zip varchar(20) NOT NULL, \
                phone varchar(20), \
                email varchar(450), \
                owner_name varchar(100), \
                PRIMARY KEY (provider_name, zip))"""
        query2 = """
                CREATE TABLE IF NOT EXISTS providers_contact (
                        id varchar(255) NOT NULL,
                        provider_name varchar(255) NOT NULL, \
                        phone varchar(20), \
                        email varchar(450), \
                        owner_name varchar(100), \
                        PRIMARY KEY (id))"""
        try:
            with self.conn.cursor() as cur:
                cur.execute(query)
                cur.execute(query2)
                self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def get_num_care_providers(self):
        try:
            query = "SELECT count(*) FROM " \
                    "(SELECT provider_name FROM providers GROUP BY provider_name, type_of_care, zip) temp;"
            with self.conn.cursor() as cur:
                cur.execute(query)
                return cur.fetchone()
        except (Exception, psycopg2.DatabaseError) as error:
            raise error

    def get_max_providers_zip(self):
        try:
            query = "SELECT zip FROM (SELECT zip, COUNT(1) AS cnt " \
            "FROM providers GROUP BY zip) A " \
            "WHERE cnt = " \
            "(SELECT max(cnt) FROM (SELECT zip, COUNT(1) AS cnt FROM providers GROUP BY zip) B);"
            with self.conn.cursor() as cur:
                cur.execute(query)
                return cur.fetchone()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)



