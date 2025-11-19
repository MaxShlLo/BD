from psycopg2 import connect
import time

class Model:
    def __init__(self):
        self.connection = connect(
            database="postgres",
            user="postgres",
            password="1234",
            host="localhost",
            port="5432",
        )

        # ======== INSERT QUERIES ========
        self.insert_queries = {
            "laboratory": """INSERT INTO laboratory(lab_name) VALUES (%s)""",
            "researcher": """INSERT INTO researcher(full_name, level, laboratory_id) VALUES (%s, %s, %s)""",
            "object_type": """INSERT INTO object_type(type, galaxy_location) VALUES (%s, %s)""",
            "object": """INSERT INTO object(name, distance, laboratory_id, type_id) VALUES (%s, %s, %s, %s)""",
        }

        # ======== READ QUERIES ========
        self.read_queries = {
            "laboratory": "SELECT id, lab_name FROM laboratory",
            "researcher": "SELECT r.id, r.full_name, r.level, l.lab_name "
                          "FROM researcher AS r "
                          "JOIN laboratory AS l ON r.laboratory_id = l.id",
            "object": "SELECT o.id, o.name, o.distance, l.lab_name, t.type, t.galaxy_location "
                      "FROM object AS o "
                      "JOIN laboratory AS l ON o.laboratory_id = l.id "
                      "JOIN object_type AS t ON o.type_id = t.id",
            "object_type": "SELECT id, type, galaxy_location FROM object_type",
        }

        # ======== DELETE QUERIES ========
        self.delete_queries = {
            "laboratory": "DELETE FROM laboratory WHERE id = %s",
            "researcher": "DELETE FROM researcher WHERE id = %s",
            "object": "DELETE FROM object WHERE id = %s",
            "object_type": "DELETE FROM object_type WHERE id = %s",
        }

        # ======== UPDATE QUERIES ========
        self.update_queries = {
            "laboratory": {
                "lab_name": "UPDATE laboratory SET lab_name = %s WHERE id = %s",
            },
            "researcher": {
                "full_name": "UPDATE researcher SET full_name = %s WHERE id = %s",
                "level": "UPDATE researcher SET level = %s WHERE id = %s",
                "laboratory_id": "UPDATE researcher SET laboratory_id = %s WHERE id = %s",
            },
            "object_type": {
                "type": "UPDATE object_type SET type = %s WHERE id = %s",
                "galaxy_location": "UPDATE object_type SET galaxy_location = %s WHERE id = %s",
            },
            "object": {
                "name": "UPDATE object SET name = %s WHERE id = %s",
                "distance": "UPDATE object SET distance = %s WHERE id = %s",
                "laboratory_id": "UPDATE object SET laboratory_id = %s WHERE id = %s",
                "type_id": "UPDATE object SET type_id = %s WHERE id = %s",
            },
        }

    # ======== BASIC METHODS ========

    def disconnect(self):
        if self.connection and self.connection.closed == 0:
            self.connection.close()

    def _execute_select(self, query: str, data=None) -> list:
        cur = self.connection.cursor()
        try:
            cur.execute(query, data or ())
            rows = cur.fetchall()
            cur.close()
            return rows
        except Exception as e:
            print(f"\n Unexpected error in SELECT: {type(e).__name__} {e}\n")
            self.connection.rollback()
            cur.close()
            return []

    def _execute_modify(self, query: str, data: tuple):
        cur = self.connection.cursor()
        try:
            print(f"[DEBUG] Executing SQL: {query}")
            print(f"[DEBUG] With data: {data}")

            cur.execute(query, data)
            self.connection.commit()

            affected = cur.rowcount
            cur.close()
            return affected

        except Exception as e:
            print(f"\n Unexpected error in modify-query: {type(e).__name__} {e}\n")
            self.connection.rollback()     # ?? CRITICAL FIX
            cur.close()
            return 0


    # ======== CRUD OPERATIONS ========

    ## CREATE
    def create_laboratory(self, lab_name):
        self._execute_modify(self.insert_queries["laboratory"], (lab_name,))

    def create_researcher(self, full_name, level, laboratory_id): 
        self._execute_modify(self.insert_queries["researcher"], (full_name, level, laboratory_id))

    def create_object_type(self, type_name, galaxy_location):
        self._execute_modify(self.insert_queries["object_type"], (type_name, galaxy_location))

    def create_object(self, name, distance, laboratory_id, type_id):
        self._execute_modify(self.insert_queries["object"], (name, distance, laboratory_id, type_id))

    ## READ
    def read(self, table_name):
        return self._execute_select(self.read_queries[table_name])

    ## UPDATE
    def update_laboratory_field(self, lab_id, new_name):
        query = self.update_queries["laboratory"]["lab_name"]
        affected = self._execute_modify(query, (new_name, lab_id))
        return affected

    def update_researcher_field(self, researcher_id, field, new_value):
        query = self.update_queries["researcher"].get(field)
        if not query:
            raise ValueError(f"Unknown field for researcher: {field}")
        affected = self._execute_modify(query, (new_value, researcher_id))
        return affected

    def update_object_type_field(self, type_id, field, new_value):
        query = self.update_queries["object_type"].get(field)
        if not query:
            raise ValueError(f"Unknown field for object_type: {field}")
        affected = self._execute_modify(query, (new_value, type_id))
        return affected


    def update_object_field(self, object_id, field, new_value):
        query = self.update_queries["object"].get(field)
        if not query:
            raise ValueError(f"Unknown field for object: {field}")
        affected = self._execute_modify(query, (new_value, object_id))
        return affected
    ## DELETE
    def delete(self, table_name, record_id):
        self._execute_modify(self.delete_queries[table_name], (record_id,))

    # ======== DELETE METHODS ========

    def delete_laboratory(self, lab_id):
        affected = self._execute_modify(self.delete_queries["laboratory"], (lab_id,))
        if affected == 0:
            print(f"[INFO] No laboratory with id={lab_id} nothing deleted.")
        else:
            print(f"[SUCCESS] Laboratory id={lab_id} deleted successfully.")
        return affected


    def delete_researcher(self, researcher_id):
        affected = self._execute_modify(self.delete_queries["researcher"], (researcher_id,))
        if affected == 0:
            print(f"[INFO] No researcher with id={researcher_id} nothing deleted.")
        else:
            print(f"[SUCCESS] Researcher id={researcher_id} deleted successfully.")
        return affected


    def delete_object(self, object_id):
        affected = self._execute_modify(self.delete_queries["object"], (object_id,))
        if affected == 0:
            print(f"[INFO] No object with id={object_id} nothing deleted.")
        else:
            print(f"[SUCCESS] Object id={object_id} deleted successfully.")
        return affected


    def delete_object_type(self, type_id):
        affected = self._execute_modify(self.delete_queries["object_type"], (type_id,))
        if affected == 0:
            print(f"[INFO] No object_type with id={type_id} nothing deleted.")
        else:
            print(f"[SUCCESS] Object type id={type_id} deleted successfully.")
        return affected


    def generate_laboratories(self, n: int):
        query = """
        WITH gen AS (
            SELECT
                chr(65 + trunc(random()*26)::int) ||
                chr(65 + trunc(random()*26)::int) ||
                chr(65 + trunc(random()*26)::int) ||
                '-' ||
                (ARRAY['L','O','I','R'])[floor(random()*4)::int + 1] AS lab_name
            FROM generate_series(1, %s)
        )
        INSERT INTO laboratory(lab_name)
        SELECT DISTINCT lab_name
        FROM gen
        WHERE lab_name NOT IN (SELECT lab_name FROM laboratory);
        """

        cur = self.connection.cursor()
        cur.execute(query, (n,))
        self.connection.commit()

        rowcount = cur.rowcount
        cur.close()
        return rowcount

    def generate_researchers(self, n: int):
        # Get all existing laboratory IDs
        lab_query = "SELECT id FROM laboratory;"
        lab_ids = self._execute_select(lab_query)

        if not lab_ids:
            print("[ERROR] Cannot generate researchers: no laboratories exist.")
            return 0

        flat_ids = [row[0] for row in lab_ids]

        sql = """
        WITH params AS (
            SELECT %s::int[] AS lab_ids, %s::int AS n
        ),
        gen AS (
            SELECT
                -- Random full name: 5 uppercase letters
                chr(65 + floor(random()*26)::int) ||
                chr(65 + floor(random()*26)::int) ||
                chr(65 + floor(random()*26)::int) ||
                chr(65 + floor(random()*26)::int) ||
                chr(65 + floor(random()*26)::int) AS full_name,

                -- Random research level
                (ARRAY['Junior','Middle','Senior','Lead'])[floor(random()*4)::int + 1] AS level,

                -- Random lab id chosen from provided lab_ids array
                params.lab_ids[floor(random() * array_length(params.lab_ids, 1))::int + 1] AS lab_id

            FROM params, generate_series(1, params.n)
        )
        INSERT INTO researcher(full_name, level, laboratory_id)
        SELECT full_name, level, lab_id
        FROM gen;
        """

        cur = self.connection.cursor()
        # pass (lab_ids_array, n) — note the order matches params in SQL
        cur.execute(sql, (flat_ids, n))
        self.connection.commit()
        affected = cur.rowcount
        cur.close()

        return affected


    def generate_objects(self, n: int):
        # get lab IDs
        lab_ids = self._execute_select("SELECT id FROM laboratory")
        if not lab_ids:
            print("[ERROR] Cannot generate objects: no laboratories exist.")
            return 0

        flat_labs = [row[0] for row in lab_ids]

        # get type IDs
        type_ids = self._execute_select("SELECT id FROM object_type")
        if not type_ids:
            print("[ERROR] Cannot generate objects: no object types exist.")
            return 0

        flat_types = [row[0] for row in type_ids]

        sql = """
        WITH params AS (
            SELECT %s::int[] AS lab_ids,
                   %s::int[] AS type_ids,
                   %s::int    AS n
        ),
        gen AS (
            SELECT
                -- random object name
                chr(65 + floor(random()*26)::int) ||
                chr(65 + floor(random()*26)::int) ||
                chr(65 + floor(random()*26)::int) ||
                chr(65 + floor(random()*26)::int) ||
                chr(65 + floor(random()*26)::int) AS name,

                -- random distance between 1,000 and 1,000,000,000
                (random() * 999999000 + 1000)::int AS distance,

                -- random lab
                params.lab_ids[
                    floor(random() * array_length(params.lab_ids, 1))::int + 1
                ] AS lab_id,

                -- random type
                params.type_ids[
                    floor(random() * array_length(params.type_ids, 1))::int + 1
                ] AS type_id

            FROM params, generate_series(1, params.n)
        )
        INSERT INTO object(name, distance, laboratory_id, type_id)
        SELECT name, distance, lab_id, type_id
        FROM gen;
        """

        cur = self.connection.cursor()
        cur.execute(sql, (flat_labs, flat_types, n))
        self.connection.commit()

        affected = cur.rowcount
        cur.close()
        return affected


    def generate_object_types(self, n: int):
        
        sql = """
        WITH gen AS (
            SELECT
                -- random type name (5 LETTERS)
                chr(65 + floor(random()*26)::int) ||
                chr(65 + floor(random()*26)::int) ||
                chr(65 + floor(random()*26)::int) ||
                chr(65 + floor(random()*26)::int) ||
                chr(65 + floor(random()*26)::int)     AS type,

                -- random location (5 LETTERS)
                chr(65 + floor(random()*26)::int) ||
                chr(65 + floor(random()*26)::int) ||
                chr(65 + floor(random()*26)::int) ||
                chr(65 + floor(random()*26)::int) ||
                chr(65 + floor(random()*26)::int)     AS galaxy_location
            FROM generate_series(1, %s)
        )
        INSERT INTO object_type(type, galaxy_location)
        SELECT type, galaxy_location FROM gen;
        """

        cur = self.connection.cursor()
        cur.execute(sql, (n,))
        self.connection.commit()

        affected = cur.rowcount
        cur.close()
        return affected

    def search_researchers(self, lab_like, level):
        sql = """
        SELECT r.id, r.full_name, r.level, l.lab_name
        FROM researcher r
        JOIN laboratory l ON r.laboratory_id = l.id
        WHERE
            (%s = '-' OR l.lab_name LIKE %s)
            AND (%s = '-' OR r.level = %s)
        ORDER BY r.id;
        """

        args = [
            lab_like, f"%{lab_like}%",
            level, level
        ]

        t0 = time.time()
        cur = self.connection.cursor()
        cur.execute(sql, args)
        rows = cur.fetchall()
        ms = (time.time() - t0) * 1000
        cur.close()

        return rows, ms

    def search_objects(self, lab_like, type_like):
        sql = """
            SELECT o.id, o.name, CAST(o.distance AS numeric), l.lab_name, t.type, t.galaxy_location
            FROM object o
            JOIN laboratory l ON o.laboratory_id = l.id
            JOIN object_type t ON o.type_id = t.id
            WHERE 
                (%s = '' OR %s = '-' OR l.lab_name LIKE %s)
            AND 
                (%s = '' OR %s = '-' OR t.type LIKE %s)
            ORDER BY o.id;
        """
        t0 = time.time()
        cur = self.connection.cursor()

        args = [
            lab_like, lab_like, f"%{lab_like}%",
            type_like, type_like, f"%{type_like}%"
        ]

        cur.execute(sql, args)
        rows = cur.fetchall()
        ms = (time.time() - t0) * 1000
        cur.close()
        return rows, ms

    def search_labs(self, rname_like, level, obj_like):
        sql = """
        SELECT DISTINCT
            l.id,
            l.lab_name
        FROM laboratory l
        LEFT JOIN researcher r ON r.laboratory_id = l.id
        LEFT JOIN object o ON o.laboratory_id = l.id
        WHERE
            (%s = '' OR %s = '-' OR r.full_name LIKE %s)
            AND (%s = '' OR %s = '-' OR r.level = %s)
            AND (%s = '' OR %s = '-' OR o.name LIKE %s)
        ORDER BY l.id;
        """

        args = [
            rname_like, rname_like, f"%{rname_like}%",
            level, level, level,
            obj_like, obj_like, f"%{obj_like}%"
        ]

        import time
        t0 = time.time()
        cur = self.connection.cursor()
        cur.execute(sql, args)
        rows = cur.fetchall()
        t = (time.time() - t0) * 1000
        cur.close()

        return rows, t