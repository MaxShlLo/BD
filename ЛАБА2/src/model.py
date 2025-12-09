from psycopg2 import connect
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
import time

Base = declarative_base()

from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship


class Laboratory(Base):
    __tablename__ = "laboratory"

    id = Column(Integer, primary_key=True)
    lab_name = Column(String, nullable=False)

    researchers = relationship("Researcher", back_populates="laboratory")
    objects = relationship("Object", back_populates="laboratory")


class Researcher(Base):
    __tablename__ = "researcher"

    id = Column(Integer, primary_key=True)
    full_name = Column(String, nullable=False)
    level = Column(String, nullable=False)

    laboratory_id = Column(Integer, ForeignKey("laboratory.id"))
    laboratory = relationship("Laboratory", back_populates="researchers")


class ObjectType(Base):
    __tablename__ = "object_type"

    id = Column(Integer, primary_key=True)
    type = Column(String, nullable=False)
    galaxy_location = Column(String, nullable=False)

    objects = relationship("Object", back_populates="type")


class Object(Base):
    __tablename__ = "object"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    distance = Column(Integer, nullable=False)

    laboratory_id = Column(Integer, ForeignKey("laboratory.id"))
    type_id = Column(Integer, ForeignKey("object_type.id"))

    laboratory = relationship("Laboratory", back_populates="objects")
    type = relationship("ObjectType", back_populates="objects")


class Model:
    def __init__(self):
        self.engine = create_engine(
            "postgresql+psycopg2://postgres:1234@localhost:5432/postgres"
        )

        Base.metadata.create_all(self.engine)

        Session = sessionmaker(bind=self.engine)
        self.session = Session()

        
        # Додатково:
        self.connection = connect(
            dbname="postgres",
            user="postgres",
            password="1234",
            host="localhost",
            port="5432"
        )

    # ======== BASIC METHODS ========

    def disconnect(self):
        self.session.close()
        if hasattr(self, "connection"):
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
                self.connection.rollback()
                cur.close()
                return 0


    # ======== CRUD OPERATIONS ========

    ## CREATE
    def create_laboratory(self, lab_name):
        lab = Laboratory(lab_name=lab_name)
        self.session.add(lab)
        self.session.commit()

    def create_researcher(self, full_name, level, laboratory_id):
        r = Researcher(
            full_name=full_name,
            level=level,
            laboratory_id=laboratory_id
        )
        self.session.add(r)
        self.session.commit()


    def create_object_type(self, type_name, galaxy_location):
        t = ObjectType(
            type=type_name,
            galaxy_location=galaxy_location
        )
        self.session.add(t)
        self.session.commit()


    def create_object(self, name, distance, laboratory_id, type_id):
        obj = Object(
            name=name,
            distance=distance,
            laboratory_id=laboratory_id,
            type_id=type_id
        )
        self.session.add(obj)
        self.session.commit()
    ## READ
    def read(self, table_name):
        if table_name == "laboratory":
            labs = self.session.query(Laboratory).all()
            return [(l.id, l.lab_name) for l in labs]

        if table_name == "researcher":
            res = self.session.query(Researcher).join(Laboratory).all()
            return [(r.id, r.full_name, r.level, r.laboratory.lab_name) for r in res]

        if table_name == "object_type":
            types = self.session.query(ObjectType).all()
            return [(t.id, t.type, t.galaxy_location) for t in types]

        if table_name == "object":
            objs = self.session.query(Object).join(Laboratory).join(ObjectType).all()
            return [
                (o.id, o.name, o.distance, o.laboratory.lab_name, o.type.type, o.type.galaxy_location)
                for o in objs
            ]
    ## UPDATE
    def update_laboratory_field(self, lab_id, new_name):
        lab = self.session.query(Laboratory).get(lab_id)
        if not lab:
            return 0

        lab.lab_name = new_name
        self.session.commit()
        return 1

    def update_researcher_field(self, researcher_id, field, new_value):
        r = self.session.query(Researcher).get(researcher_id)
        if not r:
            return 0

        if field == "full_name":
            r.full_name = new_value
        elif field == "level":
            r.level = new_value
        elif field == "laboratory_id":
            r.laboratory_id = new_value
        else:
            raise ValueError(f"Unknown field for researcher: {field}")

        self.session.commit()
        return 1

    def update_object_type_field(self, type_id, field, new_value):
        t = self.session.query(ObjectType).get(type_id)
        if not t:
            return 0

        if field == "type":
            t.type = new_value
        elif field == "galaxy_location":
            t.galaxy_location = new_value
        else:
            raise ValueError(f"Unknown field for object_type: {field}")

        self.session.commit()
        return 1


    def update_object_field(self, object_id, field, new_value):
        obj = self.session.query(Object).get(object_id)
        if not obj:
            return 0

        if field == "name":
            obj.name = new_value
        elif field == "distance":
            obj.distance = new_value
        elif field == "laboratory_id":
            obj.laboratory_id = new_value
        elif field == "type_id":
            obj.type_id = new_value
        else:
            raise ValueError(f"Unknown field for object: {field}")

        self.session.commit()
        return 1
    ## DELETE
    def delete(self, table_name, record_id):
        entity_map = {
            "laboratory": Laboratory,
            "researcher": Researcher,
            "object": Object,
            "object_type": ObjectType
        }

        model = entity_map.get(table_name)
        if not model:
            raise ValueError(f"Unknown table: {table_name}")

        obj = self.session.query(model).get(record_id)
        if not obj:
            return 0

        self.session.delete(obj)
        self.session.commit()
        return 1

    # ======== DELETE METHODS ========

    def delete_laboratory(self, lab_id):
        lab = self.session.query(Laboratory).get(lab_id)
        if not lab:
            print(f"[INFO] No laboratory with id={lab_id} nothing deleted.")
            return 0

        self.session.delete(lab)
        self.session.commit()
        print(f"[SUCCESS] Laboratory id={lab_id} deleted successfully.")
        return 1


    def delete_researcher(self, researcher_id):
        r = self.session.query(Researcher).get(researcher_id)
        if not r:
            print(f"[INFO] No researcher with id={researcher_id} nothing deleted.")
            return 0

        self.session.delete(r)
        self.session.commit()
        print(f"[SUCCESS] Researcher id={researcher_id} deleted successfully.")
        return 1


    def delete_object(self, object_id):
        obj = self.session.query(Object).get(object_id)
        if not obj:
            print(f"[INFO] No object with id={object_id} nothing deleted.")
            return 0

        self.session.delete(obj)
        self.session.commit()
        print(f"[SUCCESS] Object id={object_id} deleted successfully.")
        return 1


    def delete_object_type(self, type_id):
        t = self.session.query(ObjectType).get(type_id)
        if not t:
            print(f"[INFO] No object_type with id={type_id} nothing deleted.")
            return 0

        self.session.delete(t)
        self.session.commit()
        print(f"[SUCCESS] Object type id={type_id} deleted successfully.")
        return 1


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