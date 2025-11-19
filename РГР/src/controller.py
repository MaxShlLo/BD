from .model import Model
from .view import View
from functools import wraps
from psycopg2.errors import StringDataRightTruncation


def catch_db_error(option):
    @wraps(option)
    def inner(self, *args, **kwargs):
        try:
            option(self, *args, **kwargs)
        except (IndexError, StringDataRightTruncation, ValueError, AssertionError) as e:
            print(f"\n Known DB error: {type(e).__name__} — {e}\n")
            self.view.output_error_message()
        except Exception as e:
            print(f"\n Unexpected error in {option.__name__}: {type(e).__name__} — {e}\n")
            self.view.output_error_message()
    return inner


class Controller:
    def __init__(self):
        self.available = {
            "create": {
                "laboratory": self.create_laboratory,
                "researcher": self.create_researcher,
                "object": self.create_object,
                "object_type": self.create_object_type,
            },
            "read": {
                "laboratories": self.read,
                "researchers": self.read,
                "objects": self.read,
                "object_types": self.read,
            },
            "update": {
                "laboratory": self.update_laboratory,
                "researcher": self.update_researcher,
                "object": self.update_object,
                "object_type": self.update_object_type,
            },
            "delete": {
                "laboratory": self.delete_laboratory,
                "researcher": self.delete_researcher,
                "object": self.delete_object,
                "object_type": self.delete_object_type,
            },
            "task_2": {
                "generate_labs": self.task_generate_labs,
                "generate_researchers": self.task_generate_researchers,
                "generate_objects": self.task_generate_objects,
                "generate_object_types": self.task_generate_object_types,
            },
            "task_3": {
                "search_researchers": self.task3_search_researchers,
                "search_objects": self.task3_search_objects,
                "search_labs": self.task3_search_labs,
            },
        }
        self.model = Model()
        self.view = View()

    def run(self):
        while True:
            chosen_mode_viewer, chosen_mode = self.view.show_menu()
            if not chosen_mode_viewer:
                self.model.disconnect()
                break
            chosen_option_viewer, chosen_option = chosen_mode_viewer()
            args_or_command = chosen_option_viewer()


            self.available[chosen_mode][chosen_option](args_or_command)

    # --- CREATE OPERATIONS ---
    @catch_db_error
    def create_laboratory(self, args):
        lab_name = args if isinstance(args, str) else args[0]
        self.model.create_laboratory(lab_name)

    @catch_db_error
    def create_researcher(self, args):
        full_name, level, lab_name = args
        self.model.create_researcher(full_name, level, lab_name)

    @catch_db_error
    def create_object(self, args):
        name, distance, lab_name, type_name = args
        self.model.create_object(name, distance, lab_name, type_name)

    @catch_db_error
    def create_object_type(self, args):
        type_name, galaxy_location = args
        self.model.create_object_type(type_name, galaxy_location)

    # --- READ ---
    def read(self, read_from):
    # Зберігаємо оригінальне ім'я (для заголовків)
        original_name = read_from

        # Перетворюємо множину на однину — тільки для SQL
        if read_from == "laboratories":
            read_from = "laboratory"
        elif read_from == "researchers":
            read_from = "researcher"
        elif read_from == "objects":
            read_from = "object"
        elif read_from == "object_types":
            read_from = "object_type"

        # Отримуємо дані з бази
        table = self.model.read(read_from)

        # Передаємо назад оригінальне ім’я для коректного заголовка
        self.view.output_table(table, original_name)

    # --- UPDATE ---
    @catch_db_error
    def update_laboratory(self, args):
        find_name, new_name = args
        #self.model.update_laboratory(find_name, new_name)

        affected = self.model.update_laboratory_field(find_name, new_name)

        if affected == 0:
            print(f"[!INFO!] NO LABORATORY WITH ID={find_name} — nothing was updated.")
        else:
            print(f"[SUCCESS] Laboratory id={find_name} updated successfully (new name: {new_name})")
   
    @catch_db_error
    def update_researcher(self, args):
        researcher_id, what_to_change, new_value = args

        field_map = {
            "full_name": "full_name",
            "level": "level",
            "laboratory_id": "laboratory_id",
        }

        field = field_map.get(what_to_change)
        if not field:
            raise ValueError(f"Unknown field: {what_to_change}")

        if field == "laboratory_id":
            new_value = int(new_value)

        affected = self.model.update_researcher_field(researcher_id, field, new_value)

        if affected == 0:
            print(f"[INFO] No researcher with id={researcher_id} — nothing was updated.")
        else:
            print(f"[SUCCESS] Researcher id={researcher_id} updated: set {field} = {new_value}")


    @catch_db_error
    def update_object(self, args):
        object_id, what_to_change, new_value = args

        field_map = {
            "name": "name",
            "distance": "distance",
            "laboratory_id": "laboratory_id",
            "type_id": "type_id",
        }

        field = field_map.get(what_to_change)
        if not field:
            raise ValueError(f"Unknown field: {what_to_change}")

       
        affected = self.model.update_object_field(object_id, field, new_value)

        if affected == 0:
            print(f"[INFO] No object with id={object_id} — nothing was updated.")
        else:
            print(f"[SUCCESS] Object id={object_id} updated: set {field} = {new_value}")


    @catch_db_error
    def update_object_type(self, args):
        type_id, what_to_change, new_value = args

        field_map = {
            "type": "type",
            "galaxy_location": "galaxy_location",
        }

        field = field_map.get(what_to_change)
        if not field:
            raise ValueError(f"Unknown field: {what_to_change}")

        affected = self.model.update_object_type_field(type_id, field, new_value)

        if affected == 0:
            print(f"[INFO] No object_type with id={type_id} — nothing was updated.")
        else:
            print(f"[SUCCESS] Object type id={type_id} updated: set {field} = {new_value}")

    # --- DELETE ---
    @catch_db_error
    def delete_laboratory(self, name):
        self.model.delete_laboratory(name)

    @catch_db_error
    def delete_researcher(self, name):
        self.model.delete_researcher(name)

    @catch_db_error
    def delete_object(self, name):
        self.model.delete_object(name)

    @catch_db_error
    def delete_object_type(self, name):
        self.model.delete_object_type(name)

    @catch_db_error
    def task_generate_labs(self, args):
        # args is n
        n = int(args)
        print(f"[TASK2] Generating {n} laboratories...")
        created = self.model.generate_laboratories(n)
        print(f"[TASK2] Laboratories inserted (approx): {created}")

    @catch_db_error
    def task_generate_researchers(self, args):
        n = int(args)
        print(f"[TASK2] Generating {n} researchers...")
        created = self.model.generate_researchers(n)
        print(f"[TASK2] Researchers inserted (approx): {created}")

    @catch_db_error
    def task_generate_objects(self, args):
        n = int(args)
        print(f"[TASK2] Generating {n} objects...")
        created = self.model.generate_objects(n)
        print(f"[TASK2] Objects inserted (approx): {created}")

    @catch_db_error
    def task_generate_object_types(self, args):
        n = int(args)
        print(f"[TASK2] Generating {n} objects...")
        created = self.model.generate_object_types(n)
        print(f"[TASK2] Objects inserted (approx): {created}")

    @catch_db_error
    def task3_search_researchers(self, args):
        table, ms = self.model.search_researchers(*args)
        self.view.output_table(table, "researchers")
        print(f"[TIME] Query executed in {ms:.2f} ms")

    @catch_db_error
    def task3_search_objects(self, args):
        table, ms = self.model.search_objects(*args)
        self.view.output_table(table, "objects")
        print(f"[TIME] Query executed in {ms:.2f} ms")

    @catch_db_error
    def task3_search_labs(self, args):
        table, ms = self.model.search_labs(*args)

        if not table:
            print("[INFO] No labs match your filters.")
        else:
            self.view.output_table(table, "laboratories")

        print(f"[TIME] Query executed in {ms:.3f} ms")