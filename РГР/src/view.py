from typing import Callable, Union
from tabulate import tabulate


class View:

    def __init__(self):
        self.available_commands_menus: dict = {
            "create": self.show_menu_create,
            "read": self.show_menu_read,
            "update": self.show_menu_update,
            "delete": self.show_menu_delete,
            "task_2": self.show_task2_menu,
            "task_3": self.show_task3_menu,
            "quit": None,
        }

        self.available_create: dict = {
            "laboratory": self.show_create_laboratory,
            "researcher": self.show_create_researcher,
            "object": self.show_create_object,
            "object_type": self.show_create_object_type,
        }

        self.available_read: dict = {
            "laboratories": self.show_read_laboratories,
            "researchers": self.show_read_researchers,
            "objects": self.show_read_objects,
            "object_types": self.show_read_object_types,
        }

        self.available_update: dict = {
            "laboratory": self.show_update_laboratory,
            "researcher": self.show_update_researcher,
            "object": self.show_update_object,
            "object_type": self.show_update_object_type,
        }

        self.available_delete: dict = {
            "laboratory": self.show_delete_laboratory,
            "researcher": self.show_delete_researcher,
            "object": self.show_delete_object,
            "object_type": self.show_delete_object_type,
        }

        self.available_task2: dict = {
            "generate_labs": self.show_task2_generate_labs,
            "generate_researchers": self.show_task2_generate_researchers,
            "generate_objects": self.show_task2_generate_objects,
            "generate_object_types": self.show_task2_generate_object_types,
        }

        self.available_task3: dict = {
            "search_researchers": self.show_task3_search_researchers,
            "search_objects": self.show_task3_search_objects,
            "search_labs": self.show_task3_search_labs,
        }

        self.table_headers: dict = {
            "laboratories": ("id", "lab_name"),
            "researchers": ("id", "full_name", "level", "laboratory_id"),
            "objects": ("id", "name", "distance", "laboratory_id", "type", "galaxy_location"),
            "object_types": ("id", "type", "galaxy_location"),
            
        }

    # ----------- TABLE OUTPUT -----------

    def output_table(self, table, table_name):
        print("\n\n")
        print(
            tabulate(
                [[field.strip() if isinstance(field, str) else field for field in row] for row in table],
                headers=self.table_headers[table_name]
            )
        )

    @staticmethod
    def output_error_message():
        print("!Incorrect input!")

    # ----------- MENU UTILS -----------

    @staticmethod
    def _output_options(options_dict: dict, amount_of_tabs: int, title: str) -> None:
        options = tuple(options_dict.keys())
        tab_string = "\t" * amount_of_tabs
        print(f"\n\n{tab_string}{title}:\n")
        for index, option in enumerate(options):
            print(f"{tab_string}{index + 1}. {option}\n")

    @staticmethod
    def _handle_wrong_input(options: dict) -> Union[Callable, str]:
        while True:
            try:
                keys = tuple(options.keys())
                option = keys[int(input("Input number of the option: ").strip()) - 1]
                return options[option]
            except (IndexError, ValueError):
                print("There is no such option, please try again")

    @staticmethod
    def _get_key_by_value(dct: dict, value):
        keys = tuple(dct.keys())
        values = tuple(dct.values())
        index = values.index(value)
        return keys[index]

    # ----------- MAIN MENUS -----------

    def show_menu(self) -> tuple[Callable, str]:
        self._output_options(
            self.available_commands_menus,
            amount_of_tabs=0,
            title="Select the actions below"
        )
        response = self._handle_wrong_input(self.available_commands_menus)
        return response, self._get_key_by_value(self.available_commands_menus, response)

    
    # ----------- CREATE -----------

    def show_menu_create(self) -> tuple[Callable, str]:
        self._output_options(
            self.available_create,
            amount_of_tabs=1,
            title="Choose what do you want to create"
        )
        response = self._handle_wrong_input(self.available_create)
        return response, self._get_key_by_value(self.available_create, response)

    @staticmethod
    def show_create_laboratory():
        lab_name = input("Enter laboratory name: ")
        return lab_name

    @staticmethod
    def show_create_researcher():
        full_name = input("Enter researcher full_name: ")
        level = input("Enter researcher level: ")
        lab_id = input("Enter laboratory ID: ")
        return full_name, level, lab_id

    @staticmethod
    def show_create_object():
        name = input("Enter object name: ")
        distance = input("Enter distance: ")
        lab_id = input("Enter laboratory ID: ")
        type_id = input("Enter object_type ID: ")
        return name, distance, lab_id, type_id

    @staticmethod
    def show_create_object_type():
        type_name = input("Enter object_type: ")
        galaxy_location = input("Enter galaxy_location: ")
        return type_name, galaxy_location

    # ----------- READ -----------

    def show_menu_read(self):
        self._output_options(
            self.available_read,
            amount_of_tabs=1,
            title="Choose what do you want to read"
        )
        response = self._handle_wrong_input(self.available_read)
        return response, self._get_key_by_value(self.available_read, response)

    @staticmethod
    def show_read_laboratories():
        return "laboratories"

    @staticmethod
    def show_read_researchers():
        return "researchers"

    @staticmethod
    def show_read_objects():
        return "objects"

    @staticmethod
    def show_read_object_types():
        return "object_types"

    # ----------- UPDATE -----------

    def show_menu_update(self):
        self._output_options(
            self.available_update,
            amount_of_tabs=1,
            title="Choose what do you want to update"
        )
        response = self._handle_wrong_input(self.available_update)
        return response, self._get_key_by_value(self.available_update, response)

    def show_update_laboratory(self):
        lab_name = input("Enter current id: ")
        new_value = input("Enter new name: ")
        return lab_name, new_value

    def show_update_researcher(self):
        name = input("Enter researcher id: ")
        change_options = {
            "change_full_name": "full_name",
            "change_level": "level",
            "change_laboratory": "laboratory_id"
        }
        self._output_options(change_options, 2, "Choose what to change")
        response = self._handle_wrong_input(change_options)
        new_value = input("Enter new value: ")
        return name, response, new_value

    def show_update_object(self):
        obj_name = input("Enter object id: ")
        change_options = {
            "change_name": "name",
            "change_distance": "distance",
            "change_lab": "laboratory_id",
            "change_type": "type_id"
        }
        self._output_options(change_options, 2, "Choose what to change")
        response = self._handle_wrong_input(change_options)
        new_value = input("Enter new value: ")
        return obj_name, response, new_value

    def show_update_object_type(self):
        type_name = input("Enter object_type id: ")
        change_options = {
            "change_type": "type",
            "change_location": "galaxy_location"
        }
        self._output_options(change_options, 2, "Choose what to change")
        response = self._handle_wrong_input(change_options)
        new_value = input("Enter new value: ")
        return type_name, response, new_value


    # ----------- DELETE -----------

    def show_menu_delete(self):
        self._output_options(
            self.available_delete,
            amount_of_tabs=1,
            title="Choose what do you want to delete"
        )
        response = self._handle_wrong_input(self.available_delete)
        return response, self._get_key_by_value(self.available_delete, response)

    @staticmethod
    def show_delete_laboratory():
        lab_name = input("Enter laboratory id: ")
        return lab_name

    @staticmethod
    def show_delete_researcher():
        name = input("Enter researcher id: ")
        return name

    @staticmethod
    def show_delete_object():
        name = input("Enter object id: ")
        return name

    @staticmethod
    def show_delete_object_type():
        type_name = input("Enter object_type id: ")
        return type_name


    def show_task2_menu(self) -> tuple[Callable, str]:
        """
        Task 2 menu - returns (viewer_callable, key)
        Viewer callable when called should return the args (here: number n).
        """
        self._output_options(
            self.available_task2,
            amount_of_tabs=1,
            title="Task 2: choose what to generate"
        )
        response = self._handle_wrong_input(self.available_task2)
        return response, self._get_key_by_value(self.available_task2, response)

    # viewer functions for each task2 option:
    @staticmethod
    def show_task2_generate_labs():
        while True:
            try:
                n = int(input("Enter number of laboratory records to generate: ").strip())
                assert n > 0
                return n
            except (ValueError, AssertionError):
                print("Please input a positive integer.")

    @staticmethod
    def show_task2_generate_researchers():
        while True:
            try:
                n = int(input("Enter number of researcher records to generate: ").strip())
                assert n > 0
                return n
            except (ValueError, AssertionError):
                print("Please input a positive integer.")

    @staticmethod
    def show_task2_generate_objects():
        while True:
            try:
                n = int(input("Enter number of object records to generate: ").strip())
                assert n > 0
                return n
            except (ValueError, AssertionError):
                print("Please input a positive integer.")


    @staticmethod
    def show_task2_generate_object_types():
        while True:
            try:
                n = int(input("Enter number of object_type records to generate: ").strip())
                assert n > 0
                return n
            except (ValueError, AssertionError):
                print("Please input a positive integer.")

    def show_task3_menu(self):
        self._output_options(
            self.available_task3,
            amount_of_tabs=1,
            title="Task 3: choose search query"
        )
        response = self._handle_wrong_input(self.available_task3)
        return response, self._get_key_by_value(self.available_task3, response)

    @staticmethod
    def show_task3_search_researchers():
        lab = input("Enter laboratory name pattern (LIKE): ")
        level = input("Enter researcher level (Junior/Middle/Senior/Lead) or '-' for all: ")
        return lab, level

    @staticmethod
    def show_task3_search_objects():
        lab = input("Enter laboratory name pattern (LIKE): ")
        type_name = input("Enter object type pattern (LIKE): ")
        return lab, type_name

    @staticmethod
    def show_task3_search_labs():
        rname = input("Enter researcher name (LIKE) or '-' for all: ").strip()
        level = input("Enter researcher level (Junior/Middle/Senior/Lead) or '-' for all: ").strip()
        obj_name = input("Enter object name (LIKE) or '-' for all: ").strip()
        return rname, level, obj_name