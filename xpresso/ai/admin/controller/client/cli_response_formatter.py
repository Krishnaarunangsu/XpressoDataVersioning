""" Converts client response into a print statement"""

__all__ = ['CLIResponseFormatter']
__author__ = 'Naveen Sinha'


class CLIResponseFormatter:
    """
    Takes a data and creates string in a specific format
    """
    # Key to print only value not key
    MESSAGE_KEY = "message"

    def __init__(self, data):
        self.data = data

    def format_string(self, input_string: str):
        return input_string.replace("_", " ").title()

    def populate_str_list(self, item, shift=0, string_statement_list=None):
        if not string_statement_list:
            string_statement_list = list()
        if not item:
            return string_statement_list
        if isinstance(item, dict):
            for key, value in item.items():
                if ((isinstance(value, list) or isinstance(value, dict))
                        and key == self.MESSAGE_KEY):
                    string_statement_list.extend(self.populate_str_list(
                        value, shift, None))
                elif isinstance(value, list) or isinstance(value, dict):
                    string_statement_list.append(f"{shift * ' '}"
                                                 f"{self.format_string(key)}:")
                    string_statement_list.extend(self.populate_str_list(
                        value, shift + 2, None))
                elif key == self.MESSAGE_KEY:
                    string_statement_list.append(
                        f"{shift * ' '}{value}")
                else:
                    string_statement_list.append(
                        f"{shift * ' '}{self.format_string(key)}: {value}")
        elif isinstance(item, list):
            temp_list = []
            for value in item:
                if isinstance(value, list) or isinstance(value, dict):
                    temp_list.extend(self.populate_str_list(
                        value, 0, string_statement_list))
                    temp_list.append("\n")
                else:
                    temp_list.append(value)
            line_string = '\n'.join(temp_list)
            string_statement_list.append(
                f"{shift * ' '}{line_string}")
        else:
            string_statement_list.append(f"{shift * ' '}{item}")
        return string_statement_list

    def get_str(self):
        """ Generate print statement"""
        string_statement_list = self.populate_str_list(self.data)
        return '\n'.join(string_statement_list)
