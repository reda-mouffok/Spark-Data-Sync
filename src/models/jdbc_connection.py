from src.data_tools import data_tools
from src.data_tools.data_tools import get_dict_value


class JDBCConnection:

    def __init__(self, file_path):
        self.dict = data_tools.read_yaml(file_path)
        self.check_argument()

    def check_argument(self):

        for connection, connection_attribute in self.dict.items():
            if not get_dict_value(connection_attribute, "url"):
                raise Exception("JDBC source : " + connection + " is not provided with a url")
            if not get_dict_value(connection_attribute, "user"):
                raise Exception("JDBC source : " + connection + " is not provided with a user")
            if not get_dict_value(connection_attribute, "password"):
                raise Exception("JDBC source : " + connection + " is not provided with a password")

    def get_url(self, jdbc_type):
        dict_jdbc = get_dict_value(self.dict, jdbc_type)
        if dict_jdbc:
            return get_dict_value(dict_jdbc, "url")
        else:
            raise Exception("JDBC source " + jdbc_type + " is not provided on connection file")

    def get_user(self, jdbc_type):
        dict_jdbc = get_dict_value(self.dict, jdbc_type)
        if dict_jdbc:
            return get_dict_value(dict_jdbc, "user")
        else:
            raise Exception("JDBC source " + jdbc_type + " is not provided on connection file")

    def get_password(self, jdbc_type):
        dict_jdbc = get_dict_value(self.dict, jdbc_type)
        if dict_jdbc:
            return get_dict_value(dict_jdbc, "password")
        else:
            raise Exception("JDBC source " + jdbc_type + " is not provided on connection file")

    def get_driver_type(self, jdbc_type):
        if jdbc_type == 'postgres':
            return "org.postgresql.Driver"
        elif jdbc_type == 'oracle':
            return "oracle.jdbc.driver.OracleDriver"
        else:
            raise Exception("JDBC source " + jdbc_type + " is not provided on connection file")
