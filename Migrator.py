import sys
from helper.connector import Connector
from helper.config_reader import ConfigReader
from helper.loader import Loader

class Migrator():
    def __init__(self):
        self.config = ConfigReader.load_config()
        connector_obj = Connector(self.config)
        ext_con = connector_obj.establish_connection('EXTRACT')
        com_con = connector_obj.establish_connection('COMMIT')
        self.helper_obj = Loader(ext_con, com_con, self.config['SETTINGS'])
        mode = self.config.get('MODE', 'default')
        if mode == 'new':
            self.transfer()
        elif mode == 'default':
            self.default()

    def transfer(self) -> None:
        print("Starting Migrator.transfer()")
        print("=========================================")
        self.helper_obj.transfer_data()
        print("Finished tranfering data from ElasticSearch to MongoDB")
        print(f"MongoDB Database name: {self.config['COMMIT']['DATABASE']}")
        print(f"Collection name: {self.config['COMMIT']['COLLECTION']}")

    def default(self):
        print("Starting Migrator.default()")
        try:
            stop_mark = self.helper_obj.find_remaining_count()
        except Exception as e:
            # TBD: Handle Exception
            stop_mark = 0

        if stop_mark == 0:
            print ('Transfer Complete!!!')
            sys.exit()

        while stop_mark != 0:
            input_data = self.helper_obj.read_data()
            self.helper_obj.write_data(input_data)
            resume_point = self.helper_obj.find_resume_point()
            stop_mark = self.helper_obj.find_remaining_count()
            print (f'{resume_point} - Partition transfer completed; {stop_mark} - Documents Left')
        print ('Transfer Complete!!!')
        return None

