import pickle
import pandas as pd

class SerializationDeserializationUtil:
    """
    Serializes & Deserializes an object to be transacted over the network


    """
    def __init__(self, data_to_be_serialized:dict):
        """
        Args:
            :param data_to_be_serialized:
        """
        self.data_to_be_serialized = data_to_be_serialized


    def store_data(self):
        """
        Initializing the data to be stored in database
        Args:

        Return:

        :return:
        """
        #database
        db = {}
        db['data_col_1'] = self.data_to_be_serialized
        db_file = ""


        # Its important to use binary mode
        db_file = open('examplePickle', 'ab')

        # source, destination
        pickle.dump(db, db_file)
        db_file.close()

        a = {'hello': 'world'}

        with open('filename3.pickle', 'ab') as handle:
            pickle.dump(a, handle)


        with open("dataset_new", 'ab') as db_file:
            pickle.dump(db, db_file, protocol=pickle.HIGHEST_PROTOCOL)

        
    def load_data(self) -> object:
        """
        for reading also binary mode is important
        :return:
        """
        db = ""
        with open('dataset_new', 'rb') as db_file:
            db = pickle.load(db_file)
            print(db)

        for keys in db:
            print(keys,'->',db[keys])



if __name__ =="__main__":
    dict_data_1: dict = {'key' : 'Omkar', 'name' : 'Omkar Pathak',
    'age' : 21, 'pay' : 40000}
    #


    df_age_count = pd.read_csv('age_count.csv')
    serialization_1 = SerializationDeserializationUtil(df_age_count)
    serialization_1.store_data()
    serialization_1.load_data()
