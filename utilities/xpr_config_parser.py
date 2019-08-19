from configparser import ConfigParser
config = ConfigParser()
config.read('../resources/ConfigFile.properties')

print(config.get('DatabaseSection', 'database.dbname'))