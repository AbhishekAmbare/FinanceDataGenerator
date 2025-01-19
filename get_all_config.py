import configparser


def get_db_config_by_role(logintype: str):
    
    config = configparser.ConfigParser()

    config.read('db_config.config')
    
    config_type = 'readonly'
    
    if logintype == 'admin':
        config_type = 'admin'
    elif logintype == 'write':
        config_type = 'write'
    else:
        config_type = 'readonly'
    
    user_config = config['userconfig'][config_type]
    
    return {
        "database": config[user_config]['database'],
        "user": config[user_config]['user'],
        "password": config[user_config]['password'],
        "host": config[user_config]['host'],
        "port": config[user_config]['port']
        }
    
def get_filepath_by_mode(mode: str):
    
    config = configparser.ConfigParser()

    config.read('path_config.config')
    
    mode_type = 'read-raw'
    
    if mode == 'write':
        mode_type = 'write-raw'
    
    return {
        "filepath":config[mode_type]['datapath'],
        "master":config['master']['masterpath']
        }