import datetime
import json
import os
import sys
import socket

try:
    import MySQLdb
except ImportError:
    raise Exception('Please install mysqlclient package: https://pypi.org/project/mysqlclient/')

try:
    # python 2
    import urllib2
except ImportError:
    # python 3
    import urllib.request


SETTINGS_FILE_NAME = os.getenv('SETTINGS_FILE_NAME', 'settings.json')
START_PROCESS_LOGS_FROM_DATETIME = os.getenv('START_PROCESS_LOGS_FROM_DATETIME', None)  # example: '2020-01-15 15:58:02'

SSO_API_HOST = os.getenv('SSO_API_HOST', 'sso.host.local')
SSO_API_PROTOCOL = os.getenv('SSO_API_PROTOCOL', 'https')
SSO_API_URL_PATH = os.getenv('SSO_API_URL_PATH', '/api/v1/oauth/users/')
SSO_AUTH_CLIENT_SECRET = os.getenv('SSO_AUTH_CLIENT_SECRET', 'replace-me')
SSO_API_URL = SSO_API_PROTOCOL + '://' + SSO_API_HOST + SSO_API_URL_PATH

EDX_MYSQL_HOST = os.getenv('EDX_MYSQL_HOST', '127.0.0.1')
EDX_MYSQL_DB_NAME = os.getenv('EDX_MYSQL_DB_NAME', 'edxapp')
EDX_MYSQL_PORT = int(os.getenv('EDX_MYSQL_PORT', 3306))
EDX_MYSQL_USERNAME = os.getenv('EDX_MYSQL_USERNAME', 'root')
EDX_MYSQL_PASSWORD = os.getenv('EDX_MYSQL_PASSWORD', '')

LOGS_SOURCE_DIR = os.path.abspath(os.getenv('LOGS_SOURCE_DIR', 'logs'))
LOGS_SOURCE_FILE_EXT = os.getenv('LOGS_SOURCE_FILE_EXT')  # specify logs extension, example: .log
LOGS_RESULT_DIR = os.path.abspath(os.getenv('LOGS_RESULT_DIR', 'logs_result'))

LOG_RESULT_FILE = datetime.datetime.strftime(datetime.datetime.now(), "%Y_%m_%d_%H_%M_%S.log")
USERS_CACHE = {}


def hostname_resolve():
    try:
        socket.gethostbyname(SSO_API_HOST.split(':')[0])
        return True
    except socket.error:
        return False


def get_users_info():
    headers = {'Client-Secret': SSO_AUTH_CLIENT_SECRET}

    if sys.version_info.major == 3:
        req = urllib.request.Request(SSO_API_URL, headers=headers)
        with urllib.request.urlopen(req) as response:
            return response.read()
    else:
        req = urllib2.Request(SSO_API_URL, headers=headers)
        response = urllib2.urlopen(req)
        return response.read()


def get_settings_dict():
    if os.path.exists(SETTINGS_FILE_NAME):
        with open(SETTINGS_FILE_NAME, 'r') as f:
            return json.loads(f.read())
    return {}


def save_settings_dict(config_dict):
    f = open(SETTINGS_FILE_NAME, 'w')
    f.write(json.dumps(config_dict))
    f.close()


def process_log_line(line, users_info, db_cur):
    line = line.strip()
    if not line:
        return False

    data = json.loads(line)
    username = data.get('username')

    if username:
        user_email = USERS_CACHE.get(username)
        if not user_email:
            db_cur.execute("""SELECT email FROM auth_user WHERE username = %s""", (username,))
            res = db_cur.fetchone()
            user_email = res[0] if res else None
            USERS_CACHE[username] = user_email
        if user_email and user_email in users_info:
            data['user_email'] = user_email
            data['user_unti_id'] = users_info[user_email]
            return json.dumps(data)
    return False


def process_log_file(file_path, config_dict, users_info, db_cur):
    process = True
    start_from_line = 0
    log_file_size = os.path.getsize(file_path)
    file_id = file_path[len(LOGS_SOURCE_DIR):]

    if file_id in config_dict:
        cache_file_size, start_from_line = config_dict[file_id]
        if cache_file_size >= log_file_size:
            process = False

    if process:
        print("Process " + file_path + " from line num " + str(start_from_line))
        line_num = 0
        with open(os.path.join(LOGS_RESULT_DIR, LOG_RESULT_FILE), 'a') as fp_res:
            with open(file_path, 'r') as fp_src:
                for line in fp_src:
                    if line_num >= start_from_line:
                        updated_log_line = process_log_line(line, users_info, db_cur)
                        if updated_log_line:
                            fp_res.write(updated_log_line + "\n")
                    line_num += 1

        config_dict[file_id] = [log_file_size, line_num]
        save_settings_dict(config_dict)


def run():
    start_process_logs_from = None
    if START_PROCESS_LOGS_FROM_DATETIME:
        start_process_logs_from = datetime.datetime.strptime(START_PROCESS_LOGS_FROM_DATETIME, "%Y-%m-%d %H:%M:%S")

    if not hostname_resolve():
        raise Exception("Can't resolve SSO hostname")

    db = MySQLdb.connect(host=EDX_MYSQL_HOST, port=EDX_MYSQL_PORT, db=EDX_MYSQL_DB_NAME,
                         user=EDX_MYSQL_USERNAME, passwd=EDX_MYSQL_PASSWORD, charset='utf8')
    db_cur = db.cursor()

    # check auth_user table exists
    db_cur.execute("""DESCRIBE auth_user""")
    db_cur.fetchall()

    if not os.path.isdir(LOGS_SOURCE_DIR):
        raise Exception("Source logs directory doesn't exist")

    config_dict = get_settings_dict()
    users_info_str = get_users_info()

    try:
        users_info = json.loads(users_info_str)
    except ValueError:
        raise Exception("SSO API response is not valid JSON")

    if not os.path.isdir(LOGS_RESULT_DIR):
        os.makedirs(LOGS_RESULT_DIR)

    for root_dir, subdirs, files in sorted(os.walk(LOGS_SOURCE_DIR)):
        for file_path_part in files:
            if LOGS_SOURCE_FILE_EXT and not file_path_part.endswith(LOGS_SOURCE_FILE_EXT):
                continue
            file_path = os.path.join(LOGS_SOURCE_DIR, root_dir, file_path_part)
            if not start_process_logs_from \
                    or start_process_logs_from < datetime.datetime.fromtimestamp(os.path.getmtime(file_path)):
                process_log_file(file_path, config_dict, users_info, db_cur)


if __name__ == "__main__":
    run()

