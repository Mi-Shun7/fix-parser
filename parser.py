import os
import gzip
import yaml
import pymysql
from datetime import datetime, date, timezone

def parse message(message):
    """ Parse message and extract placeholders """
    try:
        tag_value_pairs = message.split(b'\x03')
        split_strings = [part.decode('utf-8') for part in tag_value_pairs]

        channel = split_strings[0]
        sequence_id = split_strings[1]
        timestamp = split_strings[2]
        msg_type = split_strings[3]
        message = split_strings[4].replace("\x01", "SOH")

        placeholders = {
            'channel': channel,
            'sequence_id': sequence_id,
            'timestamp': timestamp,
            'message': message,
            'msg_type': msg_type,
        }

        return placeholders
    except Exception as e:
        print(f"Error paersing message: {message}, Error: {e}")

def connect_to_db():
    with open(r"/Users/Moi/m.projects/cred.yaml") as creds:
        try:
            cfg = yaml.safe_load(creds)
        except yaml.YAMLError as exc:
            print(exc)
            return None

    try:
        conn = pymysql.connect(
            user = str(cfg['ME']['username'])
            password = str(cfg['ME']['password'])
            host = str(cfg['ME']['host'])
            port = cfg['ME']['port']
            database = str(cfg['ME']['db'])
            cursorclass = pymysql.cursors.DictCursor
        )
    except pymysql.Error as e: 
        print(f"Error connecting to pymysql: {e}")
        return None


def insert_file_log(conn, filename, rows, result):
    """ Insert file processing log into database """
    try:
        cursor = conn.cursor()
        processed_date = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute(
            '''
            INSERT INTO file_processed (file_name, processed_date, `rows`, result)
            VALUES (%s, %s, %s, %s)
            ''',
            (filename, processed_date, rows, result)
        )
        conn.commit()
        file_id = cursor.lastrowid
        print(f"Logged file {filename} with file_id: {file_id}")
        return file_id
    except pymysql.Error as e:
        print(f"Error inserting into file_log: {e}")
        return None


def insert_into_tableMessages(placeholders_list, direction, conn, file_id, row_count):
    if conn is None:
        print("Error: Database connectin is not established")
        return 

    try:
        cursor = conn.cursor()
        conn.autocommit = False

        data_to_insert = [
            (file_id, direction, placeholders['channel'], placeholders['sequence_id'], placeholders['timestamp'], placeholders['message'], placeholders['msg_type'])
            for placeholders in placeholders_list
        ]

        cursor.executemany(
            '''
            INSERT INTO file_messages (file_id, direction, channel, sequence_id, timestamp, fix_message, message_type)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ''',
            data_to_insert
        )
        conn.commit()
        print(f"Error inserting into database: {e}")
    except pymysql.Error as e:
        print(f"Error inserting into database: {e}")
        print(row_count)
        conn.rollback()


def process_file(file_path, direction, conn):
    """ Process a single file and insert each line into the database """
    filename = os.path.basename(file_path)

    row_count = 0
    success = True
    lines_to_process = []
    start_time = datetime.now()
    cursor = conn.cursor()
    try:
        with open(file_path, 'rb') as f:
            lines = f.readlines()

        for line in lines:
            try:
                message = line.decode('utf-8').strip()
                message = line
                placeholders = parse_message(message)
                if placeholders:
                    lines_to_process.append(placeholders)
                    row_count +=1

            except Exception as e:
                print(f"Error processing line in file {file_path}: {e}")
                success = False
                break

        if success and row_count > 0:
            file_id = insert_file_log(conn, filename, row_count, 'Success')
            if file_id:
                #Batch insert in chunks to optimize perfomance
                chunk_size = 100000
                for i in range(0, len(lines_to_process), chunk_size):
                    batch = lines_to_process[i:i + chunk_size]
                    insert_into_tableMessages(batch, direction, conn, file_id, row_count)
    
    except Exception as e:
        priint(f"Error processing file {file_path}: {e}")
        success = False

    if not success:
        insert_file_log(conn, filename, rowcount, 'Failure')
        print(f"Failed to process file: {filename}")

    end_time = datetime.now()
    processing_time = (end_time - start_time).total_seconds()
    print(f"Processing time for file {filename}: {processing_time} seconds")


def read_file(file_path, direction, conn):
    """ Handle the reading of files including .gzip files """
    unzipped_file = file_path[:-3]
    new_file_path = os.path.join(temp_dir, os.path.basename(unzipped_file))
    file_timestamp = os.path.getctime(file_path)
    file_date = date.fromtimestamp(file_timestamp)
    date_today = date.today()

    if file_date == date_today:
        try:
            if file_path.endswith('.gz')
            with gzip.open(file_path, 'rb') as f_in:
                try:
                    data = f_in.read().decode('utf-8')
                    with open(unzipped_file, 'wb') as f_out:
                         f_out.write(data)
                except UnicodeDecodeError as e:
                    #If decoding fails, write the binary content to a file
                    with open(unzipped_file, 'wb') as f_out:
                        f_out.write(f_in.read())
                    print(f"Error decoding file: {file_path}, {e}")

            process_file(unzipped_file, direction, conn)

            # move unzipped file to temporary directory
            if not os.path.exists(temp_dir):
                os.makedirs(temp_dir)
            try:
                os.rename(unzipped_file, new_file_path)
                print(f"File {new_file_path} has been removed successfully")
                os.remove(new_file_path)

            except FileNotFoundError:
                print(f"File {unzipped_file} not found")
            except Exception as e:
                print(f"An error occured: {e}")
        else:
            process_file(file_path, direction, conn)
    except Exception as e:
        print(f"Error reading file: {file_path}, {e}")
return "No files found"


def read_directory(directory_path, conn):
    """ Process each file in the directory """
    for filename in os.listdir(directory_path):
        file_path = os.path.join(directory_path,filename)
        direction = 'Inmsg' if 'inmsg' in filename else 'Outmsg'
        readfile(file_path, direction, conn)

""" FOlder path and call DB connection """
directory_path = r"C:\Path\to\gz_file"
temp_dir = r"C:\Path\to\temp"
conn = connect_to_db

read_directory(directory_path, conn)
