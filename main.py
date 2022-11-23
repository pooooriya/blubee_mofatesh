import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import os
import psycopg2
import dask.dataframe as dd


class Watcher:
    DIRECTORY_TO_WATCH = "/home"

    def __init__(self):
        self.observer = Observer()

    def run(self):
        event_handler = Handler()
        self.observer.schedule(
            event_handler, self.DIRECTORY_TO_WATCH, recursive=True)
        self.observer.start()
        try:
            while True:
                time.sleep(5)
        except:
            self.observer.stop()
            print("Error")

        self.observer.join()


class Handler(FileSystemEventHandler):
    @staticmethod
    def on_any_event(event):
        DIRECTORY_TO_WATCH = "/home"
        if event.is_directory:
            return None

        elif event.event_type == 'created':
            # Take any action here when a file is first created.4
            filename, file_extension = os.path.splitext(event.src_path)
            print(file_extension)
            if file_extension == '.csv':
                dataframe = dd.read_csv(os.getcwd()+event.src_path)
                dataframe.columns = [x.lower().replace(" ", "_").replace("-", "_").replace(r"/", "_").replace(
                    "\\", "_").replace(".", "_").replace("$", "").replace("%", "") for x in dataframe.columns]
                replacements = {
                    'timedelta64[ns]': 'varchar',
                    'object': 'varchar',
                    'float64': 'float',
                    'int64': 'int',
                    'datetime64': 'timestamp'
                }
                col_str = ", ".join("{} {}".format(n, d) for (n, d) in zip(
                    dataframe.columns, dataframe.dtypes.replace(replacements)))
                conn_string = "postgres://postgres:postgrespw@localhost:49153"
                conn = psycopg2.connect(conn_string)
                cursor = conn.cursor()
                print('opened database successfully')
                cursor.execute("CREATE TABLE IF NOT EXISTS %s (%s);" %
                               ("blubee_gooni", col_str))
                print('{0} was created or mount successfully'.format(
                    "blubee_gooni"))
                my_file = open(os.getcwd()+filename+file_extension)
                print('file opened in memory')
                SQL_STATEMENT = """
                COPY blubee_gooni FROM STDIN WITH
                    CSV
                    HEADER
                    DELIMITER AS ','
                """
                cursor.copy_expert(sql=SQL_STATEMENT, file=my_file)
                print('file copied to db')
                cursor.execute("grant select on table blubee_gooni to public")
                conn.commit()
                cursor.close()
                print('table blubee_gooni imported to db completed')


if __name__ == '__main__':
    w = Watcher()
    w.run()
