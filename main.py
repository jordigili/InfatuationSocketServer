import socket
import time
from io import StringIO
import pandas as pd
import numpy as np
import time

HOST = "event_server"
EVENT_LISTENER_PORT = 9099
EVENT_SOURCE_PORT = 9090
BUFFER_SIZE = 1024
start = time.time()
retry = 3
sleep_time = 1


def connect(s, port):
    connected = False
    current = 0
    while not connected and current < retry:
        try:
            s.connect((HOST, port))
            connected = True
        except socket.error:
            time.sleep(3)
            current = current + 1
    if current >= retry:
        raise Exception("Connection to Event Source Failed")

    return s

def get_matches():
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s = connect(s, EVENT_SOURCE_PORT)
            print('Start retrieving from Source', time.time - start)
            data = s.recv(BUFFER_SIZE)
            like_string = data.decode("utf-8")
            buffering = True
            while buffering:
                data = s.recv(BUFFER_SIZE)
                like_string += data.decode("utf-8")
                if 'EVENT END' in data.decode("utf-8"):
                    print('Finish retrieving from Source', time.time - start)
                    buffering = False

            # this is done to give a name to pandas dataframe columns
            like_string = like_string.replace("EVENT BEGIN", "sequence|type|from_user|to_user");
            # removing this as it is not needed
            like_string = like_string.replace("EVENT END", "");
            # converting to csv format so we can import it to pandas
            like_string = StringIO(like_string)
            df = pd.read_csv(like_string, sep="|")
            df = df.sort_values('sequence')

            # we only care about likes
            onlyLike = df[df.type == 'LIKE_LIKED']

            print('Performing join on likes', time.time - start)
            # we join twice on the dataframe to find the matches
            match_dataframe = pd.merge(onlyLike, onlyLike, how="inner", left_on=['from_user', 'to_user'],
                                       right_on=['to_user', 'from_user'])

            # because of the join we have duplicates, the highest sequence represents when the match occurred
            match_dataframe["result"] = np.where(match_dataframe['sequence_y'] > match_dataframe['sequence_x'],
                                                 match_dataframe['sequence_y'], match_dataframe['sequence_x'])

            # making sure we get the results on the right order
            match_dataframe = match_dataframe.sort_values('result')
            # remove duplicates
            result = match_dataframe.result.unique()

            print("Found ", len(result), " matches", time.time - start)

    except ImportError as e:
        print("Error conversion to csv: ", e)
    except pd.errors.ParserError as e:
        print("Error parsing data frame: ", e)
    except BaseException as err:
        print("Unclassified error", err)
    finally:
        s.close()

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            connect(s, EVENT_LISTENER_PORT)
            print("Sending results matches to listener", time.time - start)
            print(s.recv(BUFFER_SIZE))
            response = ''
            for index, match in enumerate(result, start=0):
                response += str(match) + '\n'
            s.send(bytearray(response, "utf-8"))
            print(s.recv(BUFFER_SIZE))
            s.close()
    except BaseException as err:
        print("Unclassified error", err)
    finally:
        s.close()


if __name__ == '__main__':
    print('Match finder started', )
    get_matches()
    print('Match finder finalized', time.time - start)