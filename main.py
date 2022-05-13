import socket
from io import StringIO
import pandas as pd
import numpy as np

HOST = "127.0.0.1"
PORT = 9090


def get_matches():
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((HOST, PORT))
            print('Start retrieving from Source')
            data = s.recv(1024)
            like_string = data.decode("utf-8")
            buffering = True
            while buffering:
                data = s.recv(1024)
                like_string += data.decode("utf-8")
                if 'EVENT END' in data.decode("utf-8"):
                    print('Finish retrieving from Source')
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

            print('Performing join on likes')
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

            print("Found ", len(result), " matches")

    except socket.error as e:
        print("Socket error on event source: ", e)
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
            s.connect((HOST, 9099))
            print("Sending results matches to listener")
            print(s.recv(4096))
            response = ''
            for index, match in enumerate(result, start=0):
                response += str(match) + '\n'
            s.send(bytearray(response, "utf-8"))
            print(s.recv(4096))
            s.close()
    except socket.error as e:
        print("Socket error on event listener: ", e)
    except BaseException as err:
        print("Unclassified error", err)
    finally:
        s.close()


if __name__ == '__main__':
    get_matches()