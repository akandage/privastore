from enum import Enum

class LogEntryType(Enum):

    START_EPOCH = 1

    CREATE_DIRECTORY = 2

    CREATE_FILE = 3

    REMOVE_FILE = 4

    REMOVE_DIRECTORY = 5
