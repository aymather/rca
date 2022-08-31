from IPython.display import clear_output
from datetime import datetime


def clear():
    clear_output(wait=True)

def createId(str_size = 7):
    return ''.join(random.choice(string.ascii_uppercase) for x in range(str_size))