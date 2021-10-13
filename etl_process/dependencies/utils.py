import os

import re

def path_valid(path):
    """Checks if a path exists and is writable. If not exists, creates the given path.

    :parameters path : path to be validated.
    """

    if os.path.isdir(path):
    	pass
    else:
    	try:
    	    os.mkdir(path)
    	except OSError:
    	    raise CustomException(f'Unable to create directory : {path}')

    if os.access(path, os.W_OK):
    	return True
    else:
    	raise CustomException(f'Unable to access directory : {path}')
    	return False
    	
def time_convertor(s):
    hour_reg = r'.*?PT(.*)H.*'
    hour = re.search(hour_reg, s)
    if hour:
        min_reg = r'.*?H(.*)M.*'
    else:
        min_reg =r'.*?PT(.*)M.*'
    minutes = re.search(min_reg, s)
    if minutes:
        if hour:
            return int(hour.group(1))*60 + int(minutes.group(1))
        else:
            return int(minutes.group(1))
    elif hour:
        return int(hour.group(1))*60
    else:
        return 0
