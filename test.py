import re

__author__ = 'Florian'
m = re.search('\S+@\S+\.\S+', "office@carl-teufel.de")
print(m.group(0))