# noinspection PyUnresolvedReferences
import random

# noinspection PyUnresolvedReferences,PyPackageRequirements
import pyodide
# noinspection PyUnresolvedReferences,PyPackageRequirements
from js import DOMParser, document, setInterval, console

# noinspection PyPackages
import weather_api

# noinspection PyPackages
from weather_api import Report


def main():
    fig = weather_api.plot_db()
    pyscript.write('lineplot', fig)
   
try:
    main()
except Exception as x:
    print("Error starting weather script: {}".format(x))
