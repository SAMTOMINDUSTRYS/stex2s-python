# https://rich.readthedocs.io/en/stable/logging.html
import logging
from rich.logging import RichHandler
log = logging.getLogger("rich")
FORMAT = "%(message)s"
logging.basicConfig(
    level="NOTSET", format=FORMAT, datefmt="[%X]", handlers=[RichHandler(markup=True)]
)

