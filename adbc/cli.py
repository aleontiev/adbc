#!?usr/bin/env python

from cleo import Application
from commands import (
    DiffCommand,
)


def main():
    diff = DiffCommand()
    application = Application()
    application.add(diff)
    application.run()


if __name__ == "__main__":
    main()
