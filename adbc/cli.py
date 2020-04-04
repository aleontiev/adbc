#!?usr/bin/env python

from cleo import Application
from adbc.commands import (
    RunCommand
)


def main():
    run = RunCommand()
    application = Application()
    application.add(run)
    application.run()


if __name__ == "__main__":
    main()
