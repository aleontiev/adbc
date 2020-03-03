#!?usr/bin/env python

from cleo import Application
from adbc.commands import (
    DiffCommand,
    RunCommand
)


def main():
    diff = DiffCommand()
    run = RunCommand()
    application = Application()
    application.add(diff)
    application.add(run)
    application.run()


if __name__ == "__main__":
    main()
