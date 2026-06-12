import sys


from _django import configure_django

configure_django()

from django.core.management import execute_from_command_line  # noqa: E402


if __name__ == "__main__":
    execute_from_command_line([sys.argv[0], "runworker", *sys.argv[1:]])
