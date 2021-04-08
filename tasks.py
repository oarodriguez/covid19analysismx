"""
Collection of development tasks.

Usage:
    python -m tasks TASK-NAME
"""
from subprocess import run
from typing import List

import typer
from covid19analysismx import __version__

app = typer.Typer()


def _run(command: List[str]):
    """Run a subcommand through python subprocess.run routine."""
    run(command, shell=True)


@app.command(name="format")
def format_():
    """Format the source code using black."""
    command = ["black", "tasks.py", "src", "tests"]
    _run(command)


@app.command()
def pydocstyle():
    """Run pydocstyle."""
    command = ["pydocstyle", "tasks.py", "src", "tests"]
    _run(command)


@app.command()
def mypy():
    """Run mypy."""
    command = ["mypy", "src", "tests"]
    _run(command)


@app.command()
def tests():
    """Run test suite."""
    command = ["pytest"]
    _run(command)


@app.command()
def version():
    """Run mypy."""
    print(__version__)


if __name__ == "__main__":
    app()
