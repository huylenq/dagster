import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any, Dict

import typer
import yaml
from jinja2 import Environment, FileSystemLoader
from rich.console import Console
from rich.syntax import Syntax
from typing_extensions import Annotated

from ..include import STARTER_PROJECT_PATH
from ..version import __version__

app = typer.Typer(
    no_args_is_help=True,
    context_settings={
        "help_option_names": ["-h", "--help"],
    },
    help="CLI tools for working with Dagster and dbt.",
    add_completion=False,
)
project_app = typer.Typer(no_args_is_help=True)
app.add_typer(
    project_app,
    name="project",
    help="Commands to initialize a new Dagster project with an existing dbt project.",
)

DBT_PROJECT_YML_NAME = "dbt_project.yml"


def validate_dagster_project_name(project_name: str) -> str:
    if not project_name.isidentifier():
        raise typer.BadParameter(
            "The project name must be a valid Python identifier containing only letters, digits, or"
            " underscores."
        )

    return project_name


def validate_dbt_project_dir(dbt_project_dir: Path) -> Path:
    dbt_project_yaml_path = dbt_project_dir.joinpath(DBT_PROJECT_YML_NAME)

    if not dbt_project_yaml_path.exists():
        raise typer.BadParameter(
            f"{dbt_project_dir} does not contain a {DBT_PROJECT_YML_NAME} file. Please specify a"
            " valid path to a dbt project."
        )

    return dbt_project_dir


def copy_scaffold(
    project_name: str,
    dagster_project_dir: Path,
    dbt_project_dir: Path,
) -> None:
    shutil.copytree(src=STARTER_PROJECT_PATH, dst=dagster_project_dir, dirs_exist_ok=True)
    dagster_project_dir.joinpath("__init__.py").unlink()

    dbt_project_yaml_path = dbt_project_dir.joinpath(DBT_PROJECT_YML_NAME)
    with dbt_project_yaml_path.open() as fd:
        dbt_project_yaml: Dict[str, Any] = yaml.safe_load(fd)
        dbt_project_name: str = dbt_project_yaml["name"]

    loader = FileSystemLoader(dagster_project_dir)
    env = Environment(loader=loader)

    for path in dagster_project_dir.glob("**/*"):
        if path.suffix == ".jinja":
            relative_path = path.relative_to(Path.cwd())
            destination_path = relative_path.parent.joinpath(relative_path.stem).as_posix()
            template_path = path.relative_to(dagster_project_dir).as_posix()

            env.get_template(template_path).stream(
                dbt_project_dir=os.fspath(dbt_project_dir),
                dbt_project_name=dbt_project_name,
                project_name=project_name,
            ).dump(destination_path)

            path.unlink()


def generate_dbt_manifest_impl(console: Console, dbt_project_dir: Path) -> None:
    dbt_target_path = "target"
    dbt_project_target_path = dbt_project_dir.joinpath(dbt_target_path)
    with console.status(
        f"Generating dbt manifest in [bold green]{dbt_project_target_path}[/bold green]"
    ) as status:
        process = subprocess.Popen(
            [
                "dbt",
                "parse",
                "--target-path",
                dbt_target_path,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env={
                **os.environ.copy(),
                "DBT_LOG_FORMAT": "json",
            },
            cwd=dbt_project_dir,
        )

        for raw_line in process.stdout or []:
            log: str = raw_line.decode().strip()
            try:
                raw_event: Dict[str, Any] = json.loads(log)
                message = raw_event["info"]["msg"]

                console.print(message)
            except:
                console.print(log)

        process.wait()

        status.update("Finished generating dbt manifest.")


@project_app.command(name="scaffold")
def project_scaffold_command(
    project_name: Annotated[
        str,
        typer.Option(
            default=...,
            callback=validate_dagster_project_name,
            show_default=False,
            prompt="Enter a name for your Dagster project (letters, digits, underscores)",
            help="The name of the Dagster project to initialize for your dbt project.",
        ),
    ],
    dbt_project_dir: Annotated[
        Path,
        typer.Option(
            default=...,
            callback=validate_dbt_project_dir,
            is_eager=True,
            help=(
                "The path of your dbt project directory. By default, we use the current"
                " working directory."
            ),
            exists=True,
            file_okay=False,
            dir_okay=True,
            writable=True,
            resolve_path=True,
        ),
    ] = Path.cwd(),
    generate_dbt_manifest: Annotated[
        bool,
        typer.Option(
            default=...,
            help="Create the dbt manifest.",
        ),
    ] = True,
) -> None:
    """This command will initialize a new Dagster project by creating directories and files that
    that loads assets from an existing dbt project.
    """
    console = Console()
    console.print(f"Running with dagster-dbt version: [bold green]{__version__}[/bold green].")
    console.print(
        f"Initializing Dagster project [bold green]{project_name}[/bold green] in current working"
        f" directory for dbt project directory [bold green]{dbt_project_dir}[/bold green]"
    )

    dagster_project_dir = Path.cwd().joinpath(project_name)

    copy_scaffold(
        project_name=project_name,
        dagster_project_dir=dagster_project_dir,
        dbt_project_dir=dbt_project_dir,
    )

    if generate_dbt_manifest:
        generate_dbt_manifest_impl(
            console=console,
            dbt_project_dir=dbt_project_dir,
        )

    console.print(
        (
            "Your Dagster project has been initialized. To view your dbt project in Dagster, run"
            " the following commands:"
        ),
        Syntax(
            code="\n".join(
                [
                    f"cd {dagster_project_dir}",
                    "dagster dev",
                ]
            ),
            lexer="bash",
            padding=1,
        ),
    )
