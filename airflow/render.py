"""Render DAG configs into Airflow DAG files: airflow/dags/<dag_id>.yml -> airflow/build/dags/<dag_id>.py

    type: <template>             picks airflow/templates/<template>.py.j2 (all extend _base.py.j2)
    description: <text>
    cron: ["<cron>", ...]        optional: run on these crons (TIMEZONE)
    after: <layer.table>         optional: run when that table is updated (an Airflow Asset)
    retries: 3                   optional (default 3)
    retry_delay_minutes: 10      optional (default 10)
    <type's own keys>            see TYPES
Neither cron nor after: runs only when triggered (by the event function or by hand).
Every DAG: one run at a time (max_active_runs=1); later runs wait in the queue.

Usage: python airflow/render.py [--check]   (--check: also import every DAG, as the scheduler would)
"""
import pathlib
import sys

import jinja2
import yaml

HERE = pathlib.Path(__file__).resolve().parent
OUT = HERE / "build" / "dags"
TIMEZONE = "Australia/Sydney"
COMMON = {"description": None, "cron": None, "after": None, "retries": 3, "retry_delay_minutes": 10}
# type -> its own keys (None = required)
TYPES = {
    "bronze_ingest": {"feed": None},
    "dbt_build": {"select": None},
    "spark_job": {"entry": None, "args": []},
}


def load(path):
    cfg = yaml.safe_load(path.read_text())
    kind = cfg.pop("type", None)
    if kind not in TYPES:
        sys.exit(f"{path.name}: type must be one of {sorted(TYPES)}")
    allowed = {**COMMON, **TYPES[kind]}
    if unknown := set(cfg) - set(allowed):
        sys.exit(f"{path.name}: unknown keys {sorted(unknown)}; allowed: type + {sorted(allowed)}")
    cfg = {**allowed, **cfg}
    if missing := [k for k, v in cfg.items() if v is None and k in ("description", *TYPES[kind])]:
        sys.exit(f"{path.name}: missing keys {missing}")
    if cfg["cron"] and cfg["after"]:
        sys.exit(f"{path.name}: use cron or after, not both")
    # The event function triggers bronze_<feed>, taken from the file's path.
    if kind == "bronze_ingest" and path.stem != f"bronze_{cfg['feed']}":
        sys.exit(f"{path.name}: must be named bronze_{cfg['feed']}.yml (events trigger DAG bronze_<feed>)")

    # schedule= as Python code
    if cfg["cron"]:
        crons = ", ".join(repr(c) for c in cfg["cron"])
        cfg["schedule"] = f"MultipleCronTriggerTimetable({crons}, timezone={TIMEZONE!r})"
    elif cfg["after"]:
        cfg["schedule"] = f"[Asset({cfg['after']!r})]"
    else:
        cfg["schedule"] = "None"
    cfg["tags"] = [kind] + (["cron"] if cfg["cron"] else ["after"] if cfg["after"] else [])
    return kind, cfg


def render():
    # [[ ]] / [% %] so the templates can keep Airflow's own {{ }} untouched.
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(HERE / "templates"), undefined=jinja2.StrictUndefined,
        variable_start_string="[[", variable_end_string="]]",
        block_start_string="[%", block_end_string="%]", comment_start_string="[#", comment_end_string="#]",
        keep_trailing_newline=True)
    env.filters["py"] = repr   # a Python literal: "text", None, [...]
    OUT.mkdir(parents=True, exist_ok=True)
    for old in OUT.glob("*.py"):
        old.unlink()
    for path in sorted((HERE / "dags").glob("*.yml")):
        kind, cfg = load(path)
        code = env.get_template(f"{kind}.py.j2").render(dag_id=path.stem, source=f"airflow/dags/{path.name}", **cfg)
        (OUT / f"{path.stem}.py").write_text(code)
        print(f"rendered {path.stem} ({kind})")


def check():
    from airflow.dag_processing.dagbag import DagBag
    bag = DagBag(str(OUT))
    if bag.import_errors:
        sys.exit(f"import errors: {bag.import_errors}")
    for dag_id, dag in sorted(bag.dags.items()):
        print(f"  {dag_id}: schedule={type(dag.timetable).__name__} retries={dag.default_args['retries']}")
    print(f"ok: {len(bag.dag_ids)} DAGs import cleanly")


if __name__ == "__main__":
    render()
    if "--check" in sys.argv:
        check()
