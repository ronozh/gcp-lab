"""Render DAG configs into Airflow DAG files: airflow/dags/<dag_id>.yml -> airflow/build/dags/<dag_id>.py

    type: <template>    picks airflow/templates/<template>.py.j2
    other keys          the template's variables (exactly the keys listed in KEYS)

Usage: python airflow/render.py [--check]   (--check: also import every DAG, as the scheduler would)
"""
import pathlib
import sys

import jinja2
import yaml

HERE = pathlib.Path(__file__).resolve().parent
OUT = HERE / "build" / "dags"
KEYS = {"bronze_ingest": {"description", "feed", "schedule"}}


def render():
    # [[ ]] / [% %] so the templates can keep Airflow's own {{ }} untouched.
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(HERE / "templates"), undefined=jinja2.StrictUndefined,
        variable_start_string="[[", variable_end_string="]]",
        block_start_string="[%", block_end_string="%]", comment_start_string="[#", comment_end_string="#]",
        keep_trailing_newline=True)
    env.filters["py"] = repr   # a Python literal: "text", None, ...
    OUT.mkdir(parents=True, exist_ok=True)
    for old in OUT.glob("*.py"):
        old.unlink()
    for path in sorted((HERE / "dags").glob("*.yml")):
        cfg = yaml.safe_load(path.read_text())
        kind = cfg.pop("type", None)
        if kind not in KEYS:
            sys.exit(f"{path.name}: type must be one of {sorted(KEYS)}")
        if set(cfg) != KEYS[kind]:
            sys.exit(f"{path.name}: keys must be type + {sorted(KEYS[kind])}, got {sorted(cfg)}")
        code = env.get_template(f"{kind}.py.j2").render(dag_id=path.stem, source=f"airflow/dags/{path.name}", **cfg)
        (OUT / f"{path.stem}.py").write_text(code)
        print(f"rendered {path.stem}")


def check():
    from airflow.dag_processing.dagbag import DagBag
    bag = DagBag(str(OUT))
    if bag.import_errors:
        sys.exit(f"import errors: {bag.import_errors}")
    print(f"ok: {len(bag.dag_ids)} DAGs import cleanly")


if __name__ == "__main__":
    render()
    if "--check" in sys.argv:
        check()
