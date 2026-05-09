from __future__ import annotations

from datetime import datetime

from rich.text import Text
from textual.app import App, ComposeResult
from textual.containers import Container, Horizontal, Vertical
from textual.css.query import NoMatches
from textual.widgets import Button, DataTable, Footer, Header, Input, Label, LoadingIndicator, Static

from airdash.airflow import AirflowService, DagRunSummary, DagSummary, MarkSuccessResult, TriggerDagsResult
from airdash.config import AirflowConfig, config_path, load_config, normalize_airflow_url, save_config


DEFAULT_MARK_SUCCESS_LIMIT = 10
VIEW_DAGS = "dags"
VIEW_RUNS = "runs"
FILTER_ACTIONS = {
    "filter_failed",
    "filter_running",
    "filter_queued",
    "filter_success",
    "filter_all",
}
DAG_ACTIONS = {
    "trigger_selected_dags",
}


class Airdash(App[None]):
    TITLE = "airdash"

    CSS = """
    Screen {
        background: #0f1117;
        color: #d8dee9;
    }

    Header {
        background: #151922;
        color: #e5e9f0;
    }

    #setup, #main {
        height: 1fr;
        padding: 2 4;
    }

    #setup-card {
        width: 72;
        max-width: 100%;
        height: auto;
        margin: 2 0;
        padding: 2 3;
        border: round #3b4252;
        background: #151922;
    }

    #title {
        text-style: bold;
        color: #88c0d0;
        margin-bottom: 1;
    }

    #subtitle, #status, #config-path {
        color: #8f98aa;
        margin-bottom: 1;
    }

    Input {
        margin-bottom: 1;
    }

    Button {
        width: 16;
        margin-top: 1;
    }

    #toolbar {
        height: auto;
        margin-bottom: 1;
    }

    #heading {
        width: 1fr;
        text-style: bold;
        color: #88c0d0;
    }

    #bulk-prompt {
        height: auto;
        margin-bottom: 1;
    }

    #bulk-label {
        width: auto;
        margin-right: 1;
        color: #8f98aa;
    }

    #run-count {
        width: 12;
    }

    #dags {
        height: 1fr;
        border: round #3b4252;
        background: #11151d;
    }

    #loader {
        height: auto;
        margin: 1 0;
    }

    .hidden {
        display: none;
    }
    """

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("v", "switch_view", "View"),
        ("r", "refresh", "Refresh"),
        ("c", "configure", "Config"),
        ("space", "toggle_selected", "Select"),
        ("s", "prompt_mark_success", "Mark success"),
        ("t", "trigger_selected_dags", "Trigger"),
        ("1", "filter_failed", "Failed"),
        ("2", "filter_running", "Running"),
        ("3", "filter_queued", "Queued"),
        ("4", "filter_success", "Success"),
        ("0", "filter_all", "All"),
        ("escape", "cancel_prompt", "Cancel"),
    ]

    def __init__(self) -> None:
        super().__init__()
        self._config = load_config()
        self._view = VIEW_DAGS
        self._run_filter: str | None = "failed"
        self._selected_dags: set[str] = set()
        self._selected_runs: set[str] = set()
        self._run_rows: dict[str, DagRunSummary] = {}

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        if self._config is None:
            yield self._setup_view()
        else:
            yield self._main_view()
        yield Footer()

    def on_mount(self) -> None:
        if self._config is not None:
            self._focus_table()
            self._start_load()

    def _setup_view(self) -> Container:
        config = self._config
        token_placeholder = "New token" if config is None else "New token (leave blank to keep current)"
        return Container(
            Vertical(
                Static("airdash", id="title"),
                Static(self._setup_subtitle(), id="subtitle"),
                Label("Airflow URL"),
                Input(config.url if config else "", placeholder="https://airflow.example.com", id="url"),
                Label("Bearer token"),
                Input(placeholder=token_placeholder, password=True, id="token"),
                Button("Save", variant="primary", id="save"),
                Static(f"Config: {config_path()}", id="config-path"),
                Static("", id="status"),
                id="setup-card",
            ),
            id="setup",
        )

    def _main_view(self) -> Container:
        table = DataTable(id="dags", zebra_stripes=True)
        table.cursor_type = "row"
        self._configure_dag_table(table)
        return Container(
            Static("DAGs", id="heading"),
            Horizontal(
                Static("Runs to mark successful:", id="bulk-label"),
                Input(str(DEFAULT_MARK_SUCCESS_LIMIT), id="run-count"),
                id="bulk-prompt",
                classes="hidden",
            ),
            LoadingIndicator(id="loader", classes="hidden"),
            table,
            Static("", id="status"),
            id="main",
        )

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "save":
            self._save_config()

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        if event.data_table.id != "dags":
            return
        if self._view == VIEW_DAGS:
            self._toggle_dag(str(event.row_key.value))
        else:
            self._toggle_run(str(event.row_key.value))

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "run-count":
            self._confirm_mark_success()

    def check_action(self, action: str, parameters: tuple[object, ...]) -> bool | None:
        if action in FILTER_ACTIONS:
            return self._config is not None and self._has_widget("#main") and self._view == VIEW_RUNS
        if action in DAG_ACTIONS:
            return self._config is not None and self._has_widget("#main") and self._view == VIEW_DAGS
        if action == "cancel_prompt":
            return self._can_cancel()
        return True

    def action_refresh(self) -> None:
        if self._config is not None and self._has_widget("#main"):
            self._start_load()

    def action_configure(self) -> None:
        self._remove_if_mounted("#main")
        if self._has_widget("#setup"):
            self.query_one("#url", Input).focus()
            return
        self.mount(self._setup_view(), before=self.query_one(Footer))
        self.query_one("#url", Input).focus()
        self.refresh_bindings()

    def action_switch_view(self) -> None:
        if self._config is None or not self._has_widget("#main"):
            return
        self._hide_bulk_prompt()
        self._view = VIEW_RUNS if self._view == VIEW_DAGS else VIEW_DAGS
        self.refresh_bindings()
        self._start_load()

    def action_toggle_selected(self) -> None:
        if self._config is None or not self._has_widget("#main"):
            return
        row_key = self._current_row_key()
        if row_key is None:
            return
        if self._view == VIEW_DAGS:
            self._toggle_dag(row_key)
        else:
            self._toggle_run(row_key)

    def action_prompt_mark_success(self) -> None:
        if self._config is None or not self._has_widget("#main"):
            return
        if self._view == VIEW_RUNS:
            self._start_mark_selected_runs()
            return
        if not self._selected_dags:
            self.query_one("#status", Static).update("Select one or more DAGs first.")
            return

        prompt = self.query_one("#bulk-prompt")
        prompt.remove_class("hidden")
        run_count = self.query_one("#run-count", Input)
        run_count.value = str(DEFAULT_MARK_SUCCESS_LIMIT)
        run_count.focus()
        self.refresh_bindings()

    def action_trigger_selected_dags(self) -> None:
        if self._config is None or not self._has_widget("#main") or self._view != VIEW_DAGS:
            return
        if not self._selected_dags:
            self.query_one("#status", Static).update("Select one or more DAGs first.")
            return

        selected_dags = sorted(self._selected_dags)
        self.query_one("#loader").remove_class("hidden")
        self.query_one("#status", Static).update(f"Triggering {len(selected_dags)} DAGs...")
        self.run_worker(
            lambda: self._trigger_dags(selected_dags),
            thread=True,
            exclusive=True,
            name="trigger-dags",
        )

    def action_cancel_prompt(self) -> None:
        if self._config is None:
            return

        if self._has_widget("#setup") and not self._has_widget("#main"):
            self._remove_if_mounted("#setup")
            self.mount(self._main_view(), before=self.query_one(Footer))
            self._focus_table()
            self.refresh_bindings()
            self._start_load()
            return

        if not self._has_widget("#main"):
            return

        prompt = self.query_one("#bulk-prompt")
        if not prompt.has_class("hidden"):
            prompt.add_class("hidden")
            self.query_one("#dags", DataTable).focus()
            self.refresh_bindings()

    def action_filter_failed(self) -> None:
        self._set_run_filter("failed")

    def action_filter_running(self) -> None:
        self._set_run_filter("running")

    def action_filter_queued(self) -> None:
        self._set_run_filter("queued")

    def action_filter_success(self) -> None:
        self._set_run_filter("success")

    def action_filter_all(self) -> None:
        self._set_run_filter(None)

    def _save_config(self) -> None:
        url = self.query_one("#url", Input).value.strip()
        token = self.query_one("#token", Input).value.strip()
        if not url:
            self.query_one("#status", Static).update("URL is required.")
            return
        if self._config is None and not token:
            self.query_one("#status", Static).update("Token is required.")
            return

        self._config = AirflowConfig(
            url=normalize_airflow_url(url),
            token=token or self._config.token,
        )
        save_config(self._config)
        self._remove_if_mounted("#setup")
        self._selected_dags.clear()
        self._selected_runs.clear()
        self._run_rows.clear()
        self.mount(self._main_view(), before=self.query_one(Footer))
        self._focus_table()
        self._start_load()

    def _start_load(self) -> None:
        if self._config is None:
            return
        self.query_one("#loader").remove_class("hidden")
        if self._view == VIEW_RUNS:
            self.query_one("#status", Static).update("Loading DAG runs...")
            self.run_worker(self._load_runs, thread=True, exclusive=True, name="load-runs")
        else:
            self.query_one("#status", Static).update("Loading DAGs...")
            self.run_worker(self._load_dags, thread=True, exclusive=True, name="load-dags")

    def _set_run_filter(self, status: str | None) -> None:
        if self._config is None or not self._has_widget("#main"):
            return
        self._hide_bulk_prompt()
        self._view = VIEW_RUNS
        self._run_filter = status
        self._selected_runs.clear()
        self.refresh_bindings()
        self._start_load()

    def _confirm_mark_success(self) -> None:
        run_count = self.query_one("#run-count", Input).value.strip()
        try:
            limit = DEFAULT_MARK_SUCCESS_LIMIT if not run_count else int(run_count)
        except ValueError:
            self.query_one("#status", Static).update("Run count must be a number.")
            return
        if limit < 1:
            self.query_one("#status", Static).update("Run count must be at least 1.")
            return

        self.query_one("#bulk-prompt").add_class("hidden")
        self.refresh_bindings()
        self.query_one("#loader").remove_class("hidden")
        self.query_one("#status", Static).update(
            f"Marking latest {limit} runs successful for {len(self._selected_dags)} DAGs..."
        )
        selected_dags = sorted(self._selected_dags)
        self.run_worker(
            lambda: self._mark_success(selected_dags, limit),
            thread=True,
            exclusive=True,
            name="mark-success",
        )

    def _load_dags(self) -> None:
        assert self._config is not None
        try:
            rows = AirflowService(self._config).list_dags()
        except Exception as exc:  # noqa: BLE001 - surface API/client errors in the TUI.
            self.call_from_thread(self._show_error, exc)
            return
        self.call_from_thread(self._show_dags, rows)

    def _load_runs(self) -> None:
        assert self._config is not None
        try:
            rows = AirflowService(self._config).list_dag_runs(status=self._run_filter)
        except Exception as exc:  # noqa: BLE001 - surface API/client errors in the TUI.
            self.call_from_thread(self._show_error, exc)
            return
        self.call_from_thread(self._show_runs, rows)

    def _mark_success(self, dag_ids: list[str], limit: int) -> None:
        assert self._config is not None
        try:
            service = AirflowService(self._config)
            result = service.mark_latest_runs_success(dag_ids, limit=limit)
            rows = service.list_dags()
        except Exception as exc:  # noqa: BLE001 - surface API/client errors in the TUI.
            self.call_from_thread(self._show_error, exc)
            return
        self.call_from_thread(self._show_mark_success_result, result, rows)

    def _trigger_dags(self, dag_ids: list[str]) -> None:
        assert self._config is not None
        try:
            service = AirflowService(self._config)
            result = service.trigger_dags(dag_ids)
            rows = service.list_dags()
        except Exception as exc:  # noqa: BLE001 - surface API/client errors in the TUI.
            self.call_from_thread(self._show_error, exc)
            return
        self.call_from_thread(self._show_trigger_result, result, rows)

    def _start_mark_selected_runs(self) -> None:
        if not self._selected_runs:
            self.query_one("#status", Static).update("Select one or more DAG runs first.")
            return

        runs = [self._run_rows[key] for key in sorted(self._selected_runs) if key in self._run_rows]
        if not runs:
            self.query_one("#status", Static).update("Selected DAG runs are no longer loaded.")
            return

        self.query_one("#loader").remove_class("hidden")
        self.query_one("#status", Static).update(f"Marking {len(runs)} DAG runs successful...")
        self.run_worker(
            lambda: self._mark_runs_success(runs),
            thread=True,
            exclusive=True,
            name="mark-runs-success",
        )

    def _mark_runs_success(self, runs: list[DagRunSummary]) -> None:
        assert self._config is not None
        try:
            service = AirflowService(self._config)
            result = service.mark_runs_success(runs)
            rows = service.list_dag_runs(status=self._run_filter)
        except Exception as exc:  # noqa: BLE001 - surface API/client errors in the TUI.
            self.call_from_thread(self._show_error, exc)
            return
        self.call_from_thread(self._show_run_mark_success_result, result, rows)

    def _show_dags(self, rows: list[DagSummary], status: str | None = None) -> None:
        if not self._has_widget("#main"):
            return
        table = self.query_one("#dags", DataTable)
        self._view = VIEW_DAGS
        self._configure_dag_table(table)
        current_dags = {row.dag_id for row in rows}
        self._selected_dags.intersection_update(current_dags)
        for row in rows:
            table.add_row(
                self._selected_marker(row.dag_id),
                row.display_name,
                self._status_text(row.last_state),
                self._history_text(row.recent_states),
                "yes" if row.paused else "no",
                self._format_schedule(row.schedule),
                self._format_time(row.last_run_at),
                self._format_time(row.next_run_at),
                key=row.dag_id,
            )

        self.query_one("#loader").add_class("hidden")
        self.query_one("#heading", Static).update("DAGs")
        self.query_one("#status", Static).update(status or self._loaded_status(len(rows)))
        self.refresh_bindings()
        self._focus_table()

    def _show_runs(self, rows: list[DagRunSummary], status: str | None = None) -> None:
        if not self._has_widget("#main"):
            return
        table = self.query_one("#dags", DataTable)
        self._view = VIEW_RUNS
        self._hide_bulk_prompt()
        self._configure_run_table(table)
        self._run_rows.clear()
        self._selected_runs.clear()

        for index, row in enumerate(rows):
            row_key = str(index)
            self._run_rows[row_key] = row
            table.add_row(
                self._selected_run_marker(row_key),
                row.dag_id,
                row.dag_run_id,
                self._status_text(row.state),
                self._format_time(row.run_after),
                self._format_duration(row.duration),
                key=row_key,
            )

        self.query_one("#loader").add_class("hidden")
        self.query_one("#heading", Static).update(self._run_heading())
        self.query_one("#status", Static).update(status or self._runs_loaded_status(len(rows)))
        self.refresh_bindings()
        self._focus_table()

    def _show_mark_success_result(
        self,
        result: MarkSuccessResult,
        rows: list[DagSummary],
    ) -> None:
        self._selected_dags.clear()
        self._show_dags(
            rows,
            status=(
                f"Updated {result.updated_count} of {result.inspected_count} runs "
                f"across {result.dag_count} DAGs"
            ),
        )

    def _show_trigger_result(
        self,
        result: TriggerDagsResult,
        rows: list[DagSummary],
    ) -> None:
        self._selected_dags.clear()
        self._show_dags(
            rows,
            status=f"Triggered {result.triggered_count} of {result.dag_count} selected DAGs",
        )

    def _show_run_mark_success_result(
        self,
        result: MarkSuccessResult,
        rows: list[DagRunSummary],
    ) -> None:
        self._selected_runs.clear()
        self._show_runs(
            rows,
            status=(
                f"Updated {result.updated_count} of {result.inspected_count} selected runs "
                f"across {result.dag_count} DAGs"
            ),
        )

    def _show_error(self, exc: Exception) -> None:
        if not self._has_widget("#main"):
            return
        self.query_one("#loader").add_class("hidden")
        if self._is_auth_error(exc):
            self.query_one("#status", Static).update(
                "Auth failed: token expired/invalid or missing permission. Press c to update config."
            )
            return
        self.query_one("#status", Static).update(f"Error: {exc}")

    def _configure_dag_table(self, table: DataTable) -> None:
        table.clear(columns=True)
        table.add_column("", key="selected", width=3)
        table.add_column("DAG", key="dag")
        table.add_column("Status", key="status")
        table.add_column("Last 10", key="history")
        table.add_column("Paused", key="paused")
        table.add_column("Schedule", key="schedule")
        table.add_column("Last Run", key="last_run")
        table.add_column("Next Run", key="next_run")

    def _configure_run_table(self, table: DataTable) -> None:
        table.clear(columns=True)
        table.add_column("", key="selected", width=3)
        table.add_column("DAG", key="dag")
        table.add_column("Run", key="run")
        table.add_column("Status", key="status")
        table.add_column("Run At", key="run_at")
        table.add_column("Duration", key="duration")

    def _toggle_dag(self, dag_id: str) -> None:
        if dag_id in self._selected_dags:
            self._selected_dags.remove(dag_id)
        else:
            self._selected_dags.add(dag_id)

        table = self.query_one("#dags", DataTable)
        table.update_cell(dag_id, "selected", self._selected_marker(dag_id))
        self.query_one("#status", Static).update(
            f"{len(self._selected_dags)} selected. Press t to trigger or s to mark latest runs successful."
        )

    def _toggle_run(self, row_key: str) -> None:
        if row_key not in self._run_rows:
            return
        if row_key in self._selected_runs:
            self._selected_runs.remove(row_key)
        else:
            self._selected_runs.add(row_key)

        table = self.query_one("#dags", DataTable)
        table.update_cell(row_key, "selected", self._selected_run_marker(row_key))
        self.query_one("#status", Static).update(
            f"{len(self._selected_runs)} selected. Press s to mark selected runs successful."
        )

    def _current_row_key(self) -> str | None:
        table = self.query_one("#dags", DataTable)
        if table.row_count == 0:
            return None
        row_key = table.coordinate_to_cell_key(table.cursor_coordinate).row_key
        return str(row_key.value)

    def _loaded_status(self, row_count: int) -> str:
        selected = len(self._selected_dags)
        if selected:
            return f"{row_count} DAGs loaded, {selected} selected"
        return f"{row_count} DAGs loaded"

    def _selected_marker(self, dag_id: str) -> str:
        return "x" if dag_id in self._selected_dags else ""

    def _selected_run_marker(self, row_key: str) -> str:
        return "x" if row_key in self._selected_runs else ""

    def _run_heading(self) -> str:
        return f"DAG Runs ({self._run_filter or 'all'})"

    def _runs_loaded_status(self, row_count: int) -> str:
        selected = len(self._selected_runs)
        prefix = f"{row_count} DAG runs loaded, filter: {self._run_filter or 'all'}"
        if selected:
            return f"{prefix}, {selected} selected"
        return prefix

    def _focus_table(self) -> None:
        self.query_one("#dags", DataTable).focus()

    def _setup_subtitle(self) -> str:
        if self._config is None:
            return "Connect once, then reopen directly into your DAGs."
        return "Update the saved Airflow URL or bearer token."

    def _has_widget(self, selector: str) -> bool:
        try:
            self.query_one(selector)
        except NoMatches:
            return False
        return True

    def _can_cancel(self) -> bool:
        if self._config is None:
            return False
        if self._has_widget("#setup") and not self._has_widget("#main"):
            return True
        if not self._has_widget("#main"):
            return False
        if not self._has_widget("#bulk-prompt"):
            return False
        return not self.query_one("#bulk-prompt").has_class("hidden")

    def _remove_if_mounted(self, selector: str) -> None:
        try:
            self.query_one(selector).remove()
        except NoMatches:
            pass

    def _hide_bulk_prompt(self) -> None:
        self.query_one("#bulk-prompt").add_class("hidden")

    @staticmethod
    def _status_text(status: str) -> Text:
        return Text(status, style=Airdash._status_color(status))

    @staticmethod
    def _history_text(states: list[str]) -> Text:
        if not states:
            return Text("-", style="#8f98aa")

        history = Text()
        for index, state in enumerate(reversed(states[:10])):
            if index:
                history.append(" ")
            history.append("■", style=Airdash._status_color(state))
        return history

    @staticmethod
    def _is_auth_error(exc: Exception) -> bool:
        status = Airdash._exception_status(exc)
        if status in {401, 403}:
            return True

        text = f"{exc.__class__.__name__} {getattr(exc, 'reason', '')} {exc}"
        return any(value in text for value in ("Unauthorized", "Forbidden"))

    @staticmethod
    def _exception_status(exc: Exception) -> int | None:
        candidates = [
            getattr(exc, "status", None),
            getattr(exc, "status_code", None),
            getattr(getattr(exc, "response", None), "status", None),
            getattr(getattr(exc, "response", None), "status_code", None),
        ]
        for value in candidates:
            if value is None:
                continue
            try:
                return int(value)
            except (TypeError, ValueError):
                continue
        return None

    @staticmethod
    def _status_color(status: str) -> str:
        palette = {
            "success": "#4ade80",
            "failed": "#bf616a",
            "running": "#facc15",
            "queued": "#b48ead",
            "none": "#8f98aa",
        }
        return palette.get(status.lower(), "#d8dee9")

    @staticmethod
    def _format_time(value: datetime | None) -> str:
        if value is None:
            return "-"
        return value.astimezone().strftime("%Y-%m-%d %H:%M")

    @staticmethod
    def _format_schedule(value: str | None) -> str:
        if not value:
            return "-"
        return value

    @staticmethod
    def _format_duration(value: float | int | None) -> str:
        if value is None:
            return "-"
        return f"{value:.1f}s"


def main() -> None:
    Airdash().run()


if __name__ == "__main__":
    main()
