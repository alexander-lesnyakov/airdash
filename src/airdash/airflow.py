from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from airflow_client.client import ApiClient, Configuration
from airflow_client.client.api.dag_api import DAGApi
from airflow_client.client.api.dag_run_api import DagRunApi
from airflow_client.client.models.dag_run_patch_body import DAGRunPatchBody
from airflow_client.client.models.dag_run_patch_states import DAGRunPatchStates
from airflow_client.client.models.trigger_dag_run_post_body import TriggerDAGRunPostBody

from airdash.config import AirflowConfig, normalize_airflow_url


@dataclass(frozen=True)
class DagSummary:
    dag_id: str
    display_name: str
    paused: bool
    last_state: str
    last_run_at: datetime | None
    next_run_at: datetime | None
    schedule: str | None
    recent_states: list[str]


@dataclass(frozen=True)
class DagRunSummary:
    dag_id: str
    dag_run_id: str
    state: str
    run_after: datetime | None
    duration: float | int | None


@dataclass(frozen=True)
class MarkSuccessResult:
    dag_count: int
    inspected_count: int
    updated_count: int


@dataclass(frozen=True)
class TriggerDagsResult:
    dag_count: int
    triggered_count: int


class AirflowService:
    def __init__(self, config: AirflowConfig) -> None:
        api_config = Configuration(
            host=normalize_airflow_url(config.url),
            access_token=config.token,
        )
        api_client = ApiClient(api_config)
        self._dags = DAGApi(api_client)
        self._dag_runs = DagRunApi(api_client)

    def list_dags(self, limit: int = 100) -> list[DagSummary]:
        dags_response = self._dags.get_dags(
            limit=limit,
            order_by=["dag_id"],
            _request_timeout=(5, 30),
        )

        summaries: list[DagSummary] = []
        for dag in dags_response.dags:
            recent_runs = self._latest_runs(dag.dag_id, limit=10)
            last_run = recent_runs[0] if recent_runs else None
            summaries.append(
                DagSummary(
                    dag_id=dag.dag_id,
                    display_name=dag.dag_display_name or dag.dag_id,
                    paused=bool(dag.is_paused),
                    last_state=self._state_value(last_run.state) if last_run else "none",
                    last_run_at=last_run.run_after if last_run else None,
                    next_run_at=dag.next_dagrun_run_after,
                    schedule=dag.timetable_summary,
                    recent_states=[self._state_value(run.state) for run in recent_runs],
                )
            )
        return summaries

    def list_dag_runs(
        self,
        status: str | None = None,
        limit: int = 100,
    ) -> list[DagRunSummary]:
        response = self._dag_runs.get_dag_runs(
            dag_id="~",
            state=[status] if status else None,
            limit=limit,
            order_by=["-run_after"],
            _request_timeout=(5, 30),
        )
        return [self._run_summary(run) for run in response.dag_runs]

    def mark_latest_runs_success(
        self,
        dag_ids: list[str],
        limit: int = 10,
    ) -> MarkSuccessResult:
        inspected_count = 0
        updated_count = 0
        body = DAGRunPatchBody(state=DAGRunPatchStates.SUCCESS)

        for dag_id in dag_ids:
            for run in self._latest_runs(dag_id, limit=limit):
                inspected_count += 1
                if self._state_value(run.state) == "success":
                    continue
                self._dag_runs.patch_dag_run(
                    dag_id=dag_id,
                    dag_run_id=run.dag_run_id,
                    dag_run_patch_body=body,
                    update_mask=["state"],
                    _request_timeout=(5, 15),
                )
                updated_count += 1

        return MarkSuccessResult(
            dag_count=len(dag_ids),
            inspected_count=inspected_count,
            updated_count=updated_count,
        )

    def mark_runs_success(self, runs: list[DagRunSummary]) -> MarkSuccessResult:
        updated_count = 0
        body = DAGRunPatchBody(state=DAGRunPatchStates.SUCCESS)

        for run in runs:
            if run.state == "success":
                continue
            self._dag_runs.patch_dag_run(
                dag_id=run.dag_id,
                dag_run_id=run.dag_run_id,
                dag_run_patch_body=body,
                update_mask=["state"],
                _request_timeout=(5, 15),
            )
            updated_count += 1

        return MarkSuccessResult(
            dag_count=len({run.dag_id for run in runs}),
            inspected_count=len(runs),
            updated_count=updated_count,
        )

    def trigger_dags(self, dag_ids: list[str]) -> TriggerDagsResult:
        body = TriggerDAGRunPostBody()
        triggered_count = 0

        for dag_id in dag_ids:
            self._dag_runs.trigger_dag_run(
                dag_id=dag_id,
                trigger_dag_run_post_body=body,
                _request_timeout=(5, 15),
            )
            triggered_count += 1

        return TriggerDagsResult(
            dag_count=len(dag_ids),
            triggered_count=triggered_count,
        )

    def _latest_runs(self, dag_id: str, limit: int):
        response = self._dag_runs.get_dag_runs(
            dag_id=dag_id,
            limit=limit,
            order_by=["-run_after"],
            _request_timeout=(5, 15),
        )
        return response.dag_runs

    def _run_summary(self, run) -> DagRunSummary:
        return DagRunSummary(
            dag_id=run.dag_id,
            dag_run_id=run.dag_run_id,
            state=self._state_value(run.state),
            run_after=run.run_after,
            duration=run.duration,
        )

    @staticmethod
    def _state_value(state: object) -> str:
        return str(getattr(state, "value", state))
