"""Optional Docker read-only requests in a bounded subprocess."""

import json
import sys
from datetime import datetime, timezone

from aiops.collectors.models import Metric
from aiops.collectors.redact import mask
from aiops.process import run
from aiops.providers.base import ProviderError


def collect_with_client(client, names):
    metrics, failures = [], []
    now = datetime.now(timezone.utc).isoformat()
    for name in names:
        try:
            container = client.containers.get(name)
            target = mask(name)
            metrics.append(
                {
                    "metric_id": "",
                    "timestamp": now,
                    "name": "container_status",
                    "value": mask(container.status),
                    "unit": "state",
                    "target": target,
                }
            )
            if container.status != "running":
                continue
            stats = container.stats(stream=False, one_shot=True)
            memory = stats.get("memory_stats", {})
            cpu, previous = stats.get("cpu_stats", {}), stats.get("precpu_stats", {})
            values = [
                ("container_memory_used", memory.get("usage"), "bytes"),
                ("container_memory_limit", memory.get("limit"), "bytes"),
            ]
            delta = cpu.get("cpu_usage", {}).get("total_usage", 0) - previous.get(
                "cpu_usage", {}
            ).get("total_usage", 0)
            system_delta = cpu.get("system_cpu_usage", 0) - previous.get(
                "system_cpu_usage", 0
            )
            count = cpu.get("online_cpus") or len(
                cpu.get("cpu_usage", {}).get("percpu_usage", [])
            )
            if previous and system_delta > 0 and delta >= 0 and count:
                values.append(
                    (
                        "container_cpu_percent",
                        delta / system_delta * count * 100,
                        "percent",
                    )
                )
            for metric, value, unit in values:
                if value is not None:
                    metrics.append(
                        {
                            "metric_id": "",
                            "timestamp": now,
                            "name": metric,
                            "value": value,
                            "unit": unit,
                            "target": target,
                        }
                    )
        except Exception:
            # SDK exceptions may contain daemon URLs or request credentials.
            failures.append(
                {
                    "service": "docker",
                    "file": mask(name),
                    "reason": "container_unavailable",
                }
            )
    return {"metrics": metrics, "failures": failures}


class DockerCollector:
    def collect(self, config, timeout):
        try:
            code, stdout, _ = run(
                [sys.executable, "-m", "aiops.collectors.docker"],
                timeout=timeout,
                stdin=config.model_dump_json(),
            )
            if code:
                raise ValueError("docker_worker_failed")
            payload = json.loads(stdout)
            return [
                Metric.model_validate(item) for item in payload["metrics"]
            ], payload["failures"]
        except (ProviderError, ValueError, KeyError):
            return [], [
                {"service": "docker", "reason": "docker_unavailable_or_timeout"}
            ]


if __name__ == "__main__":
    try:
        import docker

        config = json.load(sys.stdin)
        client = docker.from_env(timeout=config["timeout_seconds"])
        try:
            print(json.dumps(collect_with_client(client, config["containers"])))
        finally:
            client.close()
    except Exception:
        print(
            json.dumps(
                {
                    "metrics": [],
                    "failures": [{"service": "docker", "reason": "docker_unavailable"}],
                }
            )
        )
