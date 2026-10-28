import os
from datetime import datetime, timezone

import psutil

from aiops.collectors.models import Metric
from aiops.collectors.redact import mask


class SystemCollector:
    def __init__(self, api=psutil):
        self.api = api

    def collect(self, disks):
        now = datetime.now(timezone.utc)
        metrics = []

        def add(name, value, unit, target="host"):
            metrics.append(
                Metric(
                    metric_id="",
                    timestamp=now,
                    name=name,
                    value=value,
                    unit=unit,
                    target=mask(target),
                )
            )

        add("cpu_percent", self.api.cpu_percent(interval=0.1), "percent")
        memory, swap = self.api.virtual_memory(), self.api.swap_memory()
        for name, value, unit in [
            ("memory_percent", memory.percent, "percent"),
            ("memory_available", memory.available, "bytes"),
            ("memory_total", memory.total, "bytes"),
            ("swap_percent", swap.percent, "percent"),
            ("swap_used", swap.used, "bytes"),
            ("swap_total", swap.total, "bytes"),
        ]:
            add(name, value, unit)
        for index, load in enumerate(os.getloadavg()):
            add(f"load_{(1, 5, 15)[index]}m", load, "tasks")
        failures = []
        for disk in disks:
            try:
                usage = self.api.disk_usage(disk)
                add("disk_percent", usage.percent, "percent", disk)
                add("disk_free", usage.free, "bytes", disk)
                add("disk_total", usage.total, "bytes", disk)
            except OSError:
                failures.append(
                    {
                        "service": "system",
                        "file": mask(disk),
                        "reason": "disk_unavailable",
                    }
                )
        return metrics, failures
