"""Keep evidence across services when representative input is bounded."""

from collections import Counter, defaultdict

IMPORTANCE = {"UNKNOWN": 0, "TRACE": 0, "DEBUG": 0, "INFO": 1, "WARNING": 2,
              "ERROR": 3, "FATAL": 4, "CRITICAL": 5}


def balanced_groups(values, limit=100):
    services = defaultdict(list)
    for value in values:
        services[value["service"]].append(value)
    for values in services.values():
        values.sort(key=lambda item: (-IMPORTANCE.get(item["level"], 0), -item["count"]))
    names = sorted(services, key=lambda name: (-IMPORTANCE.get(services[name][0]["level"], 0), name))
    selected = []
    while names and len(selected) < limit:
        remaining = []
        for name in names:
            if len(selected) == limit:
                break
            selected.append(services[name].pop(0))
            if services[name]:
                remaining.append(name)
        names = remaining
    return selected


def evidence_bucket(item, field):
    service = item.get("service", "unknown")
    # Preserve a measured-latency sample in addition to ordinary log evidence.
    if field == "events" and item.get("metadata", {}).get("response_time_ms") is not None:
        return (service, "latency")
    return (service, "logs")


def trim_daily_detail(snapshot):
    for field in ("events", "error_groups"):
        values = snapshot.get(field, [])
        counts = Counter(evidence_bucket(item, field) for item in values)
        candidates = [i for i, item in enumerate(values) if counts[evidence_bucket(item, field)] > 1]
        if candidates:
            index = max(candidates, key=lambda i: (
                counts[evidence_bucket(values[i], field)],
                -IMPORTANCE.get(values[i].get("level"), 0),
                -values[i].get("count", 1), len(values[i].get("message", "")),
            ))
            values.pop(index)
            return True
    for field, bound in (("events", 1000), ("error_groups", 300)):
        values = [item for item in snapshot.get(field, []) if len(item.get("message", "")) > bound]
        if values:
            value = max(values, key=lambda item: len(item["message"]))
            suffix = " [input truncated]"
            value["message"] = value["message"][:bound - len(suffix)] + suffix
            return True
    # These are optional detail; aggregate counts and the omission notice remain.
    for field in ("error_groups", "events", "metrics"):
        values = snapshot.get(field, [])
        if values:
            if field != "metrics":
                index = min(range(len(values)), key=lambda i: IMPORTANCE.get(values[i].get("level"), 0))
            else:
                counts = Counter(item.get("target") for item in values)
                index = max(range(len(values)), key=lambda i: counts[values[i].get("target")])
            values.pop(index)
            return True
    return False
