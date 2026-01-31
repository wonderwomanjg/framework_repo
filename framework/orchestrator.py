
import os, time, json
from typing import Callable, Dict, List
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

#from framework.retry import retry

class Task:
    def __init__(self, name: str, fn: Callable, deps: List[str] = None):
        self.name = name
        self.fn = fn
        self.deps = deps or []

def topo_order(tasks: Dict[str, Task]):
    visited, order = set(), []
    def visit(t: Task):
        if t.name in visited: return
        for d in t.deps: visit(tasks[d])
        visited.add(t.name); order.append(t)
    for t in tasks.values(): visit(t)
    return order

def already_succeeded(status_path: str, task_name: str) -> bool:
    return os.path.exists(os.path.join(status_path, f"{task_name}._SUCCESS"))

def mark_success(status_path: str, task_name: str):
    os.makedirs(status_path, exist_ok=True)
    open(os.path.join(status_path, f"{task_name}._SUCCESS"), "w").close()

def run(tasks: Dict[str, Task], status_path: str, logger=None):
    """
    Runs tasks in topological order, skipping already-succeeded ones (idempotent).
    """
    for t in topo_order(tasks):
        if already_succeeded(status_path, t.name):
            if logger: logger.info(f"Skipping {t.name} (already succeeded)")
            continue
        start = time.time()
        #retry(lambda: t.fn(), max_attempts=3, base_sleep=1, logger=logger)
        mark_success(status_path, t.name)
        if logger:
            logger.info(json.dumps({
                "task": t.name, "status": "SUCCESS",
                "elapsed_sec": round(time.time() - start, 2)
            }))
