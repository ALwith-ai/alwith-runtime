import argparse
import hashlib
import json
import os
import queue
import subprocess
import tempfile
import threading
import time
from pathlib import Path

parser = argparse.ArgumentParser(
    description="Reproduce concurrent Runtime attention persistence without starting agents."
)
parser.add_argument("--binary", type=Path, required=True)
parser.add_argument("--serial", action="store_true")
parser.add_argument("--expect-clean", action="store_true")
args = parser.parse_args()
root = Path(tempfile.mkdtemp(prefix="alwith-attention-repro-"))
(root / "attention").mkdir()
(root / "journal").mkdir()
count = 64
states = [
    dict(
        sessionId=f"repro-{i}",
        state="done",
        reusableDraft=False,
        owner="",
        cwd=str(root),
        title=f"Repro {i}",
        since=1,
        tasks=[],
        listeningPorts=[],
        executionId=None,
        executionRole=None,
        lastActivityAt=1,
        version=1,
        interruption=None,
    )
    for i in range(count)
]
(root / "attention/run-states.json").write_text(
    json.dumps(dict(format=1, sessions=states))
)
(root / "attention/agents.json").write_text('{"format":1,"agents":[]}')
binary = args.binary.resolve(strict=True)
env = {k: v for k, v in os.environ.items() if not k.startswith("ALWITH_")}
env.update(
    ALWITH_RUNTIME_ATTENTION=str(root / "attention"),
    ALWITH_RUNTIME_JOURNAL=str(root / "journal"),
    ALWITH_RUNTIME_ENGINES=json.dumps({"unused": {"command": "/usr/bin/false"}}),
    RUST_LOG="error",
    TOKIO_WORKER_THREADS="8",
)
q = queue.Queue()
with (root / "stderr.log").open("w") as err, (root / "stdout.jsonl").open("w") as out:
    p = subprocess.Popen(
        [str(binary), "--listen", "stdio://"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=err,
        text=True,
        env=env,
    )

    def read():
        for line in p.stdout:
            out.write(line)
            out.flush()
            try:
                q.put(json.loads(line))
            except json.JSONDecodeError as error:
                q.put({"parseError": str(error)})

    threading.Thread(target=read, daemon=True).start()

    def send(v):
        p.stdin.write(json.dumps(v) + "\n")
        p.stdin.flush()

    def wait_ids(ids):
        deadline = time.monotonic() + 20
        responses = []
        while ids:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"pending ids {ids}")
            v = q.get(timeout=remaining)
            if "parseError" in v:
                raise RuntimeError(v["parseError"])
            if v.get("id") in ids:
                ids.remove(v["id"])
                responses.append(v)
        return responses

    try:
        send({"id": 0, "method": "runStates", "params": None})
        initial = wait_ids({0})
        responses = []
        for i in range(count):
            send(
                {
                    "id": i + 1,
                    "method": "markRead",
                    "params": {"sessionId": f"repro-{i}", "version": 1},
                }
            )
            if args.serial:
                responses.extend(wait_ids({i + 1}))
        if not args.serial:
            responses = wait_ids(set(range(1, count + 1)))
        send({"id": 1000, "method": "shutdown"})
        wait_ids({1000})
        p.stdin.close()
        p.wait(timeout=5)
    finally:
        if p.poll() is None:
            p.kill()
            p.wait()
log = (root / "stderr.log").read_text()
failures = [line for line in log.splitlines() if "persist run states" in line]
summary = dict(
    mode="serial" if args.serial else "concurrent",
    evidence=str(root),
    binary=str(binary),
    sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
    exit=p.returncode,
    requests=count,
    responseErrors=[r for r in responses if r.get("error")],
    persistenceErrors=len(failures),
    firstErrors=failures[:4],
    initialSessions=len(initial[0].get("result", [])),
)
(root / "summary.json").write_text(json.dumps(summary, indent=2))
print(json.dumps(summary, indent=2))

assert summary["initialSessions"] == count, (
    "Runtime did not load all synthetic snapshots"
)
assert not summary["responseErrors"], "Runtime RPC failed"
assert summary["exit"] == 0, "Runtime exited unsuccessfully"
if args.expect_clean:
    assert not failures, "Runtime persistence errors reproduced; see stderr.log"
    final = json.loads((root / "attention/run-states.json").read_text())["sessions"]
    assert len(final) == count and all(
        s["state"] == "idle" and s["version"] == 2 for s in final
    ), "Persisted snapshot is stale or incomplete"
