"""Incremental evidence collection. Missing markers are never proof of failure."""
import re

NUMBER = r"[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[DdEe][-+]?\d+)?"
G16_LINKS = {
    "l101": "输入解析", "l103": "几何优化/内坐标", "l202": "坐标建立",
    "l301": "基组/赝势", "l502": "SCF", "l508": "SCF QC/DIIS",
    "l716": "力常数/频率", "l9999": "优化步数或通用退出（需查看日志）",
}


def fresh(program):
    return {"program": program, "normal": False, "error": None, "scf": False,
            "opt": False, "current_opt": False, "frequencies": [], "frequency_seen": False,
            "frequency_complete": False, "normal_count": 0,
            "electronic": False, "ionic": False, "ediff": None,
            "nelm": None, "nsw": None, "ibrion": None, "icharg": None,
            "electronic_iteration": 0, "unconverged_steps": 0,
            "ionic_index": None, "last_energy_eV": None, "evidence": [], "tail": [], "lines": 0}


def evidence(facts, line):
    facts["evidence"].append({"line": facts["lines"], "text": line[:500]})
    facts["evidence"] = facts["evidence"][-16:]


def feed(facts, line):
    """Consume a complete line from the explicitly mapped primary log."""
    count = facts["lines"] + 1
    program = facts["program"]
    if ((program == "g16" and "Entering Gaussian System" in line)
            or (program == "vasp" and re.match(r"^\s*vasp\.\d", line))):
        facts.clear()
        facts.update(fresh(program))
    facts["lines"] = count
    facts["tail"].append({"line": count, "text": line[:500]})
    facts["tail"] = facts["tail"][-12:]
    if program == "g16":
        _g16(facts, line)
    else:
        _vasp(facts, line)


def _g16(f, line):
    link = re.search(r"(?:Enter|Leave).*?[/\\](l\d+)\.exe", line)
    if link:
        f["stage"] = G16_LINKS.get(link[1], link[1])
    step = re.search(r"Step number\s+(\d+)\s+out of a maximum of\s+(\d+)", line)
    if step:
        f["optimization_step"], f["optimization_limit"] = map(int, step.groups())
        f["stage"] = "几何优化"
    cycle = re.search(r"\bCycle\s+(\d+)\s+Pass", line)
    if cycle:
        f["scf_cycles"] = int(cycle[1])
        f["stage"] = "SCF 迭代"
        f["scf"] = False
    convergence = re.search(r"^\s*(Maximum Force|RMS\s+Force|Maximum Displacement|RMS\s+Displacement)\s+(" + NUMBER + r")\s+(" + NUMBER + r")\s+(YES|NO)\s*$", line)
    if convergence:
        metric, value, threshold, passed = convergence.groups()
        if metric == "Maximum Force":
            f["convergence"] = {}
        f.setdefault("convergence", {})[" ".join(metric.split())] = {"value": float(value.replace("D", "E")), "threshold": float(threshold.replace("D", "E")), "passed": passed == "YES"}
        f["convergence_step"] = f.get("optimization_step")
    if "Entering Link 1" in line or "Proceeding to internal job step number" in line:
        # A previous linked step's normal termination cannot finish this step.
        f["normal"] = False
        f["error"] = None
        f["scf"] = False
        f["current_opt"] = False
        f["frequencies"] = []
        f["frequency_seen"] = False
        f["frequency_complete"] = False
        for key in ("stage", "scf_cycles", "last_energy_Ha", "optimization_step", "optimization_limit", "convergence", "convergence_step"):
            f.pop(key, None)
    if "SCF Done:" in line:
        f["scf"] = True
        f["stage"] = "SCF 收敛"
        energy = re.search(r"SCF Done:.*?=\s*(" + NUMBER + r")", line)
        if energy:
            f["last_energy_Ha"] = float(energy[1].replace("D", "E"))
        cycles = re.search(r"after\s+(\d+)\s+cycles", line)
        if cycles:
            f["scf_cycles"] = int(cycles[1])
        evidence(f, line)
    if "Optimization completed" in line:
        f["opt"] = True
        f["current_opt"] = True
        f["stage"] = "几何优化完成"
        evidence(f, line)
    if "Frequencies --" in line:
        f["stage"] = "频率分析"
        f["frequency_seen"] = True
        # This is the start of a new frequency block after a completed block.
        if f["frequency_complete"]:
            f["frequencies"] = []
            f["frequency_complete"] = False
        values = line.split("Frequencies --", 1)[1].split()
        try:
            f["frequencies"].extend(float(x.replace("D", "E")) for x in values)
        except ValueError:
            f["error"] = "频率结果存在无法解析的数值"
        evidence(f, line)
    if "Zero-point correction=" in line and f["frequency_seen"]:
        f["frequency_complete"] = True
        evidence(f, line)
    if "Normal termination of Gaussian" in line:
        f["normal"] = True
        f["normal_count"] += 1
        f["stage"] = "程序正常结束"
        evidence(f, line)
    if "Error termination" in line or "Convergence failure" in line:
        match = re.search(r"(l\d+)\.exe", line)
        link = match[1] if match else "unknown"
        f["error"] = f"{link}: {G16_LINKS.get(link, '程序错误，需查看原始日志')}"
        f["normal"] = False
        f["stage"] = "程序异常"
        evidence(f, line)


def _vasp(f, line):
    if "TOTEN" in line:
        energy = re.search(r"free\s+energy\s+TOTEN\s*=\s*(" + NUMBER + r")", line)
        if energy:
            f["last_energy_eV"] = float(energy[1].replace("D", "E"))
    if "=" in line:
        for tag in ("NELM", "NSW", "IBRION", "ICHARG"):
            if tag in line:
                match = re.search(r"\b" + tag + r"\s*=\s*(-?\d+)", line)
                if match:
                    f[tag.lower()] = int(match[1])
        if "EDIFF" in line:
            match = re.search(r"\bEDIFF\s*=\s*(" + NUMBER + r")", line)
            if match:
                f["ediff"] = float(match[1].replace("D", "E"))
    match = re.search(r"\bIteration\s+(\d+)\(\s*(\d+)\)", line) if "Iteration" in line else None
    if match:
        ionic, electronic = map(int, match.groups())
        if ionic != f["ionic_index"]:
            if f["ionic_index"] is not None and not f["electronic"]:
                f["unconverged_steps"] += 1
            f["ionic_index"] = ionic
            f["ionic"] = False
        f["electronic"] = False
        f["electronic_iteration"] = electronic
        f["normal"] = False
    if "aborting loop because EDIFF is reached" in line:
        f["electronic"] = True
        evidence(f, line)
    if "reached required accuracy" in line and "structural energy" in line:
        f["ionic"] = True
        evidence(f, line)
    if "General timing and accounting informations for this job" in line:
        f["normal"] = True
        evidence(f, line)
    fatal = ("VERY BAD NEWS", "ZBRENT: fatal error", "ERROR FEXCP",
             "Error EDDDAV", "EDDDAV: Call to ZHEGV failed", "BRMIX: very serious problems")
    lowered = line.lower()
    if any(token.lower() in lowered for token in fatal):
        f["error"] = line.strip()[:300]
        evidence(f, line)


def program_result(f, task, expected_links=1, expected_imaginary=None, tolerance=0.0):
    """Result is independent of Slurm. Scheduler reconciliation happens later."""
    detail = {"termination": "normal" if f["normal"] else "unconfirmed",
              "electronic_converged": f["electronic"] if f["program"] == "vasp" else f["scf"],
              "geometry_converged": f["ionic"] if f["program"] == "vasp" else (f["current_opt"] if task == "opt" else f["opt"]),
              "evidence": f["evidence"], "tail": f["tail"], "notes": []}
    supported = {"g16": {"sp", "opt", "freq", "opt_freq"}, "vasp": {"sp", "relax"}}
    if task not in supported[f["program"]]:
        return "INSUFFICIENT", "该程序的任务类型尚未支持", detail
    if f["error"]:
        return "PROGRAM_FAILED", f["error"], detail
    if not f["normal"]:
        return "INSUFFICIENT", "未确认程序正常结束", detail
    if f["program"] == "g16":
        if f["normal_count"] < expected_links:
            return "INSUFFICIENT", "正常结束次数少于登记的 Link1 步数", detail
        if task in ("opt", "opt_freq") and not (f["current_opt"] if task == "opt" else f["opt"]):
            return "GOAL_NOT_REACHED", "程序结束，但未找到 Optimization completed", detail
        if task == "sp" and not f["scf"]:
            return "INSUFFICIENT", "程序结束，但未找到 SCF Done", detail
        if task in ("freq", "opt_freq"):
            if not f["frequency_complete"] or not f["frequencies"]:
                return "INSUFFICIENT", "频率结果或零点能汇总不完整", detail
            imaginary = sum(x < -tolerance for x in f["frequencies"])
            detail["imaginary_count"] = imaginary
            detail["notes"].append(f"虚频数 {imaginary}（阈值 < -{tolerance:g} cm^-1）；不是自动结构有效性认证")
            if expected_imaginary is not None and imaginary != expected_imaginary:
                return "GOAL_NOT_REACHED", f"虚频数 {imaginary}，登记目标为 {expected_imaginary}", detail
    else:
        if f["icharg"] is None or f["ibrion"] is None or f["nsw"] is None:
            return "INSUFFICIENT", "OUTCAR 参数信息不完整，无法核对计算类型", detail
        if f["icharg"] >= 10 or f["ediff"] is None or f["ediff"] <= 0:
            return "INSUFFICIENT", "首版仅判读 EDIFF>0 的自洽计算", detail
        if task == "relax" and (f["ibrion"] not in (1, 2, 3) or f["nsw"] <= 0):
            return "INSUFFICIENT", "登记的 relax 与 OUTCAR 参数不匹配", detail
        if task == "sp" and not (f["nsw"] == 0 or f["ibrion"] == -1):
            return "INSUFFICIENT", "登记的 sp 与 OUTCAR 参数不匹配", detail
        if not f["electronic"]:
            if f["nelm"] and f["electronic_iteration"] >= f["nelm"]:
                return "GOAL_NOT_REACHED", "末次电子迭代达到 NELM，未确认 EDIFF 收敛", detail
            return "INSUFFICIENT", "末次电子步未找到 EDIFF 收敛证据", detail
        if task == "relax" and not f["ionic"]:
            return "GOAL_NOT_REACHED", "电子收敛，但未确认离子优化达到目标", detail
        if f["unconverged_steps"]:
            return "NEEDS_REVIEW", "存在此前未确认电子收敛的离子步，需人工复核", detail
    return "SUCCEEDED", "程序正常结束，登记目标的日志判据通过", detail
