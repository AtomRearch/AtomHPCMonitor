import contextlib
import hashlib
import io
import json
import os
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from atomhpcmonitor.cli import main, spec_from
from atomhpcmonitor.discovery import discover, gaussian_input
from atomhpcmonitor.monitor import initial_runtime, poll
from atomhpcmonitor.parsers import fresh, feed
from atomhpcmonitor.reports import render_report
from atomhpcmonitor.slurm import Slurm, parse_sacct, parse_squeue, duration, seconds
from atomhpcmonitor.store import Store

CONFIG = {"settle_seconds": 120, "unknown_seconds": 600, "read_bytes": 8388608, "poll_seconds": 300,
          "smtp": {"enabled": False}, "accounts": {
              "hpc_user": {"programs": ["g16", "vasp"], "default_logs": {"g16": "slurm-{job_id}.out", "vasp": "OUTCAR"}},
              "gaussian_user": {"programs": ["g16"], "default_logs": {"g16": "1.log"}}}}
VASP = "vasp.6.3.0\n IBRION = 2; NSW = 400; NELM = 200; ICHARG = 2; EDIFF = 1E-5\n Iteration 86( 2)\n"
G16 = "Entering Gaussian System\n (Enter /home/hpc_user/g16/l502.exe)\n Cycle  3 Pass 1\n SCF Done: E(RB3LYP) = -100.123456 A.U. after 6 cycles\n Step number 7 out of a maximum of 100\n Maximum Force 0.002000 0.000450 NO\n RMS Force 0.001000 0.000300 NO\n Maximum Displacement 0.003000 0.001800 NO\n RMS Displacement 0.001000 0.001200 YES\n"


class AllJobsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.store = Store(self.root / 'hpc_user.sqlite3')
        self.now = datetime.fromisoformat('2026-10-11T22:15:00+08:00').timestamp()
        self.submit = '2026-10-11T11:59:00'
        self.start = '2026-10-11T12:00:00'
        self.end = '2026-10-11T12:30:00'
        self.start_ts = datetime.fromisoformat(self.start + '+00:00').timestamp()
        self.end_ts = datetime.fromisoformat(self.end + '+00:00').timestamp()
        self.state = 'RUNNING'
        self.user = 'hpc_user'
        self.program = 'vasp'
        self.script = self.root / 'vasp.slurm'
        self.script.write_text('srun --mpi=pmi2 vasp_std\n')
        (self.root / 'INCAR').write_text('NSW=400; IBRION=2; ICHARG=2\n')
        self.log = self.root / 'OUTCAR'
        self.log.write_text(VASP)
        self.history = False
        self.no_details = False
        self.touch(self.now)

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def touch(self, stamp):
        for path in self.root.iterdir():
            if path.suffix not in ('.sqlite3', '.lock') and path.is_file():
                os.utime(path, (stamp, stamp))

    def gaussian(self, explicit=False):
        self.program = 'g16'
        self.script.write_text('g16 1.gjf 1.log\n' if explicit else 'g16 < 1.gjf\n')
        (self.root / '1.gjf').write_text('%mem=4GB\n#p b3lyp/6-31g(d) opt freq\n\nTitle\n\n0 1\nH 0 0 0\n')
        self.log = self.root / ('1.log' if explicit else 'slurm-88.out')
        self.log.write_text(G16)
        self.touch(self.now)

    def runner(self, args):
        if args[0] == 'squeue':
            return '' if self.history else f'88|{self.state}|{self.submit}|{self.start}|{self.user}|{self.root}|compute|sample-B_cont|06:38:30|1|node001|None|R\n'
        if args[0] == 'sacct':
            end = self.end if self.state != 'RUNNING' else 'Unknown'
            return f'88|{self.state}|0:0|0:0|{self.submit}|{self.start}|{end}|{self.user}|{self.root}|job name|compute|00:30:00|1|node001|1800\n'
        if args[0] == 'scontrol':
            if self.no_details:
                raise RuntimeError('Invalid job id specified')
            return f'JobId=88 JobName=job name UserId={self.user}(1000) SubmitTime={self.submit} StartTime={self.start} WorkDir={self.root} Command={self.script} StdOut={self.root}/slurm-%j.out StdErr={self.root}/slurm-%j.err'
        raise AssertionError(args)

    def find(self, register=False, program='all', history_hours=None):
        return discover(self.store, self.user, Slurm(self.runner), CONFIG, spec_from, register, program, history_hours, self.now)

    def observe(self):
        return poll(self.store, self.user, Slurm(self.runner), CONFIG, now=self.now)

    def past(self, state='COMPLETED'):
        self.history, self.state = True, state
        self.touch(self.end_ts)

    def test_queue_columns_match_actual_cluster_shape(self):
        row = parse_squeue(self.runner(['squeue']))[0]
        self.assertEqual(row['partition'], 'compute')
        self.assertEqual(row['job_name'], 'sample-B_cont')
        self.assertEqual(row['elapsed_seconds'], 23910)
        self.assertEqual((row['nodes'], row['node_list'], row['short_state']), ('1', 'node001', 'R'))

    def test_accounting_terminal_metadata(self):
        self.past()
        row = parse_sacct(self.runner(['sacct']))[0]
        self.assertEqual(row['state'], 'COMPLETED')
        self.assertEqual(row['elapsed_seconds'], 1800)
        self.assertEqual(row['partition'], 'compute')
        self.assertEqual(seconds('2-06:38:30'), 196710)
        self.assertEqual(duration(196710), '2-06:38:30')

    def test_g16_stdin_stdout_and_opt_freq(self):
        self.gaussian()
        self.assertEqual(self.find(True)['registered'], ['88'])
        job = self.observe()['jobs'][0]
        self.assertEqual((job['program'], job['task'], job['status']), ('g16', 'opt_freq', 'RUNNING'))
        self.assertTrue(job['log'].endswith('slurm-88.out'))
        p = job['progress']
        self.assertEqual((p['optimization_step'], p['optimization_limit'], p['scf_cycles']), (7, 100, 6))
        self.assertAlmostEqual(p['last_energy_Ha'], -100.123456)
        self.assertFalse(p['convergence']['Maximum Force']['passed'])
        self.assertTrue(p['convergence']['RMS Displacement']['passed'])

    def test_gaussian_user_explicit_gaussian_log(self):
        self.user = 'gaussian_user'
        self.gaussian(True)
        result = self.find(True)
        self.assertTrue(result['candidates'][0]['log'].endswith('1.log'))
        self.assertEqual(result['candidates'][0]['program'], 'g16')

    def test_gaussian_unsupported_routes_and_link1(self):
        for text in ('#p b3lyp/6-31g irc\n\n', '#p hf/sto-3g\n\n--Link1--\n#p freq\n\n'):
            with self.assertRaises(ValueError):
                gaussian_input(text)
        self.assertEqual(gaussian_input('#p b3lyp/6-31g\n\n'), 'sp')

    def test_gaussian_variables_and_multiple_commands_skipped(self):
        self.gaussian()
        self.script.write_text('g16 < "$input"\n')
        self.assertEqual(self.find()['candidates'], [])
        self.script.write_text('g16 1.gjf 1.log\ng16 2.gjf 2.log\n')
        self.assertEqual(self.find()['candidates'], [])

    def test_history_success_without_scontrol_and_no_old_alert(self):
        self.log.write_text(VASP + ' aborting loop because EDIFF is reached\n reached required accuracy - stopping structural energy minimisation\n General timing and accounting informations for this job:\n')
        self.past()
        self.no_details = True
        result = self.find(True, history_hours=24)
        self.assertEqual(result['registered'], ['88'])
        job = self.observe()['jobs'][0]
        self.assertEqual(job['status'], 'SUCCEEDED')
        self.assertTrue(job['historical_import'])
        self.assertEqual(self.store.outbox(self.user), [])

    def test_history_timeout_without_output(self):
        self.past('TIMEOUT')
        self.log.unlink()
        self.assertEqual(self.find(True, history_hours=24)['registered'], ['88'])
        self.assertEqual(self.observe()['jobs'][0]['status'], 'TIMEOUT')

    def test_cancelled_before_start_does_not_adopt_previous_log(self):
        self.past('CANCELLED')
        self.start = 'Unknown'
        self.no_details = True
        self.log.write_text(VASP + ' General timing and accounting informations for this job:\n')
        self.touch(self.end_ts)
        result = self.find(True, history_hours=24)
        self.assertEqual(result['registered'], ['88'])
        self.assertFalse(result['candidates'][0]['adopt_existing'])
        self.assertEqual(self.observe()['jobs'][0]['status'], 'CANCELLED')

    def test_history_modified_log_or_input_not_adopted(self):
        self.past()
        os.utime(self.log, (self.now, self.now))
        self.assertEqual(self.find(history_hours=24)['candidates'], [])
        os.utime(self.log, (self.end_ts, self.end_ts))
        os.utime(self.root / 'INCAR', (self.now, self.now))
        self.assertEqual(self.find(history_hours=24)['candidates'], [])

    def test_history_ambiguous_script_not_adopted(self):
        self.past()
        self.no_details = True
        (self.root / 'other.slurm').write_text('srun vasp_std\n')
        self.assertEqual(self.find(history_hours=24)['candidates'], [])

    def test_history_window_filters_old_jobs(self):
        self.past()
        result = discover(self.store, self.user, Slurm(self.runner), CONFIG, spec_from, False, 'all', 1, self.now)
        self.assertEqual(result['candidates'], [])

    def test_discovery_and_history_do_not_modify_calculation_files(self):
        self.past()
        paths = [self.script, self.root / 'INCAR', self.log]
        before = [hashlib.sha256(p.read_bytes()).hexdigest() for p in paths]
        self.find(True, history_hours=24)
        self.observe()
        self.assertEqual(before, [hashlib.sha256(p.read_bytes()).hexdigest() for p in paths])

    def test_completed_snapshot_survives_log_reuse_and_missing_accounting(self):
        self.gaussian(True)
        self.log.write_text(G16 + 'Optimization completed.\nFrequencies -- 100 200 300\nZero-point correction= 0.01\nNormal termination of Gaussian 16\n')
        self.touch(self.now)
        self.find(True)
        self.history, self.state = True, 'COMPLETED'
        self.assertEqual(self.observe()['jobs'][0]['status'], 'SUCCEEDED')
        self.log.write_text('Entering Gaussian System\nCycle  2 Pass 1\n')
        class Missing:
            def query(inner, account, ids):
                return {jid: {'job_id': jid, 'state': 'UNKNOWN', 'submit': None, 'start': None, 'query_errors': []} for jid in ids}
        job = poll(self.store, self.user, Missing(), CONFIG, now=self.now + 300)['jobs'][0]
        self.assertEqual(job['status'], 'SUCCEEDED')
        self.assertTrue(job['terminal_snapshot'])
        second = spec_from({'job_id': '89', 'program': 'g16', 'task': 'sp', 'workdir': str(self.root), 'log': '1.log', 'adopt_existing': True}, self.user, CONFIG)
        self.store.add(second, initial_runtime(second))
        self.assertEqual(len(self.store.jobs(self.user)), 2)

    def test_frozen_job_requeue_never_uses_old_success(self):
        self.log.write_text(VASP + ' aborting loop because EDIFF is reached\n reached required accuracy - stopping structural energy minimisation\n General timing and accounting informations for this job:\n')
        self.find(True)
        self.history, self.state = True, 'COMPLETED'
        self.assertEqual(self.observe()['jobs'][0]['status'], 'SUCCEEDED')
        self.history, self.state = False, 'RUNNING'
        self.start = '2026-10-11T14:00:00'
        job = self.observe()['jobs'][0]
        self.assertEqual(job['status'], 'RUNNING')
        self.assertTrue(any('重排队' in x for x in job['issues']))
        self.history, self.state = True, 'COMPLETED'
        self.end = '2026-10-11T14:10:00'
        self.assertEqual(self.observe()['jobs'][0]['status'], 'INSUFFICIENT')

    def test_gaussian_new_step_table_does_not_mix_previous_rows(self):
        facts = fresh('g16')
        for line in G16.splitlines():
            feed(facts, line)
        feed(facts, 'Step number 8 out of a maximum of 100')
        feed(facts, 'Maximum Force 0.001000 0.000450 NO')
        self.assertEqual(list(facts['convergence']), ['Maximum Force'])
        self.assertEqual(facts['convergence_step'], 8)

    def test_report_contains_all_statuses_and_resources(self):
        self.find(True)
        job = self.observe()['jobs'][0]
        completed = {**job, 'job_id': '90', 'status': 'SUCCEEDED'}
        failed = {**job, 'job_id': '91', 'status': 'TIMEOUT'}
        report = render_report(self.user, {'jobs': [job, completed, failed]}, ['g16', 'vasp'], {'timezone': 'UTC+08:00'}, self.now)
        self.assertEqual(report['payload']['counts'], {'RUNNING': 1, 'SUCCEEDED': 1, 'TIMEOUT': 1})
        for token in ('⏳', '✅', '⌛', 'PARTITION', 'compute', '06:38:30', 'node001', 'NODES'):
            self.assertIn(token, report['html_body'])

    def test_cli_all_dry_run_keeps_database_empty(self):
        self.gaussian()
        config_path = self.root / 'config.json'
        config_path.write_text(json.dumps({**CONFIG, 'state_dir': str(self.root)}))
        output = io.StringIO()
        with patch('atomhpcmonitor.cli.current_user', return_value='hpc_user'), patch('atomhpcmonitor.cli.Slurm', return_value=Slurm(self.runner)), contextlib.redirect_stdout(output):
            code = main(['--config', str(config_path), '--account', self.user, 'poll', '--discover', 'all', '--dry-run'])
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(output.getvalue())['jobs'][0]['program'], 'g16')
        self.assertEqual(self.store.jobs(self.user), [])


if __name__ == '__main__':
    unittest.main()
