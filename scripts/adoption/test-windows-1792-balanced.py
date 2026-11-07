#!/usr/bin/env python3
"""Inert guards: synthetic records only; never spawn the workload or Kache."""

import ast
from copy import deepcopy
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

HELPER = Path(__file__).with_name('windows-1792-balanced.py')
spec = importlib.util.spec_from_file_location('balanced', HELPER)
helper = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helper)


def fixture(pid=42, binary='kache-engine.exe', daemon_pid=42):
    trace = {'kache_trace': {'version': 1, 'clock': 'monotonic', 'scope': 'wrapper-thread'},
             'traceEvents': [
                 {'name': 'owned_probe', 'cat': 'rustc', 'ph': 'X', 'pid': pid, 'ts': 200,
                  'dur': 100, 'args': {'dropped_events': 0}},
                 {'name': 'compiler', 'cat': 'rustc', 'ph': 'X', 'pid': pid, 'ts': 210, 'dur': 50}]}
    receipt = {'processes': [{'pid': pid, 'start_filetime': 7, 'start_us': 100,
                             'observed_us': 250, 'binary': binary}],
               'daemon_identities': [{'pid': daemon_pid, 'start_filetime': 7}], 'monitor_errors': []}
    return trace, receipt


class RouteGuards(unittest.TestCase):
    def test_real_pipeline_requires_pid_and_start_identity(self):
        trace, receipt = fixture()
        self.assertEqual(helper.classify_trace(trace, receipt)['route'], 'daemon_pipeline')
        receipt['processes'][0]['start_us'] = 201
        self.assertEqual(helper.classify_trace(trace, receipt)['route'], 'unknown')

    def test_proxy_root_does_not_prove_delegation(self):
        trace, receipt = fixture()
        trace['traceEvents'] = trace['traceEvents'][:1]
        self.assertEqual(helper.classify_trace(trace, receipt)['route'], 'unknown')

    def test_local_engine_and_client_are_positive_binary_observations(self):
        trace, receipt = fixture(daemon_pid=77)
        # A different engine PID alone does not establish local forwarding.
        self.assertEqual(helper.classify_trace(trace, receipt)['route'], 'unknown')
        receipt['processes'][0]['parent_pid'] = 21
        receipt['processes'].append({'pid': 21, 'start_filetime': 6, 'start_us': 80,
                                     'observed_us': 250, 'binary': 'kache.exe'})
        self.assertEqual(helper.classify_trace(trace, receipt)['route'], 'local_engine_pipeline')
        receipt['processes'][0]['binary'] = 'kache.exe'
        self.assertEqual(helper.classify_trace(trace, receipt)['route'], 'client_pipeline')
        receipt['processes'].clear()
        self.assertEqual(helper.classify_trace(trace, receipt)['route'], 'unknown')

    def test_worker_dropped_mixed_restarted_and_absent_are_unknown(self):
        original_trace, original_receipt = fixture()
        for mutate in (
            lambda t, r: t['kache_trace'].update(scope='worker-thread'),
            lambda t, r: t['traceEvents'][0]['args'].update(dropped_events=1),
            lambda t, r: t['traceEvents'][1].update(pid=99),
            lambda t, r: r['daemon_identities'].append({'pid': 43, 'start_filetime': 8}),
            lambda t, r: r['daemon_identities'].clear(),
            lambda t, r: r['monitor_errors'].append('OSError'),
            lambda t, r: r['processes'].append(deepcopy(r['processes'][0])),
        ):
            trace, receipt = deepcopy(original_trace), deepcopy(original_receipt)
            mutate(trace, receipt)
            with self.subTest(trace=trace, receipt=receipt):
                self.assertEqual(helper.classify_trace(trace, receipt)['route'], 'unknown')

    def test_corrupt_trace_records_unknown_and_keeps_valid_observations(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / 'phase-traces').mkdir()
            (root / 'phase-traces/bad.trace.json').write_text('{')
            trace, receipt = fixture()
            (root / 'phase-traces/good.trace.json').write_text(json.dumps(trace))
            monitor = type('Monitor', (), {'receipt': lambda self: deepcopy(receipt)})()
            result = helper.route_receipt(root, root, monitor)
            self.assertEqual(result['counts'], {'unknown': 1, 'daemon_pipeline': 1})


class IsolationGuards(unittest.TestCase):
    def test_environment_drops_tokens_wrappers_and_policy_overrides(self):
        with patch.dict(os.environ, {'PATH': 'native', 'LIB': 'sdk', 'GITHUB_TOKEN': 'secret',
                         'ACTIONS_RUNTIME_TOKEN': 'secret', 'AWS_SECRET_ACCESS_KEY': 'secret',
                         'RUSTC_WRAPPER': 'other', 'KACHE_SCHEDULER': '0'}, clear=True):
            self.assertEqual(helper.environment(), {'PATH': 'native', 'LIB': 'sdk'})

    def test_private_configs_differ_only_in_supported_scheduler_setting(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            first = helper.private_config(root / 'first')
            second = helper.private_config(root / 'second', scheduler=False)
            first_text = Path(first['KACHE_CONFIG']).read_text()
            second_text = Path(second['KACHE_CONFIG']).read_text()
            self.assertEqual(first_text.replace('scheduler=true', 'scheduler=false'), second_text)
            self.assertEqual(first['KACHE_HOST_CONFIG'], '')
            for env in (first, second):
                self.assertNotEqual(env['KACHE_RUNTIME_DIR'], env['KACHE_CACHE_DIR'])
            with self.assertRaises(FileExistsError):
                helper.private_config(root / 'first')

    def test_evidence_export_never_copies_private_daemon_record_or_cache(self):
        with tempfile.TemporaryDirectory() as temp:
            root, evidence = Path(temp) / 'root', Path(temp) / 'evidence'
            (root / 'run').mkdir(parents=True)
            evidence.mkdir()
            (root / 'run/daemon.state.json').write_text('private')
            (root / 'run/daemon.record').write_text('authentication')
            (root / 'run/events.jsonl').write_text('events')
            (root / 'cache').mkdir()
            (root / 'cache/index.db').write_text('private database')
            helper.copy_evidence(root, root / 'target', evidence)
            self.assertEqual([path.name for path in evidence.iterdir()], ['run-events.jsonl'])

    def test_latin_square_has_every_arm_in_every_position(self):
        self.assertEqual(len(helper.ROUNDS), 3)
        for arms in (*helper.ROUNDS, *zip(*helper.ROUNDS)):
            self.assertEqual(set(arms), {'bare', 'default', 'disabled'})

    def test_budget_clips_and_fails_before_global_deadline(self):
        with patch.object(helper.time, 'time', return_value=5000):
            self.assertEqual(helper.Budget(100).limit(1200), 20)
            with self.assertRaises(TimeoutError):
                helper.Budget(0).limit(1200)

    def test_bare_path_has_no_kache_start_stats_or_trace(self):
        tree = ast.parse(HELPER.read_text())
        run_case = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == 'run_case')
        # Guard the actual bare execution branch, not an independently copied model.
        execution = next(node for node in ast.walk(run_case)
                         if isinstance(node, ast.If) and isinstance(node.test, ast.Name)
                         and node.test.id == 'wrapped' and any(isinstance(child, ast.With) for child in node.body))
        calls = [node for child in execution.orelse for node in ast.walk(child) if isinstance(node, ast.Call)]
        self.assertEqual([node.func.id for node in calls if isinstance(node.func, ast.Name)], ['capture'])


if __name__ == '__main__':
    unittest.main()
