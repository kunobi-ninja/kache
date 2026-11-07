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

    def test_bare_and_wrapped_env_override_cargo_config_wrappers(self):
        inherited = {'PATH': 'native', 'RUSTC_WRAPPER': 'old-wrapper',
                     'RUSTC_WORKSPACE_WRAPPER': 'old-workspace-wrapper', 'CARGO_BUILD_JOBS': '2'}
        bare = helper.workload_environment(inherited, Path('target'))
        wrapped = helper.workload_environment(inherited, Path('target'), Path('pinned/kache.exe'))
        self.assertEqual(bare['RUSTC_WRAPPER'], '')
        self.assertEqual(wrapped['RUSTC_WRAPPER'], str(Path('pinned/kache.exe')))
        for env in (bare, wrapped):
            self.assertEqual(env['RUSTC_WORKSPACE_WRAPPER'], '')
            self.assertNotIn('CARGO_BUILD_JOBS', env)
        self.assertEqual(inherited['RUSTC_WRAPPER'], 'old-wrapper')

    def test_collects_original_report_schema_stats_and_trace_separately(self):
        with patch.object(helper, 'capture') as capture:
            helper.export_stats('pinned/kache.exe', Path('workload'), {}, Path('evidence'), object())
        self.assertEqual([call.args[0] for call in capture.call_args_list], [
            ['pinned/kache.exe', 'report', '--format', 'json'],
            ['pinned/kache.exe', 'stats', '--format', 'json'],
            ['pinned/kache.exe', 'stats', '--format', 'trace']])
        self.assertEqual([call.args[3].name for call in capture.call_args_list], ['report', 'stats', 'timeline'])

    def test_short_names_only_widen_prefilter_not_authoritative_image_match(self):
        names = {'kache.exe', 'kache-engine.exe'}
        for name in ('kache.exe', 'KACHE-ENGINE.EXE', 'KACHE-~1.EXE', 'KACHE-~2.EXE', 'KACHE~1.EXE'):
            self.assertTrue(helper.candidate_process_name(name, names), name)
        for name in ('chrome.exe', 'KACHE-~.EXE', 'KACHE-~1.EXE.cmd', 'KACHE-~0.EXE'):
            self.assertFalse(helper.candidate_process_name(name, names), name)
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            pinned = root / 'pinned/kache-engine.exe'
            pinned.parent.mkdir()
            pinned.write_text('pinned')
            unrelated = root / 'other/KACHE-~1.EXE'
            unrelated.parent.mkdir()
            unrelated.write_text('other')
            expected = {os.path.normcase(str(pinned.resolve())): 'kache-engine.exe'}
            self.assertEqual(helper.pinned_image(pinned, expected), 'kache-engine.exe')
            self.assertTrue(helper.candidate_process_name(unrelated.name, names))
            self.assertIsNone(helper.pinned_image(unrelated, expected))

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


class TimedRouteGateGuards(unittest.TestCase):
    def write_report(self, evidence):
        (evidence / 'report.stdout').write_text(json.dumps({'all_events': [
            {'crate_name': helper.TIMED_UNIT, 'compiler_runs': 1},
            {'crate_name': helper.TIMED_UNIT, 'compiler_runs': 1},
        ]}))

    def compiler(self, route, timestamp=100):
        return {'unit': helper.TIMED_UNIT, 'route': route, 'pipeline_markers': ['compiler'],
                'pid': 42, 'start_us': timestamp, 'duration_us': 20}

    def test_positive_local_or_daemon_compiler_phase_qualifies(self):
        with tempfile.TemporaryDirectory() as temp:
            evidence = Path(temp)
            self.write_report(evidence)
            for route in ('daemon_pipeline', 'local_engine_pipeline', 'client_pipeline'):
                helper.require_timed_route({'traces': [
                    self.compiler(route), self.compiler(route, timestamp=120),
                    {'unit': helper.TIMED_UNIT, 'route': 'unknown', 'pipeline_markers': []},
                ]}, evidence)
                self.assertTrue(json.loads((evidence / 'timed-route-gate.json').read_text())['passed'])

    def test_absent_irrelevant_proxy_and_partly_unknown_compilers_stop(self):
        cases = (
            [],
            [{'unit': 'other_crate', 'route': 'daemon_pipeline', 'pipeline_markers': ['compiler']}],
            [{'unit': helper.TIMED_UNIT, 'route': 'unknown', 'pipeline_markers': []}],
            [self.compiler('unknown')],
            [self.compiler('daemon_pipeline'), self.compiler('unknown', timestamp=120)],
            [self.compiler('daemon_pipeline')],  # Short library known, final test trace missing.
            [self.compiler('daemon_pipeline'), self.compiler('daemon_pipeline')],  # Duplicate cannot cover the test.
        )
        with tempfile.TemporaryDirectory() as temp:
            evidence = Path(temp)
            self.write_report(evidence)
            for records in cases:
                with self.subTest(records=records), self.assertRaises(RuntimeError):
                    helper.require_timed_route({'traces': records}, evidence)
                receipt = json.loads((evidence / 'timed-route-gate.json').read_text())
                self.assertFalse(receipt['passed'])
                self.assertEqual(receipt['unit'], helper.TIMED_UNIT)


if __name__ == '__main__':
    unittest.main()
