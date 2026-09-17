import unittest
from feature_engine.events import CrawlerObservationRecorded, EventValidationError
from feature_engine.plan_status import reduce_daily_plan_status
from test_v16_domain_events import discovery_payload, recent_payload, video_payload, event


def login_discovery():
    result = discovery_payload()
    result.update(first_seen=[], first_seen_count=0, detail_success_count=0,
        discovered_count=1, silent_drop_count=0, silent_drop_video_ids=[],
        dispositions=[dict(video_id='login-video', kind='terminal_excluded',
            reason_code='login_required', retry_class=None)], recheck_dispositions=[],
        stored_count=0, deferred_count=0, terminal_excluded_count=1,
        unresolved_count=0, unresolved_video_ids=[], recheck_deferred_video_ids=[],
        recheck_deferred_count=0, pending_deferred_video_ids=[], pending_deferred_count=0,
        blocking_deferred_video_ids=[], recheck_stored_count=0, recheck_terminal_excluded_count=0,
        login_required_excluded_count=1)
    return result


class LoginRequiredEventsTest(unittest.TestCase):
    def test_login_exclusion_event_parses_and_allows_plan_completion(self):
        recent = recent_payload(partial=False)
        recent.update(success_count=3, login_required_excluded_count=1)
        source = event('video', video_payload(discovery=login_discovery(), recent_sampling=recent))
        parsed = CrawlerObservationRecorded.from_mapping(source)
        assert parsed.payload.discovery.as_facts()['login_required_excluded_count'] == 1
        assert parsed.payload.recent_sampling.as_facts()['login_required_excluded_count'] == 1
        assert parsed.payload_hash == source['payload_hash']
        assert reduce_daily_plan_status(dict(run_video=True), {'video': parsed.outcome}).status == 'succeeded'


    def test_zero_counts_and_legacy_payloads_both_round_trip(self):
        for include in (False, True):
            payload = video_payload()
            if include:
                for phase in ('discovery', 'recent_sampling'):
                    payload[phase]['payload']['login_required_excluded_count'] = 0
            parsed = CrawlerObservationRecorded.from_mapping(event('video', payload))
            for phase in ('discovery', 'recent_sampling'):
                facts = getattr(parsed.payload, phase).as_facts()
                assert ('login_required_excluded_count' in facts) == include


    def test_no_retry_is_only_allowed_for_terminal_login_exclusion(self):
        for kind, reason, retry_class in [
            ('deferred', 'login_required', None),
            ('terminal_excluded', 'private', None),
            ('terminal_excluded', 'login_required', 'low_frequency_recheck'),
        ]:
            discovery = login_discovery()
            discovery['dispositions'][0].update(kind=kind, reason_code=reason, retry_class=retry_class)
            with self.assertRaises(EventValidationError):
                CrawlerObservationRecorded.from_mapping(event('video', video_payload(discovery=discovery)))


    def test_rejects_incorrect_login_counts(self):
        discovery = login_discovery()
        discovery['login_required_excluded_count'] = 0
        with self.assertRaises(EventValidationError):
            CrawlerObservationRecorded.from_mapping(event('video', video_payload(discovery=discovery)))
        recent = recent_payload(partial=False)
        recent['login_required_excluded_count'] = 1
        with self.assertRaises(EventValidationError):
            CrawlerObservationRecorded.from_mapping(event('video', video_payload(recent_sampling=recent)))
