use lmt_core::ProcessRunSpec;
use lmt_protocol::v1alpha1::{AgentAction, PollRequest, PollResponse};

// These bytes were captured before M4 protocol work from accepted M3 commit
// 8d0c032c37d6bb34c1e398e6d68e31c20ef28881. They are compatibility artifacts,
// not fixtures generated from the current protocol structs.
const M3_POLL_REQUEST: &str = include_str!("fixtures/m3/poll-request.json");
const M3_POLL_RESPONSE: &str = include_str!("fixtures/m3/poll-response.json");
const M3_DIRECT_SPEC: &str = include_str!("fixtures/m3/direct-process-run-spec.json");

#[test]
fn accepted_m3_poll_request_bytes_remain_decodable_and_stable() {
    let request: PollRequest = serde_json::from_str(M3_POLL_REQUEST).expect("decode frozen M3 PollRequest");
    assert_eq!(request.poll_sequence, 42);
    assert_eq!(request.running.len(), 1);
    assert_eq!(
        serde_json::to_string(&request).expect("encode PollRequest"),
        M3_POLL_REQUEST.trim_end()
    );
}

#[test]
fn accepted_m3_poll_response_and_direct_spec_bytes_remain_stable() {
    let response: PollResponse = serde_json::from_str(M3_POLL_RESPONSE).expect("decode frozen M3 PollResponse");
    let direct: ProcessRunSpec = serde_json::from_str(M3_DIRECT_SPEC).expect("decode frozen M3 direct RunSpec");
    let AgentAction::StartAttempt { spec, .. } = &response.actions[0] else {
        panic!("frozen response is not StartAttempt");
    };
    assert_eq!(spec, &direct);
    assert_eq!(
        serde_json::to_string(&response).expect("encode PollResponse"),
        M3_POLL_RESPONSE.trim_end()
    );
    assert_eq!(
        serde_json::to_string(&direct).expect("encode direct RunSpec"),
        M3_DIRECT_SPEC.trim_end()
    );
}
