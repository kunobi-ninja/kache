//! Apply the application's delegation policy to shared token evidence.

use kunobi_daemon::local::security::SecurityContextComparison;

#[cfg(windows)]
pub(crate) fn matches_daemon(
    process: std::os::windows::io::BorrowedHandle<'_>,
    trusted_pipeline: bool,
) -> std::io::Result<bool> {
    delegation_problem(process, trusted_pipeline).map(|problem| problem.is_none())
}

#[cfg(windows)]
pub(crate) fn delegation_problem(
    process: std::os::windows::io::BorrowedHandle<'_>,
    trusted_pipeline: bool,
) -> std::io::Result<Option<&'static str>> {
    kunobi_daemon::local::security::compare_with_current_process(process)
        .map(|comparison| delegation_problem_for(comparison, trusted_pipeline))
}

fn delegation_problem_for(
    comparison: SecurityContextComparison,
    trusted_pipeline: bool,
) -> Option<&'static str> {
    match comparison {
        SecurityContextComparison::Equivalent => None,
        SecurityContextComparison::ProcessIdentityDiffers if trusted_pipeline => None,
        SecurityContextComparison::ProcessIdentityDiffers => Some(
            "Windows process-identity token attributes differ; trusted daemon execution requires an explicit opt-in on both peers",
        ),
        _ => Some("Windows access-token contexts are incompatible or unsupported"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_process_identity_differences_can_use_the_trust_opt_in() {
        for trusted in [false, true] {
            assert_eq!(
                delegation_problem_for(SecurityContextComparison::Equivalent, trusted),
                None
            );
            assert_eq!(
                delegation_problem_for(SecurityContextComparison::ProcessIdentityDiffers, trusted),
                if trusted {
                    None
                } else {
                    Some(
                        "Windows process-identity token attributes differ; trusted daemon execution requires an explicit opt-in on both peers",
                    )
                }
            );
            assert_eq!(
                delegation_problem_for(SecurityContextComparison::Incompatible, trusted),
                Some("Windows access-token contexts are incompatible or unsupported")
            );
        }
    }
}
