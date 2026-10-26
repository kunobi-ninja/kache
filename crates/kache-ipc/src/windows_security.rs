//! Apply the application's delegation policy to shared token evidence.

use kunobi_daemon::local::security::SecurityContextComparison;

#[cfg(windows)]
pub(crate) fn matches_daemon(
    process: std::os::windows::io::BorrowedHandle<'_>,
    trusted_pipeline: bool,
) -> std::io::Result<bool> {
    kunobi_daemon::local::security::compare_with_current_process(process)
        .map(|comparison| permits_delegation(comparison, trusted_pipeline))
}

fn permits_delegation(comparison: SecurityContextComparison, trusted_pipeline: bool) -> bool {
    match comparison {
        SecurityContextComparison::Equivalent => true,
        SecurityContextComparison::ProcessIdentityDiffers => trusted_pipeline,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_process_identity_differences_can_use_the_trust_opt_in() {
        for trusted in [false, true] {
            assert!(permits_delegation(
                SecurityContextComparison::Equivalent,
                trusted
            ));
            assert_eq!(
                permits_delegation(SecurityContextComparison::ProcessIdentityDiffers, trusted),
                trusted
            );
            assert!(!permits_delegation(
                SecurityContextComparison::Incompatible,
                trusted
            ));
        }
    }
}
