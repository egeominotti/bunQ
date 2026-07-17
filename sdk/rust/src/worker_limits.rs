pub(crate) fn pull_count(batch_size: usize, concurrency: usize) -> usize {
    batch_size.min(concurrency)
}

#[cfg(test)]
mod tests {
    use super::pull_count;

    #[test]
    fn pull_count_never_leases_more_jobs_than_can_be_heartbeated() {
        assert_eq!(pull_count(10, 4), 4);
        assert_eq!(pull_count(2, 4), 2);
    }
}
