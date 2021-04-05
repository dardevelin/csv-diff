pub trait ThreadScoper<S> {
    fn scope<F>(f: F)
    where
        for<'a> F: FnOnce(&'a S) + Send;
}

pub struct CrossbeamScope {}

impl<'scope> ThreadScoper<crossbeam_utils::thread::Scope<'scope>> for CrossbeamScope {
    fn scope<F>(f: F)
    where
        for<'a> F: FnOnce(&'a crossbeam_utils::thread::Scope<'scope>),
    {
        crossbeam_utils::thread::scope(|s| f(s)).unwrap();
    }
}

struct RayonScope {}

impl<'scope> ThreadScoper<rayon::Scope<'scope>> for RayonScope {
    fn scope<F>(f: F)
    where
        for<'a> F: FnOnce(&'a rayon::Scope<'scope>) + Send,
    {
        rayon::scope(|s| f(s));
    }
}

mod tests {
    use super::{CrossbeamScope, RayonScope, ThreadScoper};
    #[test]
    fn crossbeam_scope_add_num() {
        let num = std::sync::atomic::AtomicU64::new(0);
        CrossbeamScope::scope(|s| {
            s.spawn(|_s1| {
                num.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            });
            s.spawn(|_s2| {
                num.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            });
        });
        assert_eq!(2, num.into_inner());
    }

    #[test]
    fn rayon_scope_add_num() {
        let num = std::sync::atomic::AtomicU64::new(0);
        RayonScope::scope(|s| {
            s.spawn(|_s1| {
                num.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            });
            s.spawn(|_s2| {
                num.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            });
        });
        assert_eq!(2, num.into_inner());
    }
}
