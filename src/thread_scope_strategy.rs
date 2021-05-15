pub trait ThreadScoper<S>: Default {
    fn scope<F>(&self, f: F)
    where
        F: FnOnce(&S) + Send;
    //fn build_default_thread_scoper() -> Self;
}
#[derive(Default)]
pub struct CrossbeamScope;

impl<'scope> ThreadScoper<crossbeam_utils::thread::Scope<'scope>> for CrossbeamScope {
    fn scope<F>(&self, f: F)
    where
        F: FnOnce(&crossbeam_utils::thread::Scope<'scope>),
    {
        crossbeam_utils::thread::scope(|s| f(s)).unwrap();
    }
}

impl CrossbeamScope {
    pub fn new() -> Self {
        Self
    }
}

pub struct RayonScope {
    thread_pool: rayon::ThreadPool,
}

impl<'scope> ThreadScoper<rayon::Scope<'scope>> for RayonScope {
    fn scope<F>(&self, f: F)
    where
        F: FnOnce(&rayon::Scope<'scope>) + Send,
    {
        self.thread_pool.scope(|s| f(s));
    }
}

impl Default for RayonScope {
    fn default() -> Self {
        Self {
            thread_pool: rayon::ThreadPoolBuilder::new().build().unwrap(),
        }
    }
}

impl RayonScope {
    pub fn new(thread_pool: rayon::ThreadPool) -> Self {
        Self { thread_pool }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::{CrossbeamScope, RayonScope, ThreadScoper};
    #[test]
    fn crossbeam_scope_add_num() {
        let num = AtomicU64::new(0);
        let crossbeam_scope = CrossbeamScope::new();
        crossbeam_scope.scope(|s| {
            s.spawn(|_s1| {
                num.fetch_add(1, Ordering::SeqCst);
            });
            s.spawn(|_s2| {
                num.fetch_add(1, Ordering::SeqCst);
            });
        });
        assert_eq!(2, num.into_inner());
    }

    #[test]
    fn rayon_scope_add_num() {
        let num = AtomicU64::new(0);
        let rayon_scope = RayonScope::new(rayon::ThreadPoolBuilder::new().build().unwrap());
        rayon_scope.scope(|s| {
            s.spawn(|_s1| {
                num.fetch_add(1, Ordering::SeqCst);
            });
            s.spawn(|_s2| {
                num.fetch_add(1, Ordering::SeqCst);
            });
        });
        assert_eq!(2, num.into_inner());
    }
}
