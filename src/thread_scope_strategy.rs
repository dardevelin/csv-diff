#[cfg(feature = "rayon-threads")]
use mown::Mown;

pub trait ThreadScoper<S> {
    fn scope<F>(&self, f: F)
    where
        F: FnOnce(&S) + Send;
}
#[derive(Debug, Default)]
#[cfg(feature = "crossbeam-threads")]
pub struct CrossbeamScope;

#[cfg(feature = "crossbeam-threads")]
impl<'scope> ThreadScoper<crossbeam_utils::thread::Scope<'scope>> for CrossbeamScope {
    fn scope<F>(&self, f: F)
    where
        F: FnOnce(&crossbeam_utils::thread::Scope<'scope>),
    {
        crossbeam_utils::thread::scope(|s| f(s)).unwrap();
    }
}

#[cfg(feature = "crossbeam-threads")]
impl CrossbeamScope {
    pub fn new() -> Self {
        Self
    }
}

#[derive(Debug)]
#[cfg(feature = "rayon-threads")]
pub struct RayonScope<'tp> {
    thread_pool: Mown<'tp, rayon::ThreadPool>,
}

#[cfg(feature = "rayon-threads")]
impl<'scope> ThreadScoper<rayon::Scope<'scope>> for RayonScope<'_> {
    fn scope<F>(&self, f: F)
    where
        F: FnOnce(&rayon::Scope<'scope>) + Send,
    {
        self.thread_pool.scope(|s| f(s));
    }
}

#[cfg(feature = "rayon-threads")]
impl<'tp> RayonScope<'tp> {
    pub fn with_thread_pool_ref(thread_pool: &'tp rayon::ThreadPool) -> Self {
        Self {
            thread_pool: Mown::Borrowed(thread_pool),
        }
    }

    pub fn with_thread_pool_owned(thread_pool: rayon::ThreadPool) -> Self {
        Self {
            thread_pool: Mown::Owned(thread_pool),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    #[cfg(feature = "crossbeam-threads")]
    use super::CrossbeamScope;
    #[cfg(feature = "rayon-threads")]
    use super::RayonScope;
    use super::ThreadScoper;
    #[test]
    #[cfg(feature = "crossbeam-threads")]
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
    #[cfg(feature = "rayon-threads")]
    fn rayon_scope_add_num() {
        let num = AtomicU64::new(0);
        let tp = rayon::ThreadPoolBuilder::new().build().unwrap();
        let rayon_scope = RayonScope::with_thread_pool_ref(&tp);
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
