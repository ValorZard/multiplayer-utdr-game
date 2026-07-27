//! A small `macro_rules!` implementation of the actor pattern described in
//! <https://ryhl.io/blog/actors-with-tokio/>.
//!
//! An actor is a plain struct that owns its state and is only ever touched by
//! one task. Everything else talks to it through a cheap, cloneable handle that
//! sends messages down an `mpsc` channel. Writing that by hand means spelling
//! out the same three things for every method: a message enum variant, a match
//! arm that calls the method and replies, and an `async` handle method that
//! builds the message and awaits the reply. [`define_actor`] writes all three
//! from a single declaration, so the handle can never drift from the actor.
//!
//! Methods come in two flavours:
//!
//! - `ask` methods return a value, so the message carries a
//!   [`oneshot::Sender`](tokio::sync::oneshot::Sender) and the handle method
//!   awaits the reply.
//! - `tell` methods return nothing, so the handle method returns as soon as the
//!   message is queued and never waits for the actor.
//!
//! An actor can also declare a `tick`: a method the run loop calls on a fixed
//! interval whenever it isn't busy with a message. That covers actors that have
//! to make progress on their own rather than purely on request, like a game
//! session stepping its simulation.
//!
//! # What the macro generates
//!
//! - the message enum,
//! - `Actor::handle_message`, which dispatches one message,
//! - `Actor::run`, the loop that pulls messages off the receiver, calls the
//!   tick method on schedule, and returns once every handle has been dropped,
//! - the handle struct, one `async` method per declared actor method, and a
//!   `spawn` constructor that takes the actor's own constructor arguments,
//!   creates the channel, and puts the actor on the runtime.
//!
//! # What you still write by hand
//!
//! - the actor struct, which **must** have a field named `receiver` of type
//!   `mpsc::Receiver<Message>`,
//! - the actor's constructor, which **must** take that receiver as its last
//!   argument,
//! - the actor methods themselves.
//!
//! # Example
//!
//! ```ignore
//! struct Counter {
//!     count: u64,
//!     receiver: mpsc::Receiver<CounterMessage>,
//! }
//!
//! impl Counter {
//!     fn new(start: u64, receiver: mpsc::Receiver<CounterMessage>) -> Self {
//!         Self { count: start, receiver }
//!     }
//!
//!     fn get(&self) -> u64 {
//!         self.count
//!     }
//!
//!     fn add(&mut self, amount: u64) {
//!         self.count += amount;
//!     }
//! }
//!
//! define_actor! {
//!     actor Counter;
//!     message CounterMessage;
//!     /// Cloneable handle to a running [`Counter`].
//!     pub handle CounterHandle;
//!
//!     spawn with 32 => fn new(start: u64);
//!
//!     ask {
//!         Get => fn get() -> u64;
//!     }
//!
//!     tell {
//!         Add => fn add(amount: u64);
//!     }
//! }
//! ```
//!
//! `CounterHandle::spawn(7)` now starts a counter at 7, and the handle has
//! `async fn get(&self) -> u64` and `async fn add(&self, amount: u64)`,
//! mirroring `Counter`.

/// Generates the message enum, the actor's dispatch and run loop, and a handle
/// whose `async` methods mirror the actor's.
///
/// See the [module docs](self) for the surrounding code you still write
/// yourself.
///
/// # Syntax
///
/// ```ignore
/// define_actor! {
///     actor ActorType;
///     message MessageType;
///     handle HandleType;
///
///     // optional
///     spawn with CHANNEL_CAPACITY => fn actor_constructor(arg: ArgType, ..);
///     // optional
///     tick every Duration::from_millis(33) => fn tick_method();
///
///     ask {
///         VariantName => fn method_name(arg: ArgType, ..) -> ReturnType;
///     }
///
///     tell {
///         VariantName => fn method_name(arg: ArgType, ..);
///     }
/// }
/// ```
///
/// The `ask` and `tell` blocks are required but either may be empty. The
/// `message` and `handle` declarations accept a visibility and attributes
/// (including doc comments); individual methods accept attributes, which are
/// applied to both the enum variant and the generated handle method.
///
/// Every listed method must exist on the actor, taking `&self` or `&mut self`
/// plus the listed argument types in order. The generated handle methods are
/// always `pub`, and their argument names come from this declaration rather than
/// from the actor. The tick method takes no arguments and returns nothing: a
/// tick can't fail the run loop, so it has to deal with its own errors.
///
/// `spawn with .. => fn ..` names the actor's own constructor and lists the
/// arguments it takes *before* the receiver, which the generated
/// `Handle::spawn` appends for you. Leave the whole line out if the actor needs
/// a constructor this can't express (a fallible or `async` one, say) and write
/// a constructor on the handle by hand instead: `Actor::run` and the handle's
/// `sender` field are both reachable from the surrounding module.
///
/// # Panics
///
/// A generated `ask` method panics if the actor task died before replying,
/// which can only happen if the actor's run loop panicked. `tell` methods never
/// panic; the message is silently dropped instead.
macro_rules! define_actor {
    (
        actor $actor:ident;

        $(#[$message_meta:meta])*
        $message_vis:vis message $message:ident;

        $(#[$handle_meta:meta])*
        $handle_vis:vis handle $handle:ident;

        $(
            $(#[$spawn_meta:meta])*
            spawn with $capacity:expr => fn $constructor:ident(
                $($ctor_arg:ident: $ctor_arg_ty:ty),* $(,)?
            );
        )?

        // No attributes are accepted on this section: a second optional
        // section starting with `#[..]` would be ambiguous with the one above.
        $( tick every $tick_period:expr => fn $tick_method:ident(); )?

        ask {
            $(
                $(#[$ask_meta:meta])*
                $ask_variant:ident => fn $ask_method:ident(
                    $($ask_arg:ident: $ask_arg_ty:ty),* $(,)?
                ) -> $ask_ret:ty;
            )*
        }

        tell {
            $(
                $(#[$tell_meta:meta])*
                $tell_variant:ident => fn $tell_method:ident(
                    $($tell_arg:ident: $tell_arg_ty:ty),* $(,)?
                );
            )*
        }
    ) => {
        $(#[$message_meta])*
        $message_vis enum $message {
            $(
                $(#[$ask_meta])*
                $ask_variant($($ask_arg_ty,)* ::tokio::sync::oneshot::Sender<$ask_ret>),
            )*
            $(
                $(#[$tell_meta])*
                $tell_variant($($tell_arg_ty),*),
            )*
        }

        impl $actor {
            /// Runs one message against the actor's state.
            fn handle_message(&mut self, message: $message) {
                match message {
                    $(
                        $message::$ask_variant($($ask_arg,)* reply) => {
                            let result = self.$ask_method($($ask_arg),*);
                            // The caller may have given up on the reply, which
                            // is none of the actor's business.
                            let _ = reply.send(result);
                        }
                    )*
                    $(
                        $message::$tell_variant($($tell_arg),*) => {
                            self.$tell_method($($tell_arg),*);
                        }
                    )*
                }
            }

            /// Owns the actor for as long as it lives, handling messages (and
            /// ticking, if this actor ticks) until every handle is dropped.
            async fn run(mut self) {
                $( let mut ticker = ::tokio::time::interval($tick_period); )?

                loop {
                    ::tokio::select! {
                        message = self.receiver.recv() => {
                            match message {
                                Some(message) => self.handle_message(message),
                                // Every sender is gone, so nothing can ever
                                // reach this actor again.
                                None => break,
                            }
                        }
                        $(
                            _ = ticker.tick() => {
                                self.$tick_method();
                            }
                        )?
                    }
                }
            }
        }

        $(#[$handle_meta])*
        #[derive(Clone, Debug)]
        $handle_vis struct $handle {
            sender: ::tokio::sync::mpsc::Sender<$message>,
        }

        impl $handle {
            $(
                $(#[$spawn_meta])*
                /// Builds the actor, puts it on the current tokio runtime, and
                /// returns the handle that talks to it. The actor lives until
                /// the last handle is dropped.
                pub fn spawn($($ctor_arg: $ctor_arg_ty),*) -> Self {
                    let (sender, receiver) = ::tokio::sync::mpsc::channel($capacity);
                    ::tokio::spawn($actor::$constructor($($ctor_arg,)* receiver).run());

                    Self { sender }
                }
            )?

            $(
                $(#[$ask_meta])*
                pub async fn $ask_method(&self, $($ask_arg: $ask_arg_ty),*) -> $ask_ret {
                    let (reply, response) = ::tokio::sync::oneshot::channel();
                    let message = $message::$ask_variant($($ask_arg,)* reply);

                    // Ignore send errors. If this send fails, so does the
                    // response below. There's no reason to check for the
                    // same failure twice.
                    let _ = self.sender.send(message).await;
                    response.await.expect("Actor task has been killed")
                }
            )*
            $(
                $(#[$tell_meta])*
                pub async fn $tell_method(&self, $($tell_arg: $tell_arg_ty),*) {
                    let message = $message::$tell_variant($($tell_arg),*);
                    let _ = self.sender.send(message).await;
                }
            )*
        }
    };
}

pub(crate) use define_actor;

#[cfg(test)]
mod tests {
    // `define_actor` is in scope here textually, so it needs no import.
    use std::time::Duration;
    use tokio::sync::mpsc;

    struct Counter {
        count: u64,
        ticks: u64,
        receiver: mpsc::Receiver<CounterMessage>,
    }

    impl Counter {
        fn new(start: u64, receiver: mpsc::Receiver<CounterMessage>) -> Self {
            Self {
                count: start,
                ticks: 0,
                receiver,
            }
        }

        fn get(&self) -> u64 {
            self.count
        }

        fn ticks(&self) -> u64 {
            self.ticks
        }

        fn checked_add(&mut self, amount: u64) -> Result<u64, String> {
            self.count = self
                .count
                .checked_add(amount)
                .ok_or_else(|| "counter overflowed".to_string())?;
            Ok(self.count)
        }

        fn add(&mut self, amount: u64) {
            self.count += amount;
        }

        fn on_tick(&mut self) {
            self.ticks += 1;
        }
    }

    define_actor! {
        actor Counter;

        message CounterMessage;

        /// Cloneable handle to a running [`Counter`].
        handle CounterHandle;

        spawn with 8 => fn new(start: u64);

        tick every Duration::from_millis(10) => fn on_tick();

        ask {
            Get => fn get() -> u64;
            Ticks => fn ticks() -> u64;
            CheckedAdd => fn checked_add(amount: u64) -> Result<u64, String>;
        }

        tell {
            Add => fn add(amount: u64);
        }
    }

    #[tokio::test]
    async fn handle_mirrors_the_actor() {
        let handle = CounterHandle::spawn(0);

        assert_eq!(handle.get().await, 0);

        // `tell` methods return once queued, but the channel still preserves
        // ordering, so the following `ask` observes them.
        handle.add(2).await;
        handle.add(3).await;
        assert_eq!(handle.get().await, 5);

        assert_eq!(handle.checked_add(1).await, Ok(6));
        assert_eq!(
            handle.checked_add(u64::MAX).await,
            Err("counter overflowed".to_string())
        );
        assert_eq!(handle.get().await, 6);
    }

    #[tokio::test]
    async fn constructor_arguments_reach_the_actor() {
        let handle = CounterHandle::spawn(7);

        assert_eq!(handle.get().await, 7);
    }

    #[tokio::test]
    async fn handle_is_cloneable_and_shares_one_actor() {
        let handle = CounterHandle::spawn(0);
        let clone = handle.clone();

        handle.add(1).await;
        clone.add(1).await;

        assert_eq!(handle.get().await, 2);
    }

    #[tokio::test]
    async fn tick_runs_without_any_messages() {
        let handle = CounterHandle::spawn(0);

        // Generous next to the 10ms interval so a loaded machine still sees
        // several ticks.
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert!(handle.ticks().await > 1);
    }
}
