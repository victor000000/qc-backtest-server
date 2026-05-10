//! Slot runner launch and periodic status/monitoring loop.

mod launch;
mod qc_poller;
mod snapshot;
mod status_loop;
mod warm;

pub(crate) use launch::{LaunchSlotsParams, launch_slots};
