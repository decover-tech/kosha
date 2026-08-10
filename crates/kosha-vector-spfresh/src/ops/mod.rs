//! The five LIRE operations. `insert`/`delete` are the external interfaces;
//! `split`/`merge`/`reassign` are the internal machinery they (and
//! `ClusterIndex::rebalance`) trigger to keep the index balanced and
//! NPA-compliant — mirrors the paper's §3.2 breakdown.

pub(crate) mod delete;
pub(crate) mod insert;
pub(crate) mod merge;
pub(crate) mod reassign;
pub(crate) mod split;
