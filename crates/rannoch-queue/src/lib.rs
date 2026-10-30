//! The inbox: accepting `CloudEvents`, committing them about every 20 ms, telling workers what is
//! committed, and never losing track of anything that arrived.
