TODO:
We should take the gradual step by step evolution from direct libc kqueue usage, to abstraction like rustix, mio etc... and than the tokio itself.

Current phase is to make use of inbuf and write to a client.
