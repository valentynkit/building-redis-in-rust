
DONE 1. Rewrite Event driven architecture from direct Epoll/Kqueue into into

1. Introduce an algo, blpop should propogate the ordered list of timeouts for waiters. than higher in the networking we should call the wait with the nearest timeout, than choose another one etc...
