// The autoresearch target: grow `Consumer::recv` until this whole suite is
// green. The stub ships passing the loss/FIFO/wakeup/drain properties and
// failing priority (P1), overtake, and anti-starvation.
fastpass_testkit::property_suite!(fastpass);
