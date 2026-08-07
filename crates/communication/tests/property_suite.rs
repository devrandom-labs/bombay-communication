// The verification target: grow `Consumer::recv` until this whole suite is
// green. The stub ships passing the loss/FIFO/wakeup/drain properties and
// failing priority (P1), overtake, and anti-starvation.
communication_testkit::property_suite!(communication);
