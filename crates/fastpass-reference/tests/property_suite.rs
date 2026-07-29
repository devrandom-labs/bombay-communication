// The gold impl must pass the entire suite — this is what proves the P1–P7
// contract is satisfiable, and pins the reference behaviour kimi's crate races.
fastpass_testkit::property_suite!(fastpass_reference);
