# Fixture policy

Test fixtures must be synthetic or minimally transformed source-shaped records.
They must not contain private enrichment, patient-derived annotations, or data
copied from sources that the test does not need. Each fixture or its adjacent
test must state its source shape (archive/member name, pipe header, and the
edge case it represents) so reviewers can distinguish a privacy-safe test
fixture from an authoritative FDA bulk input.
