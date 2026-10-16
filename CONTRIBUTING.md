# Contributing

Bug reports, documentation improvements, parser fixtures, and pull requests are welcome. Contributions are distributed under the [MIT license](LICENSE).

1. Describe the expected behavior, observed behavior, Python version, and relevant Slurm command format.
2. Use invented usernames, directories, job identifiers, structures, and energies in shared examples. Replace site-specific submission wrappers with a minimal reproducer.
3. Add a focused regression test for a parser or state-transition change. Test scheduler, program-termination, and convergence evidence separately.
4. Run `python -m unittest discover -s tests -v`. The suite uses the standard library and mocked external services; no cluster or SMTP account is required.
5. Keep runtime dependencies minimal and preserve Python 3.7 compatibility. Do not introduce automatic changes to calculation files or job submission/cancellation.

The synthetic demo can be generated with `python examples/demo.py --output outputs/demo` in a new directory. Generated outputs and diagnostic bundles belong outside version control.

Please review [SECURITY.md](SECURITY.md) before attaching any real cluster or email data. Small redacted text excerpts are preferable to a full state database or diagnostic archive.
