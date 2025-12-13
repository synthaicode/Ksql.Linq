# Release v1.1.0

## Highlights

- **Hopping window support**: introduce Hopping window APIs and physical test coverage for the new window type.
- **Tumbling vs Hopping boundary**: clarify the hub rows path and function compatibility handling so Tumbling’s “C#-side compatibility” remains intact.
- **AI Assist usability**: improve onboarding and reliability guidance (Must/Should request rules, Copilot agent-mode prompting, and full/focused loading strategy).
- **Release safety**: CI enforces AI guide regeneration and library/CLI version consistency; CLI help text guides first-time usage.

## Notes

- AI Assist guide is bundled with the **Ksql.Linq** library package and is also exposed via `dotnet ksql ai-assist`.
- For upgrade considerations and operational steps, follow `docs/workflows/release_roles_and_steps.md`.

