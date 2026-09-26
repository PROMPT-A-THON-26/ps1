# Vault Frontend Accessibility

The frontend follows a semantic and keyboard-friendly dashboard model:

- A skip link moves keyboard users directly to the main content landmark.
- Navigation, dialogs, live regions, and progress indicators expose explicit accessible names/roles.
- Data tables use scoped column headers.
- Icon-only controls have accessible labels; decorative SVGs are hidden from assistive technology.
- Focus-visible outlines remain visible for keyboard navigation.
- Dynamic notifications and progress updates use live regions where appropriate.
- The interface provides a reduced-motion mode via `prefers-reduced-motion`.
- Secondary text is kept at readable contrast levels against the dark dashboard surfaces.

Accessibility changes should be validated with keyboard-only navigation and an automated accessibility audit before release.
